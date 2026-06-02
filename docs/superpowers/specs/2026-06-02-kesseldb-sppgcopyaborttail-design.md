# SP-PG-COPY-ABORT-DONE-TAIL — drain CopyDone/CopyFail tail after a COPY abort

> Status: T1 — design spec + drain-flag state + KATs (this commit).
> T2 wires the smoke + USAGE. T3 STATUS + tracker closure.
>
> SP-arc parent: SP-PG-COPY V1 (text format) and SP-PG-COPY-CSV V1
> (CSV format). The bug closed here was surfaced as a side-note in
> the SP-PG-COPY-CSV-NUMERIC T2/T3 vulcan transcript
> (`docs/superpowers/sppgcopycsvnumeric-t2-smoke-2026-06-02.txt`,
> §3 footnote) and is independent of the validator landing — every
> ErrorResponse-during-COPY path through the gateway is affected,
> not only the NUMERIC validator's.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopyaborttail-progress.md`
> (created at T1; updated each slice).
>
> Date: 2026-06-02

## §1. Context — the abort-tail protocol_violation

PG COPY protocol per §55.2.7 has a subtle behaviour the V1 gateway
missed. When the server detects a per-row error mid-CopyData (a
parse error, a NUMERIC validator rejection, a constraint failure
inside the BULKAPPLY batch flush, etc.):

1. The server emits `ErrorResponse + ReadyForQuery` and transitions
   internal state back to Idle.
2. The **client doesn't see** the ErrorResponse + RFQ until it
   reads its next inbound frame. In the meantime, it may still be
   streaming `CopyData` frames from its own input buffer AND may
   have already queued the natural-end-of-input `CopyDone` (or
   `CopyFail`) frame on the wire.
3. Real PostgreSQL **silently drains** any `CopyData` / `CopyDone`
   / `CopyFail` frames that arrive after the abort, until the
   client reads the ErrorResponse and stops writing.

V1 bug: the gateway's state machine in `server::run_session` checks
`copy_state.is_in()` and routes `c` / `f` / `d` tags accordingly.
After a `process_copy_data` Failed outcome the state is reset to
Idle — so the next inbound `c` (CopyDone) falls through to the
top-level `match tag { ... other => unsupported message tag 0x63 }`
arm, which writes `08P01` AND returns `Err(PgError::UnexpectedMessageDuringAuth { tag: b'c' })`
— closing the connection.

Observable symptom from `sppgcopycsvnumeric-t2-smoke-2026-06-02.txt`:

```
ERROR:  COPY csv row 1 column "amount" NUMERIC: bad byte 0x68 at position 0
< clean 22P02, expected >
unsupported message tag: 0x63
< spurious 08P01, then connection drops >
```

psql tolerates the connection close (it reopens for the next
command) so the user-visible end-to-end is "the error fired", but:

- The spurious `08P01 protocol_violation` is wrong — the client
  did nothing wrong.
- The connection drop forces a full reconnect (handshake + auth +
  ParameterStatus dance) per error, which is a 1-2 round-trip hit
  that pooled clients (SQLAlchemy, JDBC HikariCP, pgx pool) treat
  as a connection eviction.
- A persistent-connection client batching multiple COPY commands
  (e.g. an ETL loop with one Connection.execute per CSV chunk)
  would see every malformed-row error close the connection — a
  100× perf cliff on noisy inputs.

Real PG behavior locked here: an ErrorResponse-during-COPY leaves
the connection alive, the client sends its trailing `CopyDone` /
`CopyFail` (and any further `CopyData` it had already queued),
the server silently drains every byte until it observes the
tail-end frame, and the next Query works on the same connection.

## §2. Scope

### §2.1. V1 in-scope

1. **Drain flag in `server::run_session`.** Introduce a local
   `bool expecting_copy_tail` (default false). The flag is set to
   true the moment a `CopyData` dispatch returns
   `CopyDataOutcome::Failed` (the only path that emits
   ErrorResponse + resets `copy_state` to Idle while the client may
   still be mid-stream). It is cleared the moment a `CopyDone` /
   `CopyFail` tail is observed and silently drained.
2. **Idle-state drain arm.** Before the top-level `match tag`,
   when `copy_state` is Idle AND `expecting_copy_tail` is true AND
   the tag is one of `d` / `c` / `f`: silently discard the body and
   `continue`. `c` and `f` additionally clear the flag (they're the
   protocol-defined tail frames). `d` keeps the flag set (more tail
   data may follow before the client's writer notices the
   ErrorResponse).
3. **Defensive rejection preserved.** When `expecting_copy_tail`
   is false AND the tag is `c` / `f` (a stray CopyDone / CopyFail
   with no abort context), V1 behaviour is preserved — falls
   through to the existing `other => 08P01 unsupported message tag`
   arm. (Stray `d` was already covered by the existing KAT
   `t2_run_session_stray_copy_data_in_idle_rejected_08p01`; this
   arc does NOT widen the stray-`d` rejection.)
4. **No new wire emissions.** The drain arm writes zero bytes —
   it only reads-and-discards. The `ErrorResponse + RFQ` that
   already went out from the `Failed` outcome is the only response
   the client gets for the abort; the tail frames are absorbed
   silently per PG.

### §2.2. V1 out-of-scope (named, deferred)

- **Drain-on-handshake-failure.** A `CopyDone` / `CopyFail`
  arriving BEFORE authentication completes would still close the
  connection (the accept path runs before `run_session`'s main
  loop). This shape is impossible in practice (the client cannot
  enter COPY mode before AuthenticationOk + ReadyForQuery), so V1
  doesn't add a redundant guard. Named arc:
  `SP-PG-COPY-ABORT-DONE-TAIL-PRE-AUTH`.
- **Cancel-request mid-COPY.** PG's protocol-level CancelRequest
  (over a separate TCP connection on the same backend_pid + secret)
  would let the client signal abort without sending CopyFail. V1
  doesn't action CancelRequest (named: SP-PG T24); the abort-tail
  drain here closes the in-line CopyFail path only.
- **Binary-format mid-frame tail.** A binary COPY's per-frame length
  prefixes mean a CopyData frame may carry only a partial logical
  row at the abort boundary. The drain treats binary `d` tail
  identically — read the framed body and discard. The complete-row
  semantics are not re-checked because the abort already happened
  and the rows would be discarded regardless.

## §3. State machine

```
Idle (expecting_copy_tail=false)
  │
  │  Q "COPY t FROM STDIN"
  ▼
CopyIn  ◄────── CopyData (Continue) ──────┐
  │                                       │
  │  CopyData (Failed)                    │
  │  ↓                                    │
  │  • emit ErrorResponse + RFQ           │
  │  • reset copy_state = Idle            │
  │  • set expecting_copy_tail = true     │
  ▼                                       │
Idle (expecting_copy_tail=true)           │
  │                                       │
  │  d tag → silently drain, flag stays   │ (loops here as long as
  │  c tag → silently drain, clear flag   │  client keeps streaming
  │  f tag → silently drain, clear flag   │  pre-error frames)
  │  any other tag → fall through to      │
  │    normal Idle dispatch (flag NOT     │
  │    cleared — non-COPY frames don't    │
  │    consume the expected tail; the     │
  │    next d/c/f still drains)           │
  ▼
Idle (expecting_copy_tail=false)
  │
  │  Q "SELECT ..."  → normal dispatch
  ▼
(same connection, no reconnect)
```

The "any other tag → fall through, flag NOT cleared" choice is
deliberate: real PG drains tail bytes UNTIL the protocol-defined
end-of-COPY frame (`c` or `f`) arrives. A pipelined client could
in theory send `Q` while the tail frames are still in flight; the
flag staying set lets us still drain a delayed `c` correctly.

In practice psql / libpq always send `CopyDone` before any next
`Q`, so the flag clears immediately. The defensive shape costs
zero bytes and adds zero KATs since the `c` / `f` drain is the
only state transition.

## §4. KATs

### §4.1. T2 — in-process server-loop KATs (new)

1. `t_abort_tail_drain_copydone_after_csv_error_keeps_connection_alive` —
   The HEADLINE. Sequence: Q `COPY t FROM STDIN`, three CopyData
   frames where row #2 is malformed (parse failure or NUMERIC
   reject), then CopyDone, then a normal Q `SELECT * FROM t`,
   then Terminate. Assert: outbound bytes contain ONE `08P01` or
   `22023` ErrorResponse for the bad row + ONE RFQ + the SELECT's
   RowDescription. The connection stays alive — the test wrapper
   returns Ok (not Err::UnexpectedMessageDuringAuth { tag: b'c' }).
2. `t_abort_tail_drain_copyfail_after_csv_error_keeps_connection_alive` —
   Same as #1 but the tail frame is `CopyFail` instead of
   `CopyDone`. Same assertion (no 0x66 spurious 08P01).
3. `t_abort_tail_drain_multiple_copydata_after_error_keeps_connection_alive` —
   Sequence: Q + bad CopyData (error fires) + two more
   CopyData frames (the client's pre-error in-flight bytes) + CopyDone
   + next Q. Asserts every `d` between the error and the `c` is
   drained silently (no second ErrorResponse, no protocol_violation).
4. `t_stray_copydone_in_pristine_idle_still_rejected_08p01` —
   Negative: when `expecting_copy_tail` is false (no preceding
   abort), a `c` frame is still rejected with `08P01` per V1.
   This preserves the defensive shape and locks the flag-gated
   behaviour.
5. `t_stray_copyfail_in_pristine_idle_still_rejected_08p01` —
   Negative companion to #4 for the `f` tag.

### §4.2. T2 — additional CopyData length-guard KATs

The drain reads the frame body using the same length-prefix read
that already happens at the top of the message loop, so no new
length-validation logic is added. The existing
`PG_MAX_MESSAGE_SIZE` cap and length-too-small `08P01` guard apply
unchanged.

## §5. T3 — vulcan smoke

Confirms the design on a real psql 16:

1. Start kesseldb against a fresh data dir.
2. `CREATE TABLE abort_smoke (id BIGINT, n CHAR(32))`.
3. Pipe a CSV with a malformed numeric row through
   `psql -c 'COPY abort_smoke FROM STDIN WITH (FORMAT csv, HEADER)'`.
4. Assert: psql exits with a clean `ERROR` containing `22P02`
   (the existing NUMERIC validator) and **no** `unsupported message
   tag` line.
5. Run `psql -c 'SELECT COUNT(*) FROM abort_smoke'` on the same
   server. Assert: the SELECT succeeds (proves no reconnect was
   needed, even though psql opens a fresh connection per `-c`).
6. Tear down.

Transcript saved to
`docs/superpowers/sppgcopyaborttail-t3-smoke-2026-06-02.txt`.

## §6. T4 — arc closure

- USAGE §9 — note the abort-tail drain behavior.
- STATUS.md — new row under "Latest arc deliveries".
- Progress tracker → CLOSED with HEADLINE.
- TaskList #383 ready for completion.
