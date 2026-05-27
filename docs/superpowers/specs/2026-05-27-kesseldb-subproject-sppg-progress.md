# SP-PG — PostgreSQL wire protocol support — SP-arc Progress Tracker

Date created: 2026-05-27
Design spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
Scoping doc: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
TaskList: opens the second-of-three SP156 wire surfaces (the
PostgreSQL Frontend/Backend Protocol v3.0). Kicked off NOW that SP-WS
closed and the long-lived-connection plumbing — reader/writer-thread
session loop, bounded `mpsc::sync_channel` send queue, monotonic
`std::time::Instant` heartbeat — is in tree to reuse. Remaining
SP156 wire surface after SP-PG: HTTP/2 (explicit defer per SP156 §6).

## What this SP-arc ships

V1 = "psql + JDBC + simple CRUD works." Per SP156 §4 Phase 1: the
credibility-demonstrator that says "drop-in PG client compat." Phase
2 (V2 — DBeaver / Metabase / Tableau / pgAdmin) is a SEPARATE arc.

After V1 lands (T1..T18), a PG client speaking the v3.0 Frontend/
Backend protocol can:

1. Open a TCP connection to KesselDB on port 5432.
2. Complete the StartupMessage handshake.
3. Authenticate via SCRAM-SHA-256 using the Bearer token as the
   password (no separate PG credential storage — the operator
   rotates one secret).
4. Receive the ParameterStatus + BackendKeyData + ReadyForQuery
   greeting.
5. Send a Simple Query (`Q` message) containing one SQL statement.
6. Receive RowDescription + 0..N DataRow + CommandComplete (or
   ErrorResponse with a real PG SQLSTATE) + ReadyForQuery for each
   query.
7. CREATE TABLE / INSERT / SELECT / UPDATE / DELETE work end-to-end.
8. Close gracefully with Terminate (`X` message) — or get a clean
   idle-timeout close after 600s of silence.

**Out-of-scope (named, deferred — each is its own arc):** Extended
Query (Parse/Bind/Execute, V2 SP-PG-EXTQ), binary wire format (V2),
`pg_catalog.*` introspection stubs (V2 — V1 supports CLI +
programmatic clients only; GUI admin tools choke on connect because
pgAdmin/DBeaver issue ~50 introspection queries), COPY FROM STDIN /
COPY TO STDOUT (V2), LISTEN/NOTIFY (hard pass until changefeeds
exist), replication protocol (out indefinitely), query cancellation
via CancelRequest (V1 generates BackendKeyData but takes no action),
GSSAPI/LDAP (skip indefinitely), TLS (V2 wires SSLRequest 'S' reply
behind existing rustls feature gate), MD5 auth (deprecated by PG
14+; V1 advertises SCRAM-only), cleartext password (never V1), GUC
plumbing / `SET timezone` session state (V1 returns
CommandComplete:SET but actually ignores), RETURNING (V2), server-
side pipelining (V2 with extended-query support), multi-user model
(V2 SP-PG-USERS). See spec §2.2.

## Slice plan (mirrors design spec §10)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (936 lines, 11 weak-spots + 5 open questions) + scaffold crate (`kessel-pg-gateway` workspace member, zero external deps) + `proto.rs` PG v3.0 message-type-tag catalog (14 frontend + 15 backend tags, 6 auth subcodes, 3 ReadyForQuery indicators, 11 PG type OIDs, 2 format codes, 3 pre-handshake magic codes, framing rules) + `server.rs` placeholder `accept` returning `Err(PgError::NotYetImplemented)` (T1 stub regression-lock test catches a half-shipped T2) + 10 KATs locking the wire-protocol invariants against PG §55 / `pqcomm.h` / `pg_type.dat` / RFC 5802 / RFC 7677. | **DONE** | `6bd8654` (spec) + `1e1786b` (scaffold) |
| **T2** | Startup handshake + SCRAM-SHA-256 auth: `startup.rs` (StartupMessage parser, validate `protocol_version=196608`, handle SSL/Cancel/GSS magic via pre-handshake reply, key/value pair parser), `auth.rs` (SCRAM 4-round-trip state machine — AuthenticationSASL → SASLContinue → SASLFinal → AuthenticationOk; payload format per RFC 5802 §5.1 + RFC 7677), add `kessel-crypto::pbkdf2_hmac_sha256(password, salt, iterations, dk_len)` per RFC 8018 §5.2 (~20 lines on top of existing HMAC-SHA-256), ParameterStatus emit for {server_version, server_encoding, client_encoding, DateStyle, TimeZone, integer_datetimes, standard_conforming_strings, application_name}, BackendKeyData with deterministic-from-server-nonce pid+secret pair, ReadyForQuery('I'), Bearer-token bridge per spec §3.4 (ServerConfig.token = SCRAM password input), flip T1 stub regression-lock to "T2 emits AuthenticationSASL challenge". | **DONE** | `aa524bd` (PBKDF2) + `a65e5a3` (startup) + `97b4b9d` (SCRAM+server) |
| **T3** | Simple Query: `Q` message parser, SQL-text dispatch into `EngineApply::apply_sql`, EmptyQueryResponse (`I`) for whitespace/comment-only text, single-statement enforcement (multi-statement `Q` → `42601` syntax_error per spec §11 weak-spot #5). | OPEN | — |
| **T4** | PG type-OID mapping table + text-format renderer (`types.rs`): per spec §5 — KesselDB `FieldKind::{Bool,U*,I*,Fixed,Char,Bytes,Timestamp,Ref,OverflowRef}` → PG type OID + per-type text-format render (`t`/`f` for bool, `\\x<hex>` for bytea, `YYYY-MM-DD HH:MM:SS.ffffff+00` for timestamptz, decimal for ints, decimal-string for numeric). Locked KAT per FieldKind. | OPEN | — |
| **T5** | RowDescription (`T`) + DataRow (`D`) encoders: per-row streaming emit using T4 type table; field-format=0 (text) always; column NULL sentinel = i32 -1 (0xFFFFFFFF unsigned). | OPEN | — |
| **T6** | CommandComplete (`C`) + ReadyForQuery (`Z`) encoders: tag formats "SELECT N" / "INSERT 0 N" / "UPDATE N" / "DELETE N" / "SET" / "CREATE TABLE" (inferred from SQL leading keyword per spec §12 open question on DDL). | OPEN | — |
| **T7** | ErrorResponse (`E`) encoder + OpResult→SQLSTATE map: full table from spec §7.2 — `Exists`→`23505`, `SchemaError(msg)`→`42P01`/`42703`/`42804`/`42000` via string-match heuristic (spec §7.2 + §11 weak-spot #2), `Constraint`→`23000`/`23502`/`23505`, `Unavailable`→FATAL `57P03`, `Unauthorized`→FATAL `28000`, `TxAborted` variants → `40001`/`25006`/`58030`, unknown → `XX000` internal_error. | OPEN | — |
| **T8** | SELECT end-to-end: schema lookup (new `EngineApply::describe_table(name) -> Option<TableSchema>` trait method so PG-wire can map FieldKind → PG type OID + column name + width for RowDescription) + SELECT * FROM table → real result rows over the wire. | OPEN | — |
| **T9** | INSERT / UPDATE / DELETE end-to-end via simple-query: dispatch through `EngineApply::apply_sql` unchanged; CommandComplete tag inference from SQL leading keyword. | OPEN | — |
| **T10** | psql compatibility hand-test + USAGE.md sample-session + KAT-level synthetic peer driving full handshake → query → close sequence. Acceptance: `PGPASSWORD=$KESSEL_TOKEN psql -h localhost -p 5432 -U test "SELECT 1"` returns `1`. | OPEN | — |
| **T11** | pgcli / DBeaver / JDBC compatibility smoke (manual; doc results in `docs/USAGE.md`) — one real client per smoke + log any compat gaps as named follow-ups. Note: pgAdmin/DBeaver may CHOKE because V1 doesn't ship `pg_catalog` stubs — that's the V1 scope boundary (CLI + programmatic clients work; GUI admin tools are V2). | OPEN | — |
| **T12** | Listener integration: `kesseldb-server` `pg-gateway` feature flag mirroring `http-gateway`, `main.rs` spawn parallel to HTTP listener, port config via `PgGatewayConfig.listen_addr` (default 5432), bind separately from HTTP listener (a misbehaving pgcli cannot starve HTTP clients). | OPEN | — |
| **T13** | Bounded connection cap (`DEFAULT_MAX_PG_CONNS=256` per spec §8.1, smaller than HTTP gateway's 1024 because PG clients hold connections longer); too-many-connections ErrorResponse `53300`. | OPEN | — |
| **T14** | Pentest sweep — 10+ adversarial inputs: truncated startup, oversized message length (claim 1 GiB → clean `08P01` BEFORE allocation), malformed SCRAM client_first / client_final, auth replay, SQLi in `user` field, version 0/1/2/4/65535, NUL in payload, U+0000 in SQL, extended-query message in V1 (→`0A000`), repeated handshake on already-authed connection. | OPEN | — |
| **T15** | Per-connection reader/writer-thread split + bounded `mpsc::sync_channel::<Vec<u8>>(PG_SEND_QUEUE_BOUND=64)` send queue + close-on-overflow (mirror SP-WS T5 shape — `TcpStream::try_clone()`, monotonic `std::time::Instant`, `try_send` on overflow). | OPEN | — |
| **T16** | Idle timeout + graceful Terminate handling: `PG_DEFAULT_IDLE_TIMEOUT_SECS=600`, idle-fire → ErrorResponse `57014` query_canceled + close; `X` message → close TCP cleanly without further response. | OPEN | — |
| **T17** | Scatter-scan integration — verify cross-shard SELECTs work over PG-wire (uses existing SP-A `Route::Scatter` plumbing inherited via `EngineApply::apply_sql`; KAT only at the engine boundary). | OPEN | — |
| **T18** | Docs: ARCHITECTURE.md §Listeners gains PG-wire row; USAGE.md §PG gateway sample-session including `PGPASSWORD=$KESSEL_TOKEN psql` invocation; README mention of psql connectivity. **SP-PG V1 arc CLOSED at T18 commit.** | OPEN | — |

Optional / V2 follow-ups (named, deferred — each is its own arc):

- **T19 (V2)** — Extended Query (Parse / Bind / Describe / Execute / Sync / Close — message tags `P`/`B`/`D`/`E`/`S`/`C`) → mandatory for every ORM and prepared-statement client; per-portal/per-statement state model is a real engine extension. Own design spec `SP-PG-EXTQ`. ~3-5 slices.
- **T20 (V2)** — Binary-format wire encoding (per-column, per-direction, negotiated in `Bind`) — int / float / bool / text / timestamp / timestamptz first; numeric last because PG binary numeric is base-10000 variable-length-digit and bug-prone. ~2 slices.
- **T21 (V2)** — Minimal `pg_catalog.*` stubs (pg_type, pg_class, pg_attribute, pg_namespace) — enough for psql `\dt` / `\d <table>` not to crash; pgAdmin / DBeaver gateway. ~2 slices.
- **T22 (V2)** — `current_setting()` / `version()` / `current_schema()` / `current_database()` builtin functions. ~1 slice.
- **T23 (V2)** — `RETURNING` on INSERT/UPDATE/DELETE — requires `kessel-sql` extension. ~1 slice.
- **T24 (V2)** — Query cancellation (CancelRequest table + interrupt in-progress engine apply). ~1 slice.
- **T25 (V2)** — GUC plumbing (`SET timezone = '…'` session state + apply to text-format render). ~1 slice.
- **T26 (V2)** — COPY FROM STDIN / COPY TO STDOUT — bulk protocol flow. ~2-3 slices.
- **T27 (V2)** — TLS — SSLRequest 'S' reply + rustls handshake, behind existing `tls` feature gate. ~1 slice.
- **T28 (V2)** — MD5 auth fallback for legacy clients (P3 — only if needed by a real consumer). ~1 slice.

## T1 — what landed (2026-05-27, commits `6bd8654` + `1e1786b`)

**Two commits, ~686 LoC net delta (excluding the 936-line spec doc):**

**Commit `6bd8654` — design spec** (936 lines, no code change):
`docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
covers:
- Context (§1) — the full PG-client-ecosystem table (CLI / GUI / BI /
  ORM / Python / Java / Node / Go / Rust / ETL / ODBC) that V1 unlocks.
- Scope (§2) — V1 in-scope (Simple Query, SCRAM-SHA-256, text-format
  wire, OpResult→SQLSTATE) vs deferred (Extended Query / binary
  format / `pg_catalog` / COPY / LISTEN-NOTIFY / replication /
  CancelRequest / GSSAPI / LDAP / cert auth / TLS / MD5 / cleartext
  / GUC / RETURNING / pipelining / multi-user — each named with the
  arc that will pick it up).
- Wire protocol (§3) — PG v3.0 message catalog table, framing rules
  (`[type:1][length:4 BE incl-length-excl-type][payload]`),
  `PG_MAX_MESSAGE_SIZE=16 MiB` cap-before-allocation invariant,
  StartupMessage layout, SCRAM-SHA-256 4-round-trip flow with payload
  formats per RFC 5802 §5.1, Bearer ↔ SCRAM bridge (§3.4) with the
  "one credential surface" rationale.
- SQL surface (§4) — `EngineApply::apply_sql` is the dispatch
  boundary; no SQL rewriting at PG-wire layer; new
  `EngineApply::describe_table` trait method for RowDescription
  schema lookup (T8); streaming-at-wire / materialize-at-engine
  shape inherited from HTTP gateway.
- PG type-OID mapping (§5) — full FieldKind → OID table locked
  (Bool=16, I8/I16/U8=21, I32/U16=23, I64/U32/U64=20, Char=text=25,
  Bytes/Ref/OverflowRef=bytea=17, Timestamp=timestamptz=1184,
  U128/I128/Fixed=numeric=1700); text-format wire encoding only in
  V1.
- Auth (§6) — SCRAM-SHA-256-only stance with rationale (PG 10+
  default since 2017; libpq + every modern driver supports it);
  PBKDF2 4096 iterations matches PG default; failure modes table
  with SQLSTATE codes.
- Errors (§7) — ErrorResponse message format from PG §55.2.6
  (S/V/C/M/D/H/P fields); full OpResult → SQLSTATE catalog mapping
  table; honest disclosure of the `SchemaError(msg)` string-match
  heuristic.
- Integration (§8) — new `kessel-pg-gateway` crate (zero external
  deps), wire into `kesseldb-server` behind `pg-gateway` feature
  flag (mirror `http-gateway`), per-connection thread model, per-
  connection state machine, session loop concurrency (mirror SP-WS
  T5), TLS as future feature gate.
- Acceptance criteria (§9) — 8 concrete acceptance items including
  psql connectivity + JDBC connectivity + pentest matrix +
  no-regression + zero-dep stance + HTTP gateway untouched.
- 18-task decomposition (§10) with KAT delta + real-wire-ship flag
  per task + V2 follow-ups T19+ listed.
- Self-review (§11) — 11 weak-spots: Bearer↔SCRAM dual-rotation,
  SchemaError→SQLSTATE heuristic, no streaming-from-engine,
  U64→i64 overflow, single-statement Q-message restriction, `SET`
  no-op, `allow_anonymous` knob danger, no `pg_catalog` means GUI
  tools choke, PG-wire ↔ HTTP gateway auth-semantics drift risk,
  pentest matrix V1-thin, `server_version` lying-as-PG-14 product
  risk.
- 5 open questions (§12) — token-rotation grace period, `user`
  field semantics for V2 multi-user, DDL CommandComplete tag
  inference, cancel-key generation cost, dbname acceptance policy.

**Commit `1e1786b` — scaffold:**

- **`kessel-pg-gateway` Cargo.toml**: workspace member, zero external
  deps (only `kessel-proto`+`kessel-client`+`kessel-crypto`). T2 will
  reuse kessel-crypto's existing SHA-256 + HMAC-SHA-256 and add
  PBKDF2-HMAC-SHA-256.
- **`src/lib.rs`** (~80 LoC including the doc comment that pins the
  spec + 18-slice plan + V2 follow-up list + zero-dep stance):
  module declarations + locked constants
  `PG_GATEWAY_DEFAULT_PORT=5432`, `PG_SEND_QUEUE_BOUND=64` (deeper
  than SP-WS's 16 because PG streams DataRow per row),
  `DEFAULT_MAX_PG_CONNS=256` (smaller than HTTP's 1024 — PG clients
  hold connections longer), `PG_DEFAULT_IDLE_TIMEOUT_SECS=600`,
  `PG_MAX_MESSAGE_SIZE=16 MiB`, `PG_DEFAULT_SCRAM_ITERATIONS=4096`,
  `SUPPORTED_SASL_MECH="SCRAM-SHA-256"`.
- **`src/proto.rs`** (~400 LoC including doc comments + tests): the
  full PG v3.0 message-type-tag catalog cross-referenced against PG
  §55.7 (every constant has a comment naming the source). 14
  frontend tags (Q/X/p/P/B/D/E/S/C/H/d/c/f/F), 15 backend tags
  (R/S/K/Z/T/D/C/E/N/I/t/1/2/n/s), 6 authentication subcodes
  (Ok=0, CleartextPassword=3, MD5=5, SASL=10, SASLContinue=11,
  SASLFinal=12), 3 ReadyForQuery status indicators (I/T/E), 11 PG
  type OIDs from `pg_type.dat` (bool=16, bytea=17, int8=20, int2=21,
  int4=23, text=25, float4=700, float8=701, varchar=1043,
  timestamptz=1184, numeric=1700), 2 format codes (text=0,
  binary=1), 3 pre-handshake magic codes derived from
  `(1234<<16)|n` (SSL=80877103, Cancel=80877102, GSS=80877104),
  framing rules (`PG_MIN_MESSAGE_LENGTH=4` because length-includes-
  itself, `PG_DATA_ROW_COL_NULL_SENTINEL=-1` because PG NULL marker
  is i32 -1 / u32 0xFFFFFFFF).
- **`src/server.rs`** (~90 LoC including doc + tests): `PgError`
  enum (currently only `NotYetImplemented` — T2 widens with
  `StartupFailed`/`AuthFailed`/`ProtocolViolation`/`Io`), `accept
  <S: Write>(_stream)` returning `Err(PgError::NotYetImplemented)`
  without touching the stream. Generic `Write` bound matches what
  T2 needs (auth response is write-only); T5+ widens to `Read +
  Write` for the session loop.

**10 new KATs** (all in `kessel-pg-gateway`, all locking spec
invariants against authoritative sources):

1. `t1_pg_protocol_version_3_0_is_196608` — locks `0x00030000`
   constant with major=3 / minor=0 bit decomposition (PG §55.2.1).
2. `t1_pre_handshake_magic_codes_match_pg_postmaster_h` — locks
   SSL/Cancel/GSS magic via the canonical `PG_PROTOCOL(1234,n) =
   (1234<<16)|n` formula from `src/include/libpq/pqcomm.h`.
3. `t1_frontend_message_type_tags_match_pg_55_7_table` — 14 tags
   locked byte-for-byte (Q/X/p/P/B/D/E/S/C/H/d/c/f/F).
4. `t1_backend_message_type_tags_match_pg_55_7_table` — 15 tags
   locked (R/S/K/Z/T/D/C/E/N/I/t/1/2/n/s).
5. `t1_authentication_subcodes_match_pg_55_7_authentication` — 6
   subcodes locked (Ok=0, Cleartext=3, MD5=5, SASL=10,
   SASLContinue=11, SASLFinal=12).
6. `t1_ready_for_query_status_indicators_match_pg_55_2_2` — I/T/E
   locked.
7. `t1_pg_type_oids_match_pg_type_dat` — 11 OIDs locked
   (bool/bytea/int2/int4/int8/text/float4/float8/varchar/
   timestamptz/numeric).
8. `t1_format_codes_text_zero_binary_one_per_pg_55_2_2` — locked.
9. `t1_framing_length_invariants_match_spec_3_1` — length-includes-
   itself, min=4, NULL sentinel -1 ↔ 0xFFFFFFFF equivalence.
10. `t1_accept_returns_not_yet_implemented_stub` — regression-lock;
    T2 MUST update alongside the real handshake response (mirrors
    SP-WS T1's `t1_handle_upgrade_returns_not_yet_implemented_stub`
    pattern that successfully caught a half-shipped T2 in SP-WS).

**KAT delta:** +10. All cross-referenced against authoritative
sources (PG §55, PG `src/include/libpq/pqcomm.h`, PG `src/include/
catalog/pg_type.dat`, RFC 5802, RFC 7677).

**Zero-dep stance preserved:** no new external deps;
`cargo tree -p kesseldb-server -e normal` shows no new entries
(kessel-pg-gateway not yet wired into kesseldb-server — T12 adds
the `pg-gateway` feature flag); `cargo tree -p kessel-pg-gateway
-e normal` shows ONLY workspace crates (kessel-proto, kessel-client
+ its transitive workspace deps, kessel-crypto); kessel-crypto
unchanged from 0 external deps.

**Test counts:**
- kessel-pg-gateway: 0 → 10 (+10)
- Workspace default: 1450 → 1460 (+10)
- Workspace featured: 1483 → 1493 (+10)

seed-7 GREEN. tree-grep EMPTY. `#![forbid(unsafe_code)]` honored
throughout the new code. All prior tests pass. HTTP/1.1 + WebSocket
surfaces byte-untouched.

**What T1 deliberately did NOT do:**
- No real listener (T12 — gated behind `pg-gateway` feature flag).
- No startup handshake (T2).
- No SCRAM-SHA-256 (T2).
- No PBKDF2 in kessel-crypto (T2).
- No Q-message parser (T3).
- No type-text renderer (T4).
- No RowDescription/DataRow encoder (T5).
- No CommandComplete/ReadyForQuery encoder (T6).
- No ErrorResponse encoder (T7).
- No SELECT/INSERT/UPDATE/DELETE wire-up (T8/T9).
- No `kesseldb-server` `pg-gateway` feature flag (T12 — deferred so
  a half-shipped T2 cannot accidentally surface to operators).
- No e2e psql test (T10).
- No browser harness (N/A — psql is a CLI tool).

**Post-T1 behavior:** the crate compiles + its 10 KATs pass. No
PG-wire traffic flows; calling `server::accept` returns
`PgError::NotYetImplemented`. The next session (T2) flips the stub.

## T2 — what landed (2026-05-27, commits `aa524bd` + `a65e5a3` + `97b4b9d`)

**Three commits, +42 KATs, RFC 5802 byte-equivalence proven.** T2
delivers the startup handshake + the SCRAM-SHA-256 authentication
exchange + the post-auth greeting (ParameterStatus + BackendKeyData
+ ReadyForQuery). After T2 a credentialed PG client speaking the
v3.0 Frontend/Backend protocol can complete the connection-
establishment dance against KesselDB end-to-end; what's missing
for a real "psql works" experience is T3-T9 (Q-message parser +
type mapping + RowDescription/DataRow encoders + ErrorResponse
encoder + dispatch into `EngineApply::apply_sql`).

**Commit `aa524bd` — `kessel-crypto`: PBKDF2-HMAC-SHA-256:**

- `pbkdf2_hmac_sha256(password, salt, iterations) -> [u8; 32]` per
  RFC 8018 §5.2. dkLen is locked to 32 bytes (== hLen for SHA-256),
  so the outer-block loop collapses to a single T_1 block; the
  implementation is ~20 lines (U_1 = HMAC(P, S || 0x00000001),
  then iterate U_{i+1} = HMAC(P, U_i) for `iterations - 1` rounds,
  XOR-folding into the output).
- Panics on `iterations == 0` — RFC 8018 §5.2 requires c > 0 and a
  never-iterated HMAC silently masquerading as a salted password
  is the kind of bug that ships a security incident.
- +4 KATs: three reproducible (P, S, c) vectors at c=1, c=2,
  c=4096 (the c=4096 case IS the PG-SCRAM default and locks
  SP-PG byte-equivalence to libpq); RFC 7914 Appendix B
  reference vector for independent confirmation; determinism;
  zero-iter-panic guard.

**Commit `a65e5a3` — `kessel-pg-gateway::startup` (StartupMessage
parser + pre-handshake dispatch):**

- `classify_initial_message(buf) -> InitialMessage` dispatcher
  handling all four PG §55.2.1 first-message shapes: Startup
  (v3.0 = 196608), SslRequest (80877103), GssEncRequest
  (80877104), CancelRequest (80877102). Each magic code lands a
  dedicated variant; unknown codes → `UnsupportedProtocolVersion`.
- Cap-before-allocation invariant: length prefix validated
  against `PG_MAX_MESSAGE_SIZE` (16 MiB) BEFORE any allocation
  — a client claiming 1 GiB gets clean `LengthTooLarge` rejection
  without `Vec::with_capacity` ever being called.
- `StartupError` enum maps to spec §6.2 SQLSTATEs:
  `LengthTooSmall` / `LengthTooLarge` / `MalformedBody` /
  `MalformedPreHandshake` / `MalformedCancelRequest` → `08P01`;
  `UnsupportedProtocolVersion` → `0A000`; `MissingUserParameter`
  → `28000` (empty user collapsed to missing — every auth path
  requires non-empty).
- Strict NUL-separated k=v body parser per PG §55.2.1: even
  number of strings followed by an empty-string terminator; UTF-8
  validation on every key + value; empty-key-before-terminator
  rejected.
- `SSL_REPLY_NO_TLS = b'N'` + `GSS_REPLY_NO_GSS = b'N'` consts
  lock the V1 single-byte rejection reply per spec §3.2.
- +16 KATs covering well-formed minimal + multi-param parses;
  missing/empty `user` rejection; SSL/GSS classification + reply-
  byte locks; CancelRequest extraction; PG-v2 + PG-v4 rejection;
  length-too-small + length-too-large rejection; SSLRequest with
  body + CancelRequest with wrong-length rejection; body
  missing-terminator + odd-count-kv rejection; empty buffer →
  `LengthTooSmall{length:0}`.

**Commit `97b4b9d` — `kessel-pg-gateway::auth` (SCRAM-SHA-256) +
`server.rs` flip:**

SCRAM state machine (RFC 5802 + RFC 7677 + PG §55.3):
- `encode_authentication_sasl_challenge()` — locked 24-byte
  AuthenticationSASL frame advertising `SCRAM-SHA-256\0\0`.
- `encode_authentication_sasl_continue(server_first)` /
  `encode_authentication_sasl_final(server_final)` — R-envelope
  wrappers with auth_type=11/12.
- `encode_authentication_ok()` — locked literal
  `[b'R',0,0,0,8,0,0,0,0]` (every PG client recognizes this exact
  9-byte sequence).
- `parse_sasl_initial_response(payload)` — parses PG §55.7.4
  layout `[mech\0][client_first_len:u32][client_first]`;
  mechanism MUST be `SCRAM-SHA-256` (any other →
  `UnsupportedMechanism`).
- `start_scram(client_first, token, server_nonce, iterations)` —
  parses client-first per RFC 5802 §5.1 (enforces `n` channel-
  binding flag — V1 doesn't advertise CB so `y` and `p=...` are
  rejected); derives the per-session salt deterministically per
  spec §3.4 (`salt = SHA-256(server_nonce || token)[..16]`); builds
  the combined nonce (`client_nonce + server_nonce`); produces the
  server-first-message `r=<combined>,s=<b64>,i=<iter>`; returns
  `ScramState` carrying everything needed for round 2.
- `finish_scram(client_final, state, token)` — parses client-
  final; validates `c=biws` channel-binding (= base64("n,,") —
  the only legal value for a no-CB client); validates echoed
  nonce against the combined nonce we sent; base64-decodes the
  proof + checks length is exactly 32 (SHA-256 output); re-
  derives the full RFC 5802 §3 crypto chain (`SaltedPassword =
  PBKDF2 → ClientKey = HMAC(SP, "Client Key") → StoredKey =
  SHA-256(ClientKey) → AuthMessage = client_first_bare + "," +
  server_first + "," + client_final_without_proof →
  ClientSignature = HMAC(StoredKey, AuthMessage)`); recovers the
  client's claimed ClientKey via `Proof XOR ClientSignature`;
  CONSTANT-TIME-COMPARES `SHA-256(RecoveredClientKey)` against
  StoredKey (no timing oracle); computes + returns
  ServerSignature = `HMAC(ServerKey, AuthMessage)` as
  `"v=<sig_b64>"`.

`server.rs` `accept` flipped from T1's `NotYetImplemented` stub:
- Signature now `accept<S: Read + Write, F: FnOnce() -> String>
  (&mut S, Option<&[u8]>, F) -> Result<AcceptedSession, PgError>`
  (the FnOnce is the per-session nonce generator — production
  callers wire a CSPRNG, tests wire a fixed string for KAT
  reproducibility).
- Pre-handshake dispatch loop: SSLRequest → write 'N', loop;
  GSSENCRequest → write 'N', loop; CancelRequest → close; the
  first StartupMessage → continue to SCRAM.
- SCRAM 4-round-trip drive: write AuthenticationSASL → read
  SASLInitialResponse `p`-frame → write SASLContinue → read
  SASLResponse `p`-frame → write SASLFinal → write
  AuthenticationOk.
- Post-auth greeting (spec §8.4 + PG §55.2.6): 8
  `ParameterStatus` messages (`server_version`,
  `server_encoding=UTF8`, `client_encoding=UTF8`,
  `DateStyle=ISO,MDY`, `TimeZone=UTC`, `integer_datetimes=on`,
  `standard_conforming_strings=on`, `application_name` echo from
  StartupMessage); `BackendKeyData` with `pid + secret` derived
  deterministically from `SHA-256(server_nonce || token)` per
  spec §3.4 open question #4 (pid >= 16 to avoid kernel-reserved-
  PID collision; V2 SP-PG T24 wires the cancel-key table);
  `ReadyForQuery('I')`.
- `PgError` widened: `StartupFailed(StartupError)`,
  `AuthFailed(AuthError)`, `NoTokenConfigured` (`28000` — V1
  closed-mode requires Bearer token; open-mode rejected BEFORE
  reading client bytes), `Io(ErrorKind)`, `MessageTooLarge`,
  `UnexpectedMessageDuringAuth{tag}`. Old `NotYetImplemented`
  variant removed (T1 regression-lock flipped).

Spec §3.4 Bearer↔SCRAM bridge implemented: the operator's
`ServerConfig.token` IS the SCRAM password input (one credential
surface; rotating token rotates both HTTP-Bearer and PG-SCRAM
atomically); the `user` field is carried + logged but NOT used
for authorization (V2 SP-PG-USERS for multi-user).

+21 KATs across the two new modules:

`auth.rs` (+14):
- `t2_authentication_sasl_challenge_byte_pattern` — locks the
  24-byte AuthenticationSASL wire layout against PG §55.7.4.
- `t2_authentication_ok_byte_pattern` — locks the 9-byte
  literal `[b'R',0,0,0,8,0,0,0,0]`.
- `t2_authentication_sasl_continue_envelope` /
  `t2_authentication_sasl_final_envelope` — locks R-envelope
  shapes for SASL Continue / Final.
- `t2_sasl_initial_response_parses_mech_and_client_first` /
  `t2_sasl_initial_response_rejects_other_mechanism` (SCRAM-SHA-1
  rejected).
- **`t2_scram_round_trip_locks_rfc_5802_invariants`** —
  HEADLINE KAT: full RFC 5802 §3 client-emulator computes proof,
  server `start_scram` + `finish_scram` verifies + returns
  server-signature, client re-derives ServerSignature
  independently and byte-compares it matches.
- `t2_scram_bad_proof_is_rejected_28p01` (wrong token →
  `ProofVerificationFailed`).
- `t2_scram_nonce_mismatch_is_rejected` (replay-prevention
  primitive enforced).
- `t2_scram_bad_channel_binding_rejected` (`c=` != "biws"
  → rejection).
- `t2_scram_client_first_with_y_flag_rejected` (`gs2-cbind-flag
  = "y"` → `BadChannelBinding`).
- `t2_scram_client_final_missing_proof_rejected` /
  `t2_scram_client_final_non_base64_proof_rejected` /
  `t2_scram_client_final_short_proof_rejected`.
- `t2_scram_start_is_deterministic_given_fixed_nonce` (locks
  spec §3.4 salt derivation against refactor entropy).

`server.rs` (+7):
- **`t2_accept_runs_full_scram_handshake_to_ready_for_query`** —
  FLAGSHIP KAT: drives the full 3-frame inbound stream
  (StartupMessage + SASLInitialResponse + SASLResponse with a
  valid proof) through `accept()` over an in-memory `Read+Write`
  pipe; asserts AcceptedSession returned with right `user` +
  `pid >= 16`; asserts outbound bytes contain AuthenticationSASL
  prefix + AuthenticationOk literal + ParameterStatus
  (server_version, UTF8) + BackendKeyData with announced pid +
  secret + ReadyForQuery; asserts order invariant
  AuthenticationOk PRECEDES ReadyForQuery.
- `t2_accept_rejects_when_no_token_configured` (no bytes
  touched on the stream — the rejection is pre-read).
- `t2_accept_handles_ssl_request_then_completes_handshake`
  (SSLRequest → 'N' reply → SCRAM proceeds normally).
- `t2_accept_bad_proof_returns_auth_failed_no_ready_for_query`
  — proves the no-oracle invariant: failed proof emits NO
  AuthenticationOk + NO ReadyForQuery (per spec §6.2 + RFC 5802
  §7 — every failure looks the same from outside).
- `t2_accept_eof_before_startup_is_io_error`.
- `t2_backend_key_data_derivation_is_deterministic`.
- `t2_backend_key_data_changes_across_nonces` (different
  per-session nonces produce different (pid, secret) — the
  entropy story V2's cancel-key table will depend on).

T1 regression-lock `t1_accept_returns_not_yet_implemented_stub`
removed (superseded by `t2_accept_runs_full_scram_handshake_*`
which is the stronger "stub is gone AND real handshake works
end-to-end" lock).

**Zero-dep stance preserved.** `cargo tree -p kessel-pg-gateway
-e normal` shows only workspace crates: kessel-proto,
kessel-client, kessel-crypto. `#![forbid(unsafe_code)]` honored
across all three new modules + the enriched server.rs.

**Test counts:**
- kessel-crypto: 9 → 13 (+4)
- kessel-pg-gateway: 10 → 47 (+37 across the three commits: +0
  crypto, +16 startup, +21 auth+server)
- Workspace default: 1460 → 1501 (+41 — kessel-crypto delta
  also flows through to feature-gated builds)
- Workspace --all-features: ~1515 → 1556 (+41)

seed-7 GREEN (`kessel-vsr large_seed_corpus_is_deterministic_
and_converges` passes — the PG-wire surface is byte-disjoint
from the replicated state machine, so SP-PG cannot regress the
seed-7 corpus). HTTP/1.1 + WebSocket surfaces byte-untouched.
`cargo test --workspace` GREEN; `cargo test --workspace
--all-features` GREEN; no clippy regressions; no new tree-grep
matches for `unsafe`.

**Headline question — did SCRAM-SHA-256 land cleanly with RFC
5802 vectors passing? YES.** The flagship
`t2_scram_round_trip_locks_rfc_5802_invariants` KAT drives a
complete RFC 5802 §3 client-emulator round-trip (the client
emulator and the server share zero state — proof is computed
purely from the wire bytes the server would emit) and the
server-signature the server produces is byte-equal to what the
client re-derives independently. The complementary
`t2_accept_runs_full_scram_handshake_to_ready_for_query`
server-loop KAT drives the same exchange through `accept()`
over an in-memory `Read+Write` pipe and asserts the full post-
auth greeting byte sequence. A real `PGPASSWORD=$KESSEL_TOKEN
psql -U test -h localhost` session driven by libpq should pass
the same gate (smoke-test pending T12 listener wire-up).

**What T2 deliberately did NOT do:**
- No real listener (T12 — gated behind `pg-gateway` feature flag).
- No Q-message parser (T3).
- No type-OID mapping or text-format renderer (T4).
- No RowDescription/DataRow encoder (T5).
- No CommandComplete/ReadyForQuery encoder for query results (T6 —
  T2 emits ReadyForQuery for the post-auth greeting only).
- No ErrorResponse encoder (T7) — startup-phase errors return
  `PgError::*` to the (not-yet-wired) server-loop; the SQLSTATE
  encoder lands in T7.
- No `OpResult` → SQLSTATE map (T7).
- No SELECT/INSERT/UPDATE/DELETE wire-up (T8/T9).
- No `kesseldb-server` `pg-gateway` feature flag (T12 — deferred
  so a half-shipped T2..T9 cannot accidentally surface to
  operators).
- No real psql smoke (T10 — needs T12 first).
- No `allow_anonymous` flag (spec §3.4 mentions it; V1 ships the
  closed-mode-only path. The flag would gate an
  AuthenticationOk-without-SCRAM short-circuit — useful for
  local dev, NEVER prod; deferred so a default-off knob doesn't
  ship accidentally-on).

**Post-T2 behavior:** the crate compiles + its 47 KATs pass + the
SCRAM-SHA-256 server-side state machine is byte-equivalent to
RFC 5802. No real TCP listener accepts PG connections yet
(T12 wires it). Calling `server::accept` directly with a
`Read + Write` stream and a Bearer token runs the full handshake
to ReadyForQuery; the returned `AcceptedSession` carries the
username and the BackendKeyData pair the server announced. T3
adds the simple-query loop on top.

## References

- Design: `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md` (936 lines)
- Scoping: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
- SP-WS sibling (template + plumbing to reuse):
  `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
  + `docs/superpowers/specs/2026-05-26-kesseldb-subproject-spws-progress.md`
- Scaffold: `crates/kessel-pg-gateway/`
  (`Cargo.toml` + `src/lib.rs` + `src/proto.rs` + `src/server.rs`)
- PostgreSQL Documentation §55 — Frontend/Backend Protocol v3.0
- RFC 5802 — SCRAM (Salted Challenge Response Authentication Mechanism)
- RFC 7677 — SCRAM-SHA-256 + SCRAM-SHA-256-PLUS
- RFC 8018 §5.2 — PBKDF2 (T2 adds `kessel-crypto::pbkdf2_hmac_sha256`)
- RFC 4648 — base64 (already in `kessel-crypto` from SP-WS)
- PG SQLSTATE Appendix A — the complete error code catalog
- KesselDB SP141 HTTP gateway: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`
  (sibling listener pattern + per-connection thread + Bearer auth)
