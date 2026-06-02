# SP-PG-EXTQ-CAST-VALIDATE — close the V1 "strip + hope" silent-coercion vector — SP-arc Progress Tracker

Date created: 2026-06-02

Status: IN-PROGRESS — T1+T2 design + cast-tracking + validator landing
this commit.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqcastvalidate-design.md`
Parent SP-arc: SP-PG-EXTQ-CAST V1 (closed 2026-06-02 at T2). The
closing note explicitly named this arc as the follow-up that would
re-introduce the cast type check the V1 strip discarded.

## What this SP-arc ships

V1 = "every Parse SQL with `$N::TYPE` casts gets the declared OID
tracked + validated against the bound parameter OID at Bind time;
mismatches reject with `42846 cannot_coerce`."

1. `cast_stripper::strip_pg_casts_tracked(sql) -> (String,
   Vec<(usize, u32)>)` extends the V1 stripper with a tracking vec
   pairing each stripped `$N::TYPE` cast with the type's PG OID.
2. `PreparedStmt` gains a `param_casts: Vec<(usize, u32)>` field
   populated at Parse time.
3. `dispatch_bind` validates `prep.param_oids[index] ==
   declared_oid` for every tracked pair; mismatch returns
   `ExtqError::CastOidMismatch { position, declared, actual }`.
4. `server.rs` renders `CastOidMismatch` to SQLSTATE `42846
   cannot_coerce` with a human-readable message.

## Out-of-scope (named, deferred)

- **`SP-PG-EXTQ-CAST-VALIDATE-COMPAT`** — replace strict OID equality
  with PG's type-category compatibility table.
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL`** — also validate `::TYPE`
  casts on literals (not just `$N`).
- **`SP-PG-EXTQ-CAST-VALIDATE-MULTIWORD`** — recognise multi-word
  PG type names like `TIMESTAMP WITH TIME ZONE`.

## Slice plan (mirrors design spec §8)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + this progress tracker. | DONE (folded into T2 commit) | — |
| **T2** | `cast_stripper::strip_pg_casts_tracked` + `PreparedStmt.param_casts` + `dispatch_parse` integration + `dispatch_bind` validator + `ExtqError::CastOidMismatch` + server.rs renderer + KATs. | IN-PROGRESS | — |
| **T3** | vulcan psql literal-cast regression smoke transcript + USAGE §9 update flagging V1 "strip + hope" → V2 "strip + validate". | QUEUED | — |
| **T4** | STATUS row + parent SP-PG-EXTQ-CAST progress tracker follow-up entry → CLOSED + this progress tracker → CLOSED. | QUEUED | — |

KAT delta target: +8-12 (cast_stripper module + extq dispatch).

## Headline

Parse(`SELECT * FROM t WHERE id = $1::int8`, param_oids=[25/* TEXT */])
+ Bind → `42846 cannot_coerce` (was: silent strip + coerce). Real
JDBC literal-cast (`SELECT 1 FROM t WHERE id = 1::int8` no `$N`) still
PASSES — V1 only validates `$N` casts, not literal casts.
