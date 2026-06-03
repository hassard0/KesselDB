# SP-PG-SERIAL-RETURNING — deterministic autoincrement + INSERT RETURNING — design

Date: 2026-06-02

## 1. Context

`SP-PG-SQL-ORM-PARSE` landed the SQLAlchemy 2.0 declarative-ORM CRUD
smoke at **7/7** — but only for models with an **EXPLICIT** primary key
(`User(id=1, name="alice")`). Real ORM models overwhelmingly use
**autoincrement**: the model declares `id = Column(BigInteger,
primary_key=True, autoincrement=True)`, the application does NOT supply
`id`, the database assigns it, and the ORM reads it back via `INSERT …
RETURNING id`. SQLAlchemy 2.0 ALWAYS appends `RETURNING id` to an
autoincrement INSERT (the `implicit_returning` default).

Two named follow-ups close this together — they are a single coupled
feature (autoincrement is useless without reading the value back, and
RETURNING on an INSERT is mostly used for the auto-assigned id):

- **SP-PG-SERIAL** — deterministic autoincrement (`BIGSERIAL`/`SERIAL`).
- **SP-PG-RETURNING** — return server-assigned values to the client.

Today `CREATE TABLE … id BIGSERIAL PRIMARY KEY` parses (accept-and-skip:
the SERIAL alias maps to a plain integer width and the row's `id` is the
ObjectId pseudo-PK), but INSERT still REQUIRES an explicit `id` column or
`ID <n>`. This arc makes the engine assign it.

## 2. Determinism analysis (THE critical section)

KesselDB's whole correctness story is that `StateMachine::apply(op_number,
op)` is a pure function of `(committed state, op)` — NO clock, NO RNG.
Every replica applies the identical op stream in op-number order on a
single deterministic thread and converges bit-for-bit (the digest is a
CRC fold over all storage keys). Autoincrement must NOT break this.

**The sequence counter lives in the digest.** We add a per-type counter
in a reserved keyspace `SERIAL_TYPE = 0xFFFF_FFF4`, keyed by `type_id`
(big-endian in the object-id slot). This is EXACTLY the proven pattern
the global sequencer (SP79 `SEQ_TYPE = 0xFFFF_FFF0`) already uses:

- The counter is an ordinary storage key written via
  `self.storage.put(op_number, serial_counter_key(type_id), …)`. Because
  `make_key` produces a 20-byte key, it is covered by `Storage::digest`
  (which only skips the 28-byte MVCC keyspace).
- On an autoincrement insert: read the current counter (absent ⇒ 0),
  `next = current + 1`, write `next` back, and use `next` as the assigned
  value. Because the counter advances strictly in op-number order on the
  single deterministic apply thread, **every replica computes the
  identical sequence** — no RNG, no wall-clock, no per-connection state.
- **Crash-safe.** The counter is a normal WAL-backed put at `op_number`,
  so a crash + WAL replay resumes it exactly (the SP94 replay guard
  already short-circuits a re-applied mutating op at/below the durable
  cursor, so a replayed autoincrement insert never double-advances).
- **3-replica byte-identity**: identical op stream ⇒ identical counter
  writes ⇒ identical digest, asserted by reusing the sequencer-style
  `assert_eq!(a.digest(), b.digest())` harness.

**ObjectId assignment.** KesselDB rows are keyed by a 16-byte ObjectId
that the CALLER supplies today. For a SERIAL PRIMARY KEY the ENGINE must
assign it. The `id` pseudo-column IS the ObjectId (it is not a real
record field). So for the dominant `id BIGSERIAL PRIMARY KEY` shape:
`ObjectId = ObjectId::from_u128(counter)`. The mapping is the trivial
identity `u128(counter)`, so a subsequent `WHERE id = <n>` (ORM
select/update/delete by PK) addresses the row exactly.

**How the gateway stays deterministic.** The gateway/SQL layer must NOT
read or advance the counter (it has no replicated state and runs per
connection). Instead, an autoincrement INSERT compiles to an
`Op::Create` carrying a **sentinel ObjectId** (`SERIAL_SENTINEL = [0xFF;
16]`, an id no honest caller would pick — it is the reserved
`u128::MAX`). The SM recognizes the sentinel on a `serial_pk` type,
assigns the next counter value, and returns `OpResult::Created { id }`.
The assignment happens on the apply thread; the gateway only renders the
returned id.

## 3. Scope (V1)

- **Feature A**: `BIGSERIAL`/`SERIAL`/`SMALLSERIAL` as a real
  deterministic autoincrement PRIMARY KEY. CREATE TABLE flags the
  serial-PK column; INSERT omitting it triggers SM-side assignment.
- **Feature B**: `INSERT … RETURNING col1, col2, …`. Parse the clause;
  the SM returns the assigned id; the gateway emits RowDescription +
  DataRow(assigned values) + CommandComplete.
- Catalog: `ObjectType.serial_pk: bool` (backward-compatible trailer).
- Proto: `OpResult::Created { id: u128 }` (additive, new tag).

## 4. V1 out-of-scope (named follow-ups)

- **UPDATE/DELETE RETURNING** — `SP-PG-SQL-RETURNING-DML`. V1 scopes to
  INSERT RETURNING (the autoincrement case).
- **CREATE SEQUENCE DDL / `nextval`/`setval` functions** —
  `SP-PG-SEQUENCE-DDL`. V1 has an implicit per-table sequence only.
- **Non-PK SERIAL columns** (a `SERIAL` column that is NOT the primary
  key) — `SP-PG-SERIAL-NONPK`. V1 assigns only the PK/ObjectId.
- **Multiple SERIAL columns in one table** — only one autoincrement
  (the PK) per table in V1.
- **Gapless guarantee under abort** — PostgreSQL itself does NOT roll
  back a sequence on a failed/aborted INSERT (sequences are
  non-transactional); a failed insert that already advanced the counter
  leaves a gap. **We match PG: the counter advances when the row is
  successfully written; an insert that fails a constraint AFTER counter
  advance can leave a gap.** We minimize gaps by advancing the counter
  only on the successful-write path (after all constraint checks pass),
  so a rejected insert does NOT consume a value. Documented, not a bug.

## 5. Acceptance

A SQLAlchemy model declared WITHOUT an explicit id —
`id = Column(BigInteger, primary_key=True, autoincrement=True)` — does
full CRUD against KesselDB over the PG wire, and `w.id` reads back the
DB-assigned value after `session.commit()` (SQLAlchemy fetches it via the
`RETURNING id` clause). Measured on vulcan. All existing kessel-sql /
SM / gateway / catalog KATs pass (regression guard). The seed-7 VSR
oracle + 3-replica byte-identity digest test stay green.

## 6. Weak spots (named, not all fixed in V1)

1. **Counter-in-digest replication** — the counter is a digest-covered
   storage key advanced only on the apply thread; a 3-replica
   byte-identity test pins it. Risk if any non-apply-thread path ever
   advanced it (none does).
2. **Abort leaves gaps** — see §4. PG does too. Mitigated by advancing
   only on successful write; never a correctness bug.
3. **Serial that is NOT the PK** — out of scope (`SP-PG-SERIAL-NONPK`).
   A non-PK SERIAL column is currently a plain integer that the caller
   must supply.
4. **Multiple serial columns** — V1 supports one (the PK). A second
   SERIAL column is treated as a plain integer.
5. **Counter overflow** — the counter is u64; at `u64::MAX` the next
   insert would wrap. PG's `bigint` sequence raises `nextval: reached
   maximum value`; V1 rejects with a SchemaError rather than wrapping
   (a 64-bit counter is not reachable in practice — 1.8e19 inserts).
6. **RETURNING on multi-row INSERT** — a multi-row autoincrement INSERT
   `VALUES (…),(…) RETURNING id` should return one DataRow per row. V1
   handles the single-row autoincrement case (the SQLAlchemy ORM unit-
   of-work flushes one INSERT per new object by default); multi-row
   RETURNING is `SP-PG-RETURNING-MULTIROW`.
7. **RETURNING `*`** — `RETURNING *` (all columns) vs an explicit list;
   V1 parses an explicit column list and `id`. `RETURNING *` is
   `SP-PG-RETURNING-STAR`.

## 7. Execution

5-8 commits: T1 design, T2 catalog `serial_pk` + SM deterministic
sequence + KATs, T3 kessel-sql RETURNING parse + SERIAL-PK flag + KATs,
T4 gateway RETURNING wire + KATs, T5 vulcan SQLAlchemy autoincrement
smoke, T6 closure. All cargo on vulcan with
`CARGO_TARGET_DIR=/tmp/kdb-t-serial`. Direct commits to main; CI green is
the gate. Determinism is sacred — the seed-7 oracle + 3-replica
byte-identity digest test PROVE replicas agree.
