# SP-PG-DDL-FK-ENFORCE — design

**Headline:** a `FOREIGN KEY` declared in `CREATE TABLE` DDL now ENFORCES
referential integrity. This is a **WIRING arc**: the FK enforcement engine
already existed (Sub-project 6 / Sub-project 11 — `enforce_foreign_keys`,
`Op::AddForeignKey`, ON DELETE actions 0–4, DROP guards). The gap was that the
SQL DDL parser parsed `FOREIGN KEY (col) REFERENCES tbl …` and then **threw the
descriptor away** (`skip_referential_actions`). This arc captures the descriptor
during DDL parse, threads it through the `CreateType` op BY NAME, and registers
the FK at apply time through the **same** path `Op::AddForeignKey` uses.

## The pre-existing machinery (NOT touched / reused as-is)

- `kessel-proto`: `Op::AddForeignKey { type_id, field_id, ref_type_id, on_delete }`
  (tag 12) — unchanged.
- `kessel-sm`: row-write FK enforcement (`check_fk`, rejects an INSERT/UPDATE
  whose non-NULL FK has no matching parent; NULL FK allowed → `OpResult::Constraint`),
  the `Op::AddForeignKey` apply path (registers + validates existing data +
  backfills the reverse index for RESTRICT/CASCADE), DROP TABLE / DROP COLUMN FK
  guards, and ON DELETE actions `0=NO ACTION 1=RESTRICT 2=CASCADE 3=SET NULL
  4=SET DEFAULT`.
- `kessel-pg-gateway`: `constraint_to_sqlstate` already maps a `"foreign key"`
  constraint message to SQLSTATE `23503`.

## The change (4 parts)

### 1. Capture FK descriptors at DDL parse (`kessel-sql`)

`parse_referential_actions` (replaces `skip_referential_actions`) now RETURNS the
engine `on_delete` code parsed from the `ON DELETE` clause (default `0` = NO
ACTION, matching PostgreSQL). `ON UPDATE` actions are still parsed-and-ignored
(deferred — `SP-PG-DDL-FK-ON-UPDATE`). Both the table-level `FOREIGN KEY (col)
REFERENCES tbl [(col)]` constraint and the inline column `… REFERENCES tbl(col)`
form push a `kessel_catalog::FkSpec { child_col, ref_table, ref_col, on_delete }`
into a `fk_specs` vec.

### 2. Thread the descriptors through `CreateType` BY NAME (`kessel-catalog`)

`CREATE TABLE child (... REFERENCES parent)` needs the child's `type_id`, which is
only minted when the create APPLIES. So we resolve at apply time. The descriptors
ride a **marker-guarded, additive trailer** in the opaque type-def blob:

```
[ base name+fields ]
[ SP86 defaults trailer ]   (existing)
[ SERIAL trailer ]          (existing)
[ 0xFE | u16 count | count×FkSpec ]   ← NEW, only when fks non-empty
```

`encode_type_def_full_fk` appends the FK trailer **only when `fks` is non-empty**,
so a no-FK `CREATE TABLE` emits a **BYTE-IDENTICAL** def to before this arc (the
`Op` enum is unchanged, so every existing `Op::CreateType { def }` construction
site across proto/sm/sql/read_pool/sharded_engine/oracles is unaffected — the
change is purely additive bytes inside an existing opaque field). The `0xFE`
marker is distinct from the serial flag byte (`1`) and the no-trailer terminus, so
the decoder peeks for it WITHOUT a presence anchor. `decode_type_fks` returns an
empty vec for old/no-FK blobs and on any short read (never load-bearing for the
name+fields decode).

### 3. Register at apply (`kessel-sm` `Op::CreateType` arm)

The `Op::AddForeignKey` body is factored into a shared `add_foreign_key(...)`
helper. On `CreateType` apply, after assigning field ids, the arm **pre-validates**
every FkSpec (resolve `field_id` from the child column name, `ref_type_id` from the
referenced table name) BEFORE mutating the catalog — so a forward reference / bad
column is a clean `SchemaError` that leaves **no half-created type behind** (atomic
create, matching PostgreSQL). It then pushes the type and registers each resolved
FK through `add_foreign_key`. Resolution is a pure function of catalog state on the
single deterministic apply thread ⇒ deterministic + atomic with the create.

### 4. Surface 23503 (`kessel-pg-gateway`)

The INSERT/UPDATE enforcement message is `"FOREIGN KEY violated on field …"` →
already maps to `23503`. We additionally widened `constraint_to_sqlstate` so the
ON DELETE RESTRICT block (`"… still references type …"`) and AddForeignKey's
existing-data check (`"… dangling reference"`) also map to `23503` (they used to
fall through to the `23000` default).

## Behaviour / supported surface

- **Enforced:** single-column FK declared table-level or inline. INSERT/UPDATE of a
  child with a non-NULL FK that has no matching parent row → `23503`. NULL FK
  allowed.
- **ON DELETE:** NO ACTION (0), RESTRICT (1), CASCADE (2), SET NULL (3), SET
  DEFAULT (4) — all the engine's existing actions, mapped from the DDL keyword.
- **Atomic create:** a forward reference (parent table not yet created) or unknown
  column → clean DDL `SchemaError`, no partial type. SQLAlchemy/Django create
  parents before children, so the common path resolves.

## Deferred (named follow-ups)

- `SP-PG-DDL-COMPOSITE-FK` — composite (multi-column) FKs (V1 captures only the
  FIRST column of a composite FK for enforcement; every row is keyed by the `id`
  pseudo-PK).
- `SP-PG-DDL-FK-ON-UPDATE` — `ON UPDATE` referential actions (parsed, not yet
  enforced).
- Circular / mutually-referential FKs declared inline at create time still require
  the referenced table to exist first; the standard parent-before-child ORM
  ordering avoids this. `ALTER TABLE ADD CONSTRAINT` (via `Op::AddForeignKey`)
  remains the escape hatch for true cycles.
