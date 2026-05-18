# KesselDB Sub-project 77 — balance-guard helper

**Date:** 2026-05-18  **Status:** shipped. 177 green. Last of the
non-gating-roadmap items that can be landed responsibly in a slice.

## What it is (and honestly is not)

A balance guard is the canonical "an account balance must never go
negative" invariant — a **named, validated `col >= 0` CHECK**.
`Op::AddBalanceGuard { type_id, field_id }`; SQL
`ALTER TABLE t ADD BALANCE GUARD [ON] <col>`.

Deliberately implemented as a thin, deterministic translation to the
already-proven `AddCheck` path rather than a parallel subsystem:

- The engine validates the column is a **signed** numeric kind
  (`I8..I128`/`Fixed`) — a guard on an unsigned column would be
  vacuously true and is almost always a mistake, so it is refused with
  a clear `SchemaError` instead of silently doing nothing.
- It compiles the canonical program `load(field) >= 0` with the
  existing deterministic expr-VM builder and applies
  `Op::AddCheck { type_id, program }` under the same op number.

That reuse is the point: existing-row validation on add, per-write
enforcement on every `Create`/`Update`, transaction atomicity (a
violation in any `Op::Txn` member rolls back the whole batch), and
determinism all come from machinery that is already correct and
tested — no new catalog persistence format (which here would have
risked corrupting an upgraded on-disk catalog, since the catalog blob
has no per-type length prefix or version).

## Verified

`balance_guard_enforces_non_negative`: a negative `INSERT` and a
negative engine `Op::Update` are both rejected with no effect; adding
the guard when a current row is already negative fails *and leaves the
guard uninstalled*; a guard on an unsigned column is refused; a
negative member inside an `Op::Txn` rolls the whole transaction back;
identical histories produce the same digest. Full workspace regression
clean; determinism / VSR partition corpus (incl. seed 7) unchanged.

## Honest boundary

It is exactly a per-row non-negative CHECK with an ergonomic surface
and signed-column validation — not a cross-row accounting/conservation
system (e.g. "debits == credits across a batch"), which would be a
different, larger feature. Because it lowers to a CHECK, a violation
reports the generic constraint result; the guard is not separately
introspectable/droppable beyond the CHECK it installs (documented, not
hidden).
