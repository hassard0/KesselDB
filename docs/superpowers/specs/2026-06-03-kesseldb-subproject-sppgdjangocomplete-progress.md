# SP-PG-DJANGO-COMPLETE — Django 6 ORM full CRUD — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** The two named gaps the
quoted-ident arc left (`SP-PG-DDL-IDENTITY`, `SP-PG-SQL-AGG-ALIAS-RENDER`)
are both closed, taking the **Django 6 ORM to full CRUD 8/8** on vulcan
(was 6/8). SQLAlchemy stays **7/7** (no regression) — two production
Python ORMs now work end-to-end against KesselDB over the PG wire.
Determinism preserved (IDENTITY reuses the proven SP-PG-SERIAL
apply-thread digest-covered counter; aggregate render is read-only). No
NEW gap surfaced. TaskList ready for completion.

Smoke harness: `scripts/sppgormdjango-smoke.py`,
  `scripts/sppgormsqlalchemy-smoke.py`
Smoke transcript: `docs/superpowers/sppgdjangocomplete-django-smoke-2026-06-03.txt`
Design: `docs/superpowers/specs/2026-06-03-kesseldb-sppgdjangocomplete-design.md`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc. | **DONE** |
| **T2** | `GENERATED { ALWAYS \| BY DEFAULT } AS IDENTITY [(seq opts)]` parse in CREATE TABLE — order-independent column-modifier run, marks the column SERIAL (reuses SP-PG-SERIAL counter). +5 KATs. | **DONE** (855fecc) |
| **T3** | `parse_agg` captures `AS alias`; new `select_aggregate`/`SelectAgg`/`agg_default_name` text helper; gateway `render_select_got` Shape 0 decodes the 16-byte LE i128 `Op::Aggregate` scalar → RowDescription(alias or default name) + 1 DataRow + CommandComplete. +9 KATs. | **DONE** (dec80a5, warning fix b27e7d4) |
| **T4** | vulcan Django smoke (6/8 → 8/8) + SQLAlchemy no-regression (7/7). | **DONE** |
| **T5** | USAGE.md (IDENTITY + aggregate render), STATUS row, this tracker → CLOSED, smoke transcript, vulcan cleanup. | **DONE** |

KAT delta: **+14** (kessel-sql +9 incl. helper, kessel-pg-gateway +3 render,
counted in 123 kessel-sql / 1002 kessel-pg-gateway passing on vulcan @
dec80a5). No PG-wire / HTTP / WS / binary surface byte touched outside
the new aggregate SELECT render; `#![forbid(unsafe_code)]` honored; no
new external deps.

## Named follow-ups (no NEW gap — these were pre-named in the smoke doc)

- `SP-PG-IDENTITY-SEQOPTS` — honor `START WITH` / `INCREMENT BY` (V1
  parses-and-ignores; the deterministic counter is start=1 step=1).
- `SP-PG-AGG-MULTI-RENDER` — wire render of grouped / multi-column
  aggregate results (the Op exists; only the single scalar render here).
- `SP-PG-AGG-FLOAT` — AVG as a fractional result (V1 i128 integer fold).
- `SP-PG-DJANGO-INTROSPECT` / `SP-PG-SAVEPOINT` — real `manage.py
  migrate` + nested `atomic()` (not exercised by this CRUD smoke).
