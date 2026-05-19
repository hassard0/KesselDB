# Parquet Test Fixtures

## Producer

**pyarrow 24.0.0** (installed via `pip install pyarrow`; confirmed `PYARROW 24.0.0`).

Exact command:

```python
import pyarrow as pa, pyarrow.parquet as pq
t = pa.table({'id': pa.array([7,-2,100], pa.int64()),
              'name': pa.array(['hi','x','zed'], pa.string()),
              'flag': pa.array([True,False,True], pa.bool_()),
              'score': pa.array([1.5,-0.25,3.0], pa.float64())})
pq.write_table(t, 'flat_required.parquet', version='1.0',
               use_dictionary=False, compression='NONE',
               data_page_version='1.0')
pq.write_table(t, 'flat_multirg.parquet', version='1.0',
               use_dictionary=False, compression='NONE',
               data_page_version='1.0', row_group_size=2)
```

Run from `crates/kessel-parquet/tests/fixtures/`.

## OBJ-2a producer constraints

- `version='1.0'`
- `use_dictionary=False` (PLAIN encoding)
- `compression='NONE'` (UNCOMPRESSED)
- `data_page_version='1.0'` (V1 data pages)
- All columns REQUIRED (flat non-null schema)

## Fixtures

### flat_required.parquet

Single row group, 4 columns, 3 rows.

| id (INT64) | name (BYTE_ARRAY/UTF8) | flag (BOOLEAN) | score (DOUBLE) |
|-----------|------------------------|----------------|----------------|
| 7         | "hi"                   | true           | 1.5            |
| -2        | "x"                    | false          | -0.25          |
| 100       | "zed"                  | true           | 3.0            |

### flat_multirg.parquet

Same logical table, 2 row groups (row_group_size=2: RG0 rows 0-1, RG1 row 2).

| id (INT64) | name (BYTE_ARRAY/UTF8) | flag (BOOLEAN) | score (DOUBLE) |
|-----------|------------------------|----------------|----------------|
| 7         | "hi"                   | true           | 1.5            |
| -2        | "x"                    | false          | -0.25          |
| 100       | "zed"                  | true           | 3.0            |

## Notes

- Test data only; not security-sensitive.
- These files are committed and tracked by git (not gitignored; verified via `git check-ignore`).
- The round-trip test `tests/fixture_roundtrip.rs` uses `include_bytes!` to load these at compile time.
