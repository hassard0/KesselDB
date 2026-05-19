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

## dict_flat.parquet (OBJ-2b-2)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    t=pa.table({'id':pa.array([7,7,-2,7,100],type=pa.int64()), \
    's':pa.array(['a','a','b','c','a'],type=pa.string())}); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/dict_flat.parquet', \
    use_dictionary=True, compression=None, version='1.0', data_page_version='1.0')"

Real pyarrow 24.0.0 output: dictionary-encoded, UNCOMPRESSED, V1, flat
REQUIRED. Expected logical rows:
id = [7, 7, -2, 7, 100]; s = ["a", "a", "b", "c", "a"].

## snappy_dict.parquet / snappy_plain.parquet (OBJ-2b-3)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    sch=pa.schema([pa.field('id',pa.int64(),nullable=False),pa.field('s',pa.large_utf8(),nullable=False)]); \
    t=pa.table({'id':pa.array([7,7,-2,7,100],type=pa.int64()),'s':pa.array(['a','a','b','c','a'],type=pa.large_utf8())},schema=sch); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/snappy_dict.parquet',use_dictionary=True,compression='snappy',version='1.0',data_page_version='1.0'); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/snappy_plain.parquet',use_dictionary=False,compression='snappy',version='1.0',data_page_version='1.0')"

Real pyarrow 24.0.0, SNAPPY-compressed, V1, flat REQUIRED.
snappy_dict = dictionary-encoded; snappy_plain = PLAIN.
Expected rows: id=[7,7,-2,7,100]; s=["a","a","b","c","a"].

## nullable.parquet / nullable_plain.parquet (OBJ-2b-4)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    t=pa.table({'id':pa.array([7,7,None,-2,100],type=pa.int64()),'s':pa.array(['a',None,'b','c','a'],type=pa.large_utf8())}); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/nullable.parquet',version='1.0',data_page_version='1.0'); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/nullable_plain.parquet',use_dictionary=False,compression=None,version='1.0',data_page_version='1.0')"

Real pyarrow 24.0.0. `nullable.parquet` = VANILLA default (OPTIONAL +
dictionary + Snappy, with NULLs). `nullable_plain.parquet` = OPTIONAL +
PLAIN + UNCOMPRESSED, with NULLs. V1, flat schema.
Expected rows: id=[7,7,null,-2,100]; s=["a",null,"b","c","a"].

## gzip_dict.parquet / gzip_plain.parquet / gzip_nullable.parquet (OBJ-2c-1)

Regenerate:

    python -c "import pyarrow as pa, pyarrow.parquet as pq; \
    sch=pa.schema([pa.field('id',pa.int64(),nullable=False),pa.field('s',pa.large_utf8(),nullable=False)]); \
    t=pa.table({'id':pa.array([7,7,-2,7,100],type=pa.int64()),'s':pa.array(['a','a','b','c','a'],type=pa.large_utf8())},schema=sch); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/gzip_dict.parquet',use_dictionary=True,compression='gzip',version='1.0',data_page_version='1.0'); \
    pq.write_table(t,'crates/kessel-parquet/tests/fixtures/gzip_plain.parquet',use_dictionary=False,compression='gzip',version='1.0',data_page_version='1.0'); \
    tn=pa.table({'id':pa.array([7,7,None,-2,100],type=pa.int64()),'s':pa.array(['a',None,'b','c','a'],type=pa.large_utf8())}); \
    pq.write_table(tn,'crates/kessel-parquet/tests/fixtures/gzip_nullable.parquet',compression='gzip',version='1.0',data_page_version='1.0')"

Real pyarrow 24.0.0, GZIP-compressed, V1. `gzip_dict` = REQUIRED +
dictionary-encoded. `gzip_plain` = REQUIRED + PLAIN. `gzip_nullable` =
OPTIONAL (nullable) + dictionary + GZIP, with NULLs.
Expected rows (dict/plain): id=[7,7,-2,7,100]; s=["a","a","b","c","a"].
Expected rows (nullable): id=[7,7,null,-2,100]; s=["a",null,"b","c","a"].
