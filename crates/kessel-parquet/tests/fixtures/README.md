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

## v2_plain / v2_dict / v2_gzip / v2_nullable .parquet (OBJ-2c-3)

Regenerate:

    python -c "
    import pyarrow as pa, pyarrow.parquet as pq
    schR = pa.schema([pa.field('id', pa.int64(), nullable=False),
                      pa.field('s',  pa.large_utf8(), nullable=False)])
    tR = pa.table({'id': pa.array([7,7,-2,7,100], type=pa.int64()),
                   's':  pa.array(['a','a','b','c','a'], type=pa.large_utf8())}, schema=schR)
    pq.write_table(tR,'crates/kessel-parquet/tests/fixtures/v2_plain.parquet',
                   use_dictionary=False, compression=None, version='1.0', data_page_version='2.0')
    pq.write_table(tR,'crates/kessel-parquet/tests/fixtures/v2_dict.parquet',
                   use_dictionary=True, compression=None, version='1.0', data_page_version='2.0')
    pq.write_table(tR,'crates/kessel-parquet/tests/fixtures/v2_gzip.parquet',
                   use_dictionary=True, compression='gzip', version='1.0', data_page_version='2.0')
    tN = pa.table({'id': pa.array([7,7,None,-2,100], type=pa.int64()),
                   's':  pa.array(['a',None,'b','c','a'], type=pa.large_utf8())})
    pq.write_table(tN,'crates/kessel-parquet/tests/fixtures/v2_nullable.parquet',
                   version='1.0', data_page_version='2.0')
    "

Real pyarrow 24.0.0, `data_page_version='2.0'`. Data pages are `DataPageHeaderV2`
(Thrift field-1 zigzag = 6; type byte `0x15 0x06`). Verified at raw-byte level:

- `v2_plain`: no dict page; first page header at offset 4 → `b[4]=0x15 b[5]=0x06` = DATA_PAGE_V2.
- `v2_nullable`: leading `DICTIONARY_PAGE` (V1-style, `0x15 0x04`) then first data page at
  `data_page_offset=41` → `b[41]=0x15 b[42]=0x06` = DATA_PAGE_V2.
- `v2_dict`: leading `DICTIONARY_PAGE` at offset 4; first data page at offset 42 →
  `b[42]=0x15 b[43]=0x06` = DATA_PAGE_V2.
- `v2_gzip`: leading `DICTIONARY_PAGE` at offset 4; first data page at offset 48 →
  `b[48]=0x15 b[49]=0x06` = DATA_PAGE_V2.

Expected rows:
- `v2_plain` / `v2_dict` / `v2_gzip`: id=[7,7,-2,7,100]; s=["a","a","b","c","a"]. REQUIRED,
  no nulls. `v2_plain` = PLAIN+UNCOMPRESSED; `v2_dict` = PLAIN_DICTIONARY+UNCOMPRESSED;
  `v2_gzip` = PLAIN_DICTIONARY+GZIP.
- `v2_nullable`: id=[7,7,null,-2,100]; s=["a",null,"b","c","a"]. OPTIONAL (nullable),
  PLAIN_DICTIONARY+SNAPPY. Proves V2 def-level null scatter.

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

## INT96 + DECIMAL (INT32/INT64/FLBA) + FLBA-UUID (OBJ-2c-4)

Ten fixtures generated and metadata-verified at planning time (pyarrow 24.0.0).

Regenerate (run from repo root):

```python
cd /c/Users/ihass/KesselDB && python3 -c "
import pyarrow as pa, pyarrow.parquet as pq
from decimal import Decimal
FIX = 'crates/kessel-parquet/tests/fixtures'

# INT96 fixtures
schI96 = pa.schema([pa.field('ts', pa.timestamp('ns'), nullable=False)])
tI96 = pa.table({'ts': pa.array([0, 86_400_000_000_000, -86_400_000_000_000],
                                 type=pa.timestamp('ns'))}, schema=schI96)
pq.write_table(tI96, f'{FIX}/int96_plain.parquet',
               use_deprecated_int96_timestamps=True, use_dictionary=False,
               compression=None, version='1.0', data_page_version='1.0')
pq.write_table(tI96, f'{FIX}/int96_dict.parquet',
               use_deprecated_int96_timestamps=True, use_dictionary=True,
               compression=None, version='1.0', data_page_version='1.0')
pq.write_table(tI96, f'{FIX}/int96_v2_snappy.parquet',
               use_deprecated_int96_timestamps=True, use_dictionary=False,
               compression='snappy', version='1.0', data_page_version='2.0')
schI96N = pa.schema([pa.field('ts', pa.timestamp('ns'), nullable=True)])
tI96N = pa.table({'ts': pa.array([0, None, -86_400_000_000_000], type=pa.timestamp('ns'))},
                  schema=schI96N)
pq.write_table(tI96N, f'{FIX}/int96_optional.parquet',
               use_deprecated_int96_timestamps=True, version='1.0', data_page_version='1.0')

# DECIMAL fixtures
tDi32 = pa.table({'d': pa.array([Decimal('1.23'), Decimal('-4.56'), Decimal('100.00')],
                                  type=pa.decimal128(5, 2))})
pq.write_table(tDi32, f'{FIX}/decimal_int32.parquet',
               use_dictionary=False, compression=None, version='2.6',
               data_page_version='1.0', store_decimal_as_integer=True)
pq.write_table(tDi32, f'{FIX}/decimal_int32_dict.parquet',
               use_dictionary=True, compression=None, version='2.6',
               data_page_version='1.0', store_decimal_as_integer=True)
tDi64 = pa.table({'d': pa.array([Decimal('1.234'), Decimal('-4.567'), Decimal('100000.000')],
                                  type=pa.decimal128(18, 3))})
pq.write_table(tDi64, f'{FIX}/decimal_int64.parquet',
               use_dictionary=False, compression=None, version='2.6',
               data_page_version='1.0', store_decimal_as_integer=True)
tDflba = pa.table({'d': pa.array([Decimal('1.23456'), Decimal('-4.56789'),
                                    Decimal('100000.00000')], type=pa.decimal128(30, 5))})
pq.write_table(tDflba, f'{FIX}/decimal_flba.parquet',
               use_dictionary=False, compression=None, version='1.0', data_page_version='1.0')
schDN = pa.schema([pa.field('d', pa.decimal128(30, 5), nullable=True)])
tDN = pa.table({'d': pa.array([Decimal('1.23456'), None, Decimal('-4.56789')],
                                type=pa.decimal128(30, 5))}, schema=schDN)
pq.write_table(tDN, f'{FIX}/decimal_flba_optional.parquet',
               version='1.0', data_page_version='1.0')

# FLBA non-DECIMAL UUID-like binary(16)
schU = pa.schema([pa.field('u', pa.binary(16), nullable=False)])
tU = pa.table({'u': pa.array([b'\x01'*16, b'\x02'*16, b'\x03'*16],
                              type=pa.binary(16))}, schema=schU)
pq.write_table(tU, f'{FIX}/flba_uuid.parquet',
               use_dictionary=False, compression=None, version='1.0', data_page_version='1.0')
print('wrote 10 fixtures')
"
```

### Metadata-verified physical types (all 10 fixtures)

| Fixture                  | phys_type              | conv_type | logical_type                       | V2-discriminator                             |
|--------------------------|------------------------|-----------|------------------------------------|----------------------------------------------|
| int96_plain              | INT96                  | NONE      | None                               | —                                            |
| int96_dict               | INT96                  | NONE      | None                               | —                                            |
| int96_v2_snappy          | INT96                  | NONE      | None                               | `b[4]=0x15 b[5]=0x06` (DATA_PAGE_V2)        |
| int96_optional           | INT96                  | NONE      | None                               | —                                            |
| decimal_int32            | INT32                  | DECIMAL   | Decimal(precision=5, scale=2)      | —                                            |
| decimal_int32_dict       | INT32                  | DECIMAL   | Decimal(precision=5, scale=2)      | —                                            |
| decimal_int64            | INT64                  | DECIMAL   | Decimal(precision=18, scale=3)     | —                                            |
| decimal_flba             | FIXED_LEN_BYTE_ARRAY   | DECIMAL   | Decimal(precision=30, scale=5)     | —; type_length=13                            |
| decimal_flba_optional    | FIXED_LEN_BYTE_ARRAY   | DECIMAL   | Decimal(precision=30, scale=5)     | —; type_length=13                            |
| flba_uuid                | FIXED_LEN_BYTE_ARRAY   | NONE      | None                               | —; type_length=16; no DECIMAL logicalType    |

`store_decimal_as_integer=True` was required for `decimal_int32` and `decimal_int64` to produce
INT32/INT64 physical type; without it pyarrow defaults to FIXED_LEN_BYTE_ARRAY for DECIMAL.
`decimal_flba` and `decimal_flba_optional` use precision=30 which forces FLBA by default.

**NOTE:** BYTE_ARRAY DECIMAL is supported by the decoder but pyarrow 24.0.0 cannot write it;
the decoder's BYTE_ARRAY DECIMAL path is exercised by hand-KATs in `lib.rs#tests`. The
end-to-end source-format-independence proof across the THREE writable physical types
(INT32 / INT64 / FLBA) is the `decimal_cross_physical_type_determinism_pin` (see the
matched-precision fixtures below).

### Expected logical rows

- `int96_plain` / `int96_dict` / `int96_v2_snappy`: ts=[0ns, +86400s ns, -86400s ns]
  → `Timestamp(0)`, `Timestamp(86_400_000_000_000)`, `Timestamp(-86_400_000_000_000)`.
- `int96_optional`: ts=[0ns, NULL, -86400s ns].
- `decimal_int32` / `decimal_int32_dict`: d=[1.23, -4.56, 100.00] precision=5 scale=2
  → `Decimal{unscaled:123,scale:2}`, `Decimal{unscaled:-456,scale:2}`, `Decimal{unscaled:10000,scale:2}`.
- `decimal_int64`: d=[1.234, -4.567, 100000.000] precision=18 scale=3
  → `Decimal{unscaled:1234,scale:3}`, `Decimal{unscaled:-4567,scale:3}`, `Decimal{unscaled:100000000,scale:3}`.
- `decimal_flba`: d=[1.23456, -4.56789, 100000.00000] precision=30 scale=5
  → `Decimal{unscaled:123456,scale:5}`, `Decimal{unscaled:-456789,scale:5}`, `Decimal{unscaled:10000000000,scale:5}`.
- `decimal_flba_optional`: d=[1.23456, NULL, -4.56789].
- `flba_uuid`: u=[0x01×16, 0x02×16, 0x03×16] → `Bytes(vec![0x01;16])` etc.

## DECIMAL matched-precision fixtures (SP108 T4 review)

These three fixtures carry the SAME 5 logical decimal values at the SAME
scale=2 across three different physical encodings, enabling the
`decimal_cross_physical_type_determinism_pin` end-to-end source-format
independence assertion through the production `extract()`.

Regenerate (run from repo root):

```python
import pyarrow as pa, pyarrow.parquet as pq
from decimal import Decimal
FIX = "crates/kessel-parquet/tests/fixtures"
LOGICAL = [Decimal("123.45"), Decimal("-67.89"), Decimal("100000.00"),
           Decimal("0.00"),   Decimal("-999999.99")]

def write_eq(prec, name, store_int):
    arr = pa.array(LOGICAL, type=pa.decimal128(prec, 2))
    t = pa.table({"d": arr})
    kw = dict(use_dictionary=False, compression=None,
              version="2.6", data_page_version="1.0")
    if store_int:
        kw["store_decimal_as_integer"] = True
    pq.write_table(t, f"{FIX}/{name}", **kw)

write_eq(9,  "decimal_int32_eq.parquet", True)   # -> INT32
write_eq(18, "decimal_int64_eq.parquet", True)   # -> INT64
write_eq(30, "decimal_flba_eq.parquet",  False)  # -> FIXED_LEN_BYTE_ARRAY
```

### Metadata-verified physical types

| Fixture                  | phys_type              | conv_type | logical_type                       |
|--------------------------|------------------------|-----------|------------------------------------|
| decimal_int32_eq         | INT32                  | DECIMAL   | Decimal(precision=9,  scale=2)     |
| decimal_int64_eq         | INT64                  | DECIMAL   | Decimal(precision=18, scale=2)     |
| decimal_flba_eq          | FIXED_LEN_BYTE_ARRAY   | DECIMAL   | Decimal(precision=30, scale=2)     |

All three encode the same 5 logical values at scale=2; since the logical
value is `unscaled / 10^scale` and scale is matched, the unscaled integer is
identical across the three fixtures (e.g. `123.45` → unscaled `12_345` in all
three). Decoded `PqValue::Decimal { unscaled, scale }` rows are byte-identical
across INT32 / INT64 / FLBA — that's the source-format-independence proof.

Expected logical rows (all three fixtures):

```
123.45        -> Decimal { unscaled:      12_345, scale: 2 }
-67.89        -> Decimal { unscaled:      -6_789, scale: 2 }
100_000.00    -> Decimal { unscaled:  10_000_000, scale: 2 }
0.00          -> Decimal { unscaled:           0, scale: 2 }
-999_999.99   -> Decimal { unscaled: -99_999_999, scale: 2 }
```

(`-99_999_999` has |v| < 2^31 = 2_147_483_648, so it fits the INT32 backing
type at precision=9. The same unscaled integer is rewritten as 8 bytes for
INT64 and as 13 bytes big-endian two's-complement for FLBA.)
