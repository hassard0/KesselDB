"""SP143 T9: pyarrow List<T> fixtures (real-data validation for nested decode).

Run from repo root:
    py crates/kessel-parquet/tests/fixtures/regen_list.py

Produces 5 .parquet files exercising the canonical 3-node LIST encoding
(outer group + REPEATED middle + primitive leaf), covering the 4-shape
matrix (REQ-REP-REQ, REQ-REP-OPT, OPT-REP-REQ, OPT-REP-OPT) plus a string
variant.

Writer settings match the project-wide producer constraints:
  version='1.0', use_dictionary=False, compression='NONE',
  data_page_version='1.0'
so leaf encoding is PLAIN (no DELTA / no DICTIONARY) and pages are V1 —
exactly what kessel-parquet's SP143 T1-T6 pipeline targets.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

HERE = Path(__file__).resolve().parent

WRITER_KWARGS = dict(
    version='1.0',
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
)

# 1. list_i64_required: REQ outer, REQ inner element (REQ-REP-REQ)
#    Records: [[1, 2, 3], [10, 20]]
schema_1 = pa.schema([
    pa.field(
        "my_list",
        pa.list_(pa.field("element", pa.int64(), nullable=False)),
        nullable=False,
    ),
])
table_1 = pa.Table.from_pydict(
    {"my_list": [[1, 2, 3], [10, 20]]},
    schema=schema_1,
)
pq.write_table(table_1, HERE / "list_i64_required.parquet", **WRITER_KWARGS)

# 2. list_i64_optional: REQ outer, OPT inner element (REQ-REP-OPT)
#    Records: [[10, None, 20]]
schema_2 = pa.schema([
    pa.field(
        "my_list",
        pa.list_(pa.field("element", pa.int64(), nullable=True)),
        nullable=False,
    ),
])
table_2 = pa.Table.from_pydict(
    {"my_list": [[10, None, 20]]},
    schema=schema_2,
)
pq.write_table(table_2, HERE / "list_i64_optional.parquet", **WRITER_KWARGS)

# 3. list_string: REQ outer, REQ inner string (REQ-REP-REQ, BYTE_ARRAY leaf)
#    Records: [["foo", "bar"], ["baz"]]
schema_3 = pa.schema([
    pa.field(
        "my_list",
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        nullable=False,
    ),
])
table_3 = pa.Table.from_pydict(
    {"my_list": [["foo", "bar"], ["baz"]]},
    schema=schema_3,
)
pq.write_table(table_3, HERE / "list_string.parquet", **WRITER_KWARGS)

# 4. optional_list_i64: OPT outer, REQ inner (OPT-REP-REQ)
#    Records: [None, [7, 8]]
schema_4 = pa.schema([
    pa.field(
        "my_list",
        pa.list_(pa.field("element", pa.int64(), nullable=False)),
        nullable=True,
    ),
])
table_4 = pa.Table.from_pydict(
    {"my_list": [None, [7, 8]]},
    schema=schema_4,
)
pq.write_table(table_4, HERE / "optional_list_i64.parquet", **WRITER_KWARGS)

# 5. list_with_null_items: REQ outer, OPT inner (REQ-REP-OPT), full def-level matrix
#    Records: [[1, None, 2], [], [None, None]]
schema_5 = pa.schema([
    pa.field(
        "my_list",
        pa.list_(pa.field("element", pa.int64(), nullable=True)),
        nullable=False,
    ),
])
table_5 = pa.Table.from_pydict(
    {"my_list": [[1, None, 2], [], [None, None]]},
    schema=schema_5,
)
pq.write_table(table_5, HERE / "list_with_null_items.parquet", **WRITER_KWARGS)

# Verify each fixture's metadata
for name in [
    "list_i64_required",
    "list_i64_optional",
    "list_string",
    "optional_list_i64",
    "list_with_null_items",
]:
    path = HERE / f"{name}.parquet"
    pf = pq.ParquetFile(path)
    print(f"=== {name} ({path.stat().st_size} bytes, {pf.metadata.num_rows} rows) ===")
    print(pf.schema)
    # Print encodings of each column chunk for sanity
    for rg_i in range(pf.metadata.num_row_groups):
        rg = pf.metadata.row_group(rg_i)
        for c_i in range(rg.num_columns):
            col = rg.column(c_i)
            print(f"  rg{rg_i} col{c_i} path={col.path_in_schema} "
                  f"encodings={col.encodings} compression={col.compression}")
    print()
