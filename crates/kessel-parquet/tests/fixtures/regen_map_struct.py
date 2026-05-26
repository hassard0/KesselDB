"""SP144 T7: pyarrow Map<K,V> + struct fixtures.

Run with: py crates/kessel-parquet/tests/fixtures/regen_map_struct.py

Produces 5 .parquet files exercising the canonical 3-node MAP encoding
and the struct-of-primitives pattern. All use PLAIN encoding (no dict)
and UNCOMPRESSED data page V1 (matches pre-existing SP141/SP143 fixtures).
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

HERE = Path(__file__).resolve().parent
WRITER_KWARGS = dict(
    compression='NONE',
    use_dictionary=False,
    data_page_version='1.0',
    version='1.0',
)

# 1. map_string_i64.parquet (REQ outer, REQ-i64 value)
map_string_i64_type = pa.map_(
    pa.string(),
    pa.field('value', pa.int64(), nullable=False),
)
t1 = pa.Table.from_pydict(
    {'my_map': pa.array([
        [('a', 1), ('b', 2)],
        [('x', 7)],
    ], type=map_string_i64_type)},
    schema=pa.schema([pa.field('my_map', map_string_i64_type, nullable=False)]),
)
pq.write_table(t1, HERE / 'map_string_i64.parquet', **WRITER_KWARGS)

# 2. optional_map_string_i64.parquet (OPT outer)
t2 = pa.Table.from_pydict(
    {'my_map': pa.array([
        None,
        [('k', 42)],
    ], type=map_string_i64_type)},
    schema=pa.schema([pa.field('my_map', map_string_i64_type, nullable=True)]),
)
pq.write_table(t2, HERE / 'optional_map_string_i64.parquet', **WRITER_KWARGS)

# 3. map_string_string.parquet
map_string_string_type = pa.map_(
    pa.string(),
    pa.field('value', pa.string(), nullable=False),
)
t3 = pa.Table.from_pydict(
    {'my_map': pa.array([
        [('lang', 'rust'), ('ver', '1.95')],
    ], type=map_string_string_type)},
    schema=pa.schema([pa.field('my_map', map_string_string_type, nullable=False)]),
)
pq.write_table(t3, HERE / 'map_string_string.parquet', **WRITER_KWARGS)

# 4. struct_i64_string.parquet (REQ outer)
struct_i64_string_type = pa.struct([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=False),
])
t4 = pa.Table.from_pydict(
    {'my_struct': pa.array([
        {'id': 1, 'name': 'alice'},
        {'id': 2, 'name': 'bob'},
    ], type=struct_i64_string_type)},
    schema=pa.schema([pa.field('my_struct', struct_i64_string_type, nullable=False)]),
)
pq.write_table(t4, HERE / 'struct_i64_string.parquet', **WRITER_KWARGS)

# 5. optional_struct.parquet (OPT outer)
t5 = pa.Table.from_pydict(
    {'my_struct': pa.array([
        {'id': 1, 'name': 'alice'},
        None,
        {'id': 3, 'name': 'carol'},
    ], type=struct_i64_string_type)},
    schema=pa.schema([pa.field('my_struct', struct_i64_string_type, nullable=True)]),
)
pq.write_table(t5, HERE / 'optional_struct.parquet', **WRITER_KWARGS)

# Verify each fixture's schema + row count
for name in [
    'map_string_i64', 'optional_map_string_i64', 'map_string_string',
    'struct_i64_string', 'optional_struct',
]:
    path = HERE / f'{name}.parquet'
    pf = pq.ParquetFile(path)
    print(f'{name}: rows={pf.metadata.num_rows}')
    print(f'  schema: {pf.schema}')
    print()
