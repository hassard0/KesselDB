"""SP145 T7: pyarrow deep-nesting fixtures (final OBJ-2c-5 slice).

Run with: py crates/kessel-parquet/tests/fixtures/regen_deep_nesting.py

Produces 6 .parquet files exercising the 4 SP145 lift shapes plus 2 BOLD
cross-products:
  1. list_of_list_i64.parquet           List<List<i64>>
  2. list_of_struct.parquet             List<struct<id:i64, name:string>>
  3. map_string_struct.parquet          Map<string, struct<count:i64, ratio:f64>>
  4. struct_with_list_field.parquet     struct<id:i64, tags:List<string>>
  5. struct_with_struct_field.parquet   struct<id:i64, inner:struct<a:i64, b:bool>>
  6. map_string_list_string.parquet     Map<string, List<string>>  (BOLD cross-product)

All use PLAIN encoding (no dict) + UNCOMPRESSED data page V1 (matches the
pre-existing SP141/SP143/SP144 fixtures).
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

# 1. list_of_list_i64.parquet  ───────────────────────────────────────────────
# List<List<i64>>: outer List, inner List, REQUIRED i64 leaf.
ll_type = pa.list_(pa.list_(pa.field('item', pa.int64(), nullable=False)))
t1 = pa.Table.from_pydict(
    {'my_lol': pa.array([
        [[1, 2, 3], [4, 5]],
        [[10]],
        [[], [100, 200]],
    ], type=ll_type)},
    schema=pa.schema([pa.field('my_lol', ll_type, nullable=False)]),
)
pq.write_table(t1, HERE / 'list_of_list_i64.parquet', **WRITER_KWARGS)

# 2. list_of_struct.parquet  ────────────────────────────────────────────────
# List<struct<id:i64, name:string>>: outer List, inner struct of 2 primitives.
struct_in_list_type = pa.struct([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=False),
])
los_type = pa.list_(pa.field('item', struct_in_list_type, nullable=False))
t2 = pa.Table.from_pydict(
    {'my_los': pa.array([
        [{'id': 1, 'name': 'alice'}, {'id': 2, 'name': 'bob'}],
        [{'id': 99, 'name': 'zoe'}],
    ], type=los_type)},
    schema=pa.schema([pa.field('my_los', los_type, nullable=False)]),
)
pq.write_table(t2, HERE / 'list_of_struct.parquet', **WRITER_KWARGS)

# 3. map_string_struct.parquet  ─────────────────────────────────────────────
# Map<string, struct<count:i64, ratio:f64>>: REQ outer, struct value.
struct_in_map_type = pa.struct([
    pa.field('count', pa.int64(), nullable=False),
    pa.field('ratio', pa.float64(), nullable=False),
])
mss_type = pa.map_(
    pa.string(),
    pa.field('value', struct_in_map_type, nullable=False),
)
t3 = pa.Table.from_pydict(
    {'my_mss': pa.array([
        [('alpha', {'count': 1, 'ratio': 0.5}),
         ('beta',  {'count': 2, 'ratio': 1.5})],
        [('gamma', {'count': 99, 'ratio': 3.14})],
    ], type=mss_type)},
    schema=pa.schema([pa.field('my_mss', mss_type, nullable=False)]),
)
pq.write_table(t3, HERE / 'map_string_struct.parquet', **WRITER_KWARGS)

# 4. struct_with_list_field.parquet  ────────────────────────────────────────
# struct<id:i64, tags:List<string>>: outer struct contains one nested LIST.
swl_type = pa.struct([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('tags', pa.list_(pa.field('item', pa.string(), nullable=False)),
             nullable=False),
])
t4 = pa.Table.from_pydict(
    {'my_swl': pa.array([
        {'id': 1, 'tags': ['rust', 'parquet']},
        {'id': 2, 'tags': []},
        {'id': 3, 'tags': ['nested']},
    ], type=swl_type)},
    schema=pa.schema([pa.field('my_swl', swl_type, nullable=False)]),
)
pq.write_table(t4, HERE / 'struct_with_list_field.parquet', **WRITER_KWARGS)

# 5. struct_with_struct_field.parquet  ──────────────────────────────────────
# struct<id:i64, inner:struct<a:i64, b:bool>>: outer struct contains a nested struct.
inner_struct_type = pa.struct([
    pa.field('a', pa.int64(), nullable=False),
    pa.field('b', pa.bool_(), nullable=False),
])
sws_type = pa.struct([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('inner', inner_struct_type, nullable=False),
])
t5 = pa.Table.from_pydict(
    {'my_sws': pa.array([
        {'id': 1, 'inner': {'a': 10, 'b': True}},
        {'id': 2, 'inner': {'a': 20, 'b': False}},
    ], type=sws_type)},
    schema=pa.schema([pa.field('my_sws', sws_type, nullable=False)]),
)
pq.write_table(t5, HERE / 'struct_with_struct_field.parquet', **WRITER_KWARGS)

# 6. map_string_list_string.parquet  (BOLD cross-product) ───────────────────
# Map<string, List<string>>: tests the Map<_, List<_>> cross-product path.
list_of_string_type = pa.list_(pa.field('item', pa.string(), nullable=False))
msls_type = pa.map_(
    pa.string(),
    pa.field('value', list_of_string_type, nullable=False),
)
t6 = pa.Table.from_pydict(
    {'my_msls': pa.array([
        [('langs', ['rust', 'go']),
         ('frameworks', ['axum', 'tokio'])],
        [('single', ['only'])],
    ], type=msls_type)},
    schema=pa.schema([pa.field('my_msls', msls_type, nullable=False)]),
)
pq.write_table(t6, HERE / 'map_string_list_string.parquet', **WRITER_KWARGS)

# Verify each fixture's schema + row count
for name in [
    'list_of_list_i64', 'list_of_struct', 'map_string_struct',
    'struct_with_list_field', 'struct_with_struct_field',
    'map_string_list_string',
]:
    path = HERE / f'{name}.parquet'
    pf = pq.ParquetFile(path)
    print(f'{name}: rows={pf.metadata.num_rows}')
    print(f'  schema: {pf.schema}')
    print()
