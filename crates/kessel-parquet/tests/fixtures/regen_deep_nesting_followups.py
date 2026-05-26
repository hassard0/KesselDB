"""SP146 T5: pyarrow deep-nesting follow-up fixtures (close OBJ-2c-5 arc).

Run with: py crates/kessel-parquet/tests/fixtures/regen_deep_nesting_followups.py

Produces 3 .parquet files exercising the 3 SP145-deferred cross-products
that SP146 closes:
  1. list_of_list_of_list_i64.parquet   List<List<List<i64>>>     (3-deep)
  2. list_of_map_string_i64.parquet     List<Map<string, i64>>
  3. map_string_map_string_i64.parquet  Map<string, Map<string, i64>>

All use PLAIN encoding (no dict) + UNCOMPRESSED data page V1 — matches the
pre-existing SP143/SP144/SP145 fixture style for byte-stable round-trips.
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

# 1. list_of_list_of_list_i64.parquet  (3-deep) ─────────────────────────────
# List<List<List<i64>>>: 3 nested LISTs, REQUIRED i64 leaf.
lll_type = pa.list_(pa.list_(pa.list_(pa.field('item', pa.int64(), nullable=False))))
t1 = pa.Table.from_pydict(
    {'my_lll': pa.array([
        [[[1, 2], [3]], [[4]]],
        [[[10]]],
        [[[20], [30]]],
    ], type=lll_type)},
    schema=pa.schema([pa.field('my_lll', lll_type, nullable=False)]),
)
pq.write_table(t1, HERE / 'list_of_list_of_list_i64.parquet', **WRITER_KWARGS)

# 2. list_of_map_string_i64.parquet  ────────────────────────────────────────
# List<Map<string, i64>>: outer LIST of inner Maps with primitive K/V.
lom_type = pa.list_(pa.field(
    'item',
    pa.map_(pa.string(), pa.field('value', pa.int64(), nullable=False)),
    nullable=False,
))
t2 = pa.Table.from_pydict(
    {'my_lom': pa.array([
        [[('a', 1), ('b', 2)], [('c', 3)]],
        [[('only', 99)]],
    ], type=lom_type)},
    schema=pa.schema([pa.field('my_lom', lom_type, nullable=False)]),
)
pq.write_table(t2, HERE / 'list_of_map_string_i64.parquet', **WRITER_KWARGS)

# 3. map_string_map_string_i64.parquet  ────────────────────────────────────
# Map<string, Map<string, i64>>: outer Map of inner Maps.
mom_type = pa.map_(
    pa.string(),
    pa.field(
        'value',
        pa.map_(pa.string(), pa.field('value', pa.int64(), nullable=False)),
        nullable=False,
    ),
)
t3 = pa.Table.from_pydict(
    {'my_mom': pa.array([
        [('alpha', [('x', 1), ('y', 2)]),
         ('beta',  [('z', 3)])],
        [('gamma', [('k', 99)])],
    ], type=mom_type)},
    schema=pa.schema([pa.field('my_mom', mom_type, nullable=False)]),
)
pq.write_table(t3, HERE / 'map_string_map_string_i64.parquet', **WRITER_KWARGS)

# Verify each fixture's schema + row count
for name in [
    'list_of_list_of_list_i64',
    'list_of_map_string_i64',
    'map_string_map_string_i64',
]:
    path = HERE / f'{name}.parquet'
    pf = pq.ParquetFile(path)
    print(f'{name}: rows={pf.metadata.num_rows}')
    print(f'  schema: {pf.schema}')
    print()
