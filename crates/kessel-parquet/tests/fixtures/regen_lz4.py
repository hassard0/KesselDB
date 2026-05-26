"""SP149 T3: pyarrow LZ4_RAW fixture.

Run from repo root:
    py crates/kessel-parquet/tests/fixtures/regen_lz4.py

Pyarrow v8+ maps `compression='lz4'` to LZ4_RAW (parquet codec id 7) —
the modern raw-block format with no Hadoop-style framing. The output
parquet should report `codec=LZ4_RAW` for both columns; if it reports
`codec=LZ4` (id 5) instead, the runtime falls back to the legacy
deprecated codec and the round-trip test will fail with a named
Unsupported error from the SP149 dispatch arm.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

HERE = Path(__file__).resolve().parent

# LZ4_RAW: pyarrow's default for compression='lz4'.
table = pa.Table.from_pydict({
    'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    'name': pa.array(['alice', 'bob', 'carol', 'dave', 'eve'], type=pa.string()),
}, schema=pa.schema([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=False),
]))

pq.write_table(
    table,
    HERE / 'lz4_raw_flat.parquet',
    compression='lz4',     # pyarrow maps to LZ4_RAW for v2.6+
    use_dictionary=False,
    data_page_version='1.0',
    version='1.0',
)

pf = pq.ParquetFile(HERE / 'lz4_raw_flat.parquet')
print(f"rows={pf.metadata.num_rows}")
for i in range(pf.metadata.num_row_groups):
    rg = pf.metadata.row_group(i)
    for j in range(rg.num_columns):
        col = rg.column(j)
        print(f"  col[{j}] {col.path_in_schema}: codec={col.compression} encodings={col.encodings}")
