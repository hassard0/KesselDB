"""SP150 T2: pyarrow Brotli fixture.

Run from repo root:
    py crates/kessel-parquet/tests/fixtures/regen_brotli.py

KesselDB SP150 recognizes parquet codec id 4 (BROTLI) at meta-decode time
(`Codec::Brotli`) but does NOT yet implement Brotli decompression — a
zero-dep RFC 7932 decoder is a dedicated multi-week SP-arc (~10-15 tasks
like SP125-SP140 zstd). This fixture exists so:

  * the `#[ignore]`'d round-trip test in fixture_roundtrip.rs is ready to
    activate the moment a Brotli decoder ships;
  * the active rejection-lock test pins the named-follow-up error
    message until then.

The output parquet should report `codec=BROTLI` for both columns.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

HERE = Path(__file__).resolve().parent

table = pa.Table.from_pydict({
    'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    'name': pa.array(['alice', 'bob', 'carol', 'dave', 'eve'], type=pa.string()),
}, schema=pa.schema([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=False),
]))

pq.write_table(
    table,
    HERE / 'brotli_flat.parquet',
    compression='brotli',
    use_dictionary=False,
    data_page_version='1.0',
    version='1.0',
)

pf = pq.ParquetFile(HERE / 'brotli_flat.parquet')
print(f"rows={pf.metadata.num_rows}")
for i in range(pf.metadata.num_row_groups):
    rg = pf.metadata.row_group(i)
    for j in range(rg.num_columns):
        col = rg.column(j)
        print(f"  col[{j}] {col.path_in_schema}: codec={col.compression}")
