# External sources & Parquet

Register a named table whose rows are populated from a remote
JSON/NDJSON/CSV/Parquet endpoint or an S3-compatible / Azure Blob
object, then query it with ordinary SQL.

- **HTTP / HTTPS sources** — `--features external-sources` (HTTP) or
  `--features external-sources-tls` (adds HTTPS via rustls).
- **Object-store sources** — `--features external-sources-objstore`
  (implies `external-sources-tls`); supports `s3://` and `az://`.

The pure-Rust zero-dep Parquet reader supports the full pyarrow 24.0.0
matrix (`UNCOMPRESSED + Snappy + GZIP + zstd + LZ4_RAW + Brotli × PLAIN
+ dictionary × V1 + V2 pages × flat REQUIRED + OPTIONAL +
LIST<primitive> + MAP<K,V> + struct + 3-deep cross-products × INT32 +
INT64 + INT96 + DECIMAL(≤38) + FLBA + BYTE_ARRAY`).

Reference:
[Usage guide (full) §7c–7f](full-usage.md#7c-external-sources-jsoncsv-over-http).
The Parquet capability matrix lives in
[`README.md`](https://github.com/hassard0/KesselDB/blob/main/README.md#parquet-capability-matrix).
