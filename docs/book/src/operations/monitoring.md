# Backup & monitoring

Backup and metrics both run on the engine thread, so a snapshot is
crash-consistent and metrics are exact.

- **Hot consistent snapshot** — `engine.snapshot("./backup-DATE")?`.
  Recover with `StateMachine::open("./backup-DATE")`; the recovered
  digest matches the live one byte-for-byte.
- **Live metrics** — `engine.stats()` returns
  `ServerStats { applied_ops, digest, uptime_secs }`. In a cluster,
  `Node::probe()` returns `(digest, op_number, commit)` so you can
  detect replica divergence by comparing across nodes.
- **Prometheus** — with `--features http-gateway`, scrape `/v1/metrics`
  for `kesseldb_ops_total` / `kesseldb_inflight` / `kesseldb_view_number`
  / `kesseldb_last_op_number` / `kesseldb_is_primary` (cardinality
  bounded by design).

Full reference:
[Usage guide (full) §11](../usage/full-usage.md#11-backup--monitoring).
