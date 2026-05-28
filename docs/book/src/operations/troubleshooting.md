# Troubleshooting

Common symptoms and the underlying cause:

| Symptom | Cause / fix |
|---|---|
| `OpResult::Unavailable` | Not the primary, or the engine is shedding load. Use `ClusterClient` (auto-rotates) or retry. |
| `OpResult::Unauthorized` | Missing/incorrect token. Use `connect_authed` / `with_token` with `ServerConfig.token`. |
| `OpResult::Constraint(msg)` | A NOT NULL / UNIQUE / FK / CHECK rejected the write. This *is* a deterministic committed result. |
| `OpResult::SchemaError(msg)` | Bad SQL, unknown table/column, malformed frame. The message names the problem. |
| `server closed the connection unexpectedly` from psql | Not built with `--features pg-gateway`, or `KESSELDB_PG_ADDR` / `KESSELDB_TOKEN` unset. |
| `FATAL: invalid_authorization_specification` | Bearer token mismatch on the SCRAM path. |
| `FATAL: sorry, too many clients already` (53300) | `pg_max_conns` hit (default 256). Raise via `ServerConfig.pg_max_conns`. |

Full table including the HTTP gateway error map and the Parquet
typed-Unsupported messages:
[Usage guide (full) §13](../usage/full-usage.md#13-troubleshooting)
and [§9 Troubleshooting](../usage/full-usage.md#troubleshooting).
