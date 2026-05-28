# Wire protocol

Each message on the binary wire is length-prefixed:
`[u32 little-endian length][payload]`.

| First byte | Meaning |
|---|---|
| (none / op bytes) | `Op::encode()` request → `OpResult::encode()` reply |
| `0xFE` | `0xFE ++ utf8 SQL` → compiled server-side, `OpResult` reply |
| `0xFD` | session frame: `0xFD ++ client(u128 LE) ++ req(u64 LE) ++ Op::encode()` (exactly-once) |
| `0xFC` | auth handshake: `0xFC ++ token` → `Ok` / `Unauthorized` |
| `0xFB` | admin: request `ServerStats` |
| `0xFA` | admin: `0xFA ++ dest_dir` → snapshot |

This is intentionally tiny — any language can speak it with a socket
and the length framing. The `kessel-client` crate implements all of it;
[`clients/python/kesseldb.py`](https://github.com/hassard0/KesselDB/blob/main/clients/python/kesseldb.py)
is a stdlib-only Python reference.

Full reference:
[Usage guide (full) §12](../usage/full-usage.md#12-wire-protocol).
