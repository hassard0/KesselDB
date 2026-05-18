"""kesseldb — a dependency-free Python client for KesselDB.

Speaks the documented wire protocol (docs/USAGE.md §10): every message
is ``[u32 little-endian length][payload]``. A SQL request payload is
``0xFE ++ utf8``; the reply is an encoded ``OpResult``. Auth (if the
server requires a token) is a first frame ``0xFC ++ token``.

Standard library only — no third-party dependencies (mirroring the
database's zero-dependency philosophy; the dependency-free rule is
about the engine, and a single-file client honours it too).

    from kesseldb import connect

    db = connect("127.0.0.1:7878")            # or connect(host, port, token=b"..")
    db.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)")
    db.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)")
    r = db.sql("SELECT SUM(bal) FROM acct WHERE owner = 100")
    print(r.ok, r.value)                       # True 50
    db.close()

Run as a script for a one-shot query (exit 0 ok / 1 error / 2 usage):

    python kesseldb.py "SELECT SUM(bal) FROM acct" [--addr H:P] [--token T]
"""

from __future__ import annotations

import socket
import struct
import sys
from dataclasses import dataclass

SQL_TAG = 0xFE
AUTH_TAG = 0xFC


@dataclass
class OpResult:
    """A decoded server reply. ``kind`` is the wire tag; the helpers
    cover the cases a SQL client cares about."""

    kind: int
    value: int | None = None        # scalar (16-byte i128) for SUM/COUNT/…
    raw: bytes | None = None        # opaque Got bytes (rows, schema, …)
    type_id: int | None = None      # TypeCreated
    message: str | None = None      # SchemaError / Constraint text

    KIND = {
        0: "ok", 1: "got", 2: "exists", 3: "not_found", 4: "type_created",
        5: "error", 6: "constraint", 7: "unavailable", 8: "unauthorized",
    }

    @property
    def ok(self) -> bool:
        # A successful statement: Ok / Got / TypeCreated. (Exists /
        # NotFound are not errors but not "ok" either — inspect .kind.)
        return self.kind in (0, 1, 4)

    @property
    def name(self) -> str:
        return self.KIND.get(self.kind, f"kind#{self.kind}")

    def __repr__(self) -> str:
        if self.kind == 1:
            return f"OpResult(got, value={self.value}, {len(self.raw or b'')}B)"
        if self.kind == 4:
            return f"OpResult(type_created={self.type_id})"
        if self.kind in (5, 6):
            return f"OpResult({self.name}: {self.message!r})"
        return f"OpResult({self.name})"


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("kesseldb: connection closed mid-frame")
        buf.extend(chunk)
    return bytes(buf)


def _read_frame(sock: socket.socket) -> bytes:
    (n,) = struct.unpack("<I", _read_exact(sock, 4))
    return _read_exact(sock, n)


def _write_frame(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(struct.pack("<I", len(payload)) + payload)


def _decode_opresult(b: bytes) -> OpResult:
    if not b:
        raise ValueError("kesseldb: empty reply frame")
    tag = b[0]
    if tag in (0, 2, 3, 7, 8):
        return OpResult(tag)
    if tag == 4:  # TypeCreated(u32)
        (tid,) = struct.unpack_from("<I", b, 1)
        return OpResult(4, type_id=tid)
    if tag == 1:  # Got: [u32 len][bytes]
        (ln,) = struct.unpack_from("<I", b, 1)
        raw = b[5:5 + ln]
        # The common scalar reply (aggregate result) is a 16-byte i128.
        val = (
            int.from_bytes(raw, "little", signed=True)
            if len(raw) == 16
            else None
        )
        return OpResult(1, value=val, raw=raw)
    if tag in (5, 6):  # SchemaError / Constraint: [u32 len][utf8]
        (ln,) = struct.unpack_from("<I", b, 1)
        return OpResult(tag, message=b[5:5 + ln].decode("utf-8", "replace"))
    raise ValueError(f"kesseldb: unknown OpResult tag {tag}")


class Client:
    """A blocking KesselDB connection. Not thread-safe (one socket);
    use one Client per thread, like ``kessel-client``."""

    def __init__(self, host: str, port: int, token: bytes | None = None):
        self._sock = socket.create_connection((host, port))
        # Small synchronous request/response — disable Nagle (matches
        # the Rust client; ~40ms/round-trip cliff otherwise on Linux).
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if token is not None:
            _write_frame(self._sock, bytes([AUTH_TAG]) + token)
            if _decode_opresult(_read_frame(self._sock)).kind != 0:
                self.close()
                raise PermissionError("kesseldb: unauthorized (bad token)")

    def sql(self, statement: str) -> OpResult:
        """Run one SQL statement; return its decoded result."""
        _write_frame(self._sock, bytes([SQL_TAG]) + statement.encode("utf-8"))
        return _decode_opresult(_read_frame(self._sock))

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


def connect(addr: str, port: int | None = None,
            token: bytes | None = None) -> Client:
    """``connect("host:port")`` or ``connect("host", port)``."""
    if port is None:
        host, _, p = addr.rpartition(":")
        return Client(host or "127.0.0.1", int(p), token)
    return Client(addr, port, token)


def _main(argv: list[str]) -> int:
    addr, token, parts = "127.0.0.1:7878", None, []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--addr" and i + 1 < len(argv):
            addr = argv[i + 1]; i += 2
        elif a == "--token" and i + 1 < len(argv):
            token = argv[i + 1].encode(); i += 2
        else:
            parts.append(a); i += 1
    if not parts:
        sys.stderr.write(
            "usage: python kesseldb.py \"SQL\" [--addr H:P] [--token T]\n")
        return 2
    try:
        db = connect(addr, token=token)
    except OSError as e:
        sys.stderr.write(f"kesseldb: cannot connect to {addr}: {e}\n")
        return 1
    r = db.sql(" ".join(parts))
    db.close()
    if r.kind == 1 and r.value is not None:
        print(f"= {r.value}")
    elif r.kind in (5, 6):
        print(f"{r.name.upper()}  {r.message}")
        return 1
    else:
        print(r.name.upper())
    return 0 if r.ok or r.kind in (2, 3) else 1


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
