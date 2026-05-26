#!/usr/bin/env python3
"""One-shot regenerator for `brotli_dictionary.bin` (SP154 / L10).

Fetches the official Brotli reference dictionary (RFC 7932 Appendix A)
from the upstream `google/brotli` GitHub repo and writes it to
`crates/kessel-parquet/src/brotli_dictionary.bin`. The file is checked
into version control; this script is a fixture-only reproducibility
helper — NOT a build dependency. The decoder uses `include_bytes!` at
compile time.

The expected output is exactly **122,784 bytes** (the full Appendix A
blob — sequences of words of length 4..=24, with per-length offset and
count tables baked into the decoder).

Usage:
    python crates/kessel-parquet/tools/regen_brotli_dictionary.py

If the upstream URL is unreachable, this script will FAIL with a clear
error message. The dictionary blob is fully determined by RFC 7932
Appendix A; the upstream `dictionary.bin` is the canonical
binary-identical representation.
"""

from __future__ import annotations

import hashlib
import os
import sys
import urllib.request

URL = (
    "https://raw.githubusercontent.com/google/brotli/"
    "v1.1.0/c/common/dictionary.bin"
)
# SHA-256 of the v1.1.0 dictionary.bin per upstream commit (pinned for
# byte-for-byte reproducibility — see RFC 7932 Appendix A).
EXPECTED_SHA256 = "20e42eb1b511c21806d4d227d07e5dd06877d8ce7b3a817f378f313653f35c70"
EXPECTED_SIZE = 122_784

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
OUT_PATH = os.path.join(
    REPO_ROOT,
    "crates",
    "kessel-parquet",
    "src",
    "brotli_dictionary.bin",
)


def main() -> int:
    print(f"Fetching: {URL}")
    try:
        with urllib.request.urlopen(URL, timeout=30) as resp:
            data = resp.read()
    except Exception as e:
        print(f"ERROR: failed to fetch upstream dictionary: {e}", file=sys.stderr)
        return 1

    size = len(data)
    sha = hashlib.sha256(data).hexdigest()
    print(f"  size = {size} bytes (expected {EXPECTED_SIZE})")
    print(f"  sha256 = {sha}")
    if size != EXPECTED_SIZE:
        print(
            f"ERROR: size mismatch ({size} != {EXPECTED_SIZE})",
            file=sys.stderr,
        )
        return 1
    if sha != EXPECTED_SHA256:
        print(
            f"WARNING: sha256 mismatch (expected {EXPECTED_SHA256}). "
            "Upstream may have rebased; verify before committing.",
            file=sys.stderr,
        )
        # Don't fail hard — but loudly flag the operator.

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "wb") as f:
        f.write(data)
    print(f"Wrote {OUT_PATH} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
