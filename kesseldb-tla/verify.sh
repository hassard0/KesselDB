#!/usr/bin/env bash
# kesseldb-tla/verify.sh — POSIX TLC runner for the S1 replication-safety spec.
# Thin wrapper: detects tla2tools.jar via $TLA2TOOLS_JAR or $TLC_JAR env var,
# then invokes TLC against Replication.tla / Replication.cfg in this directory.
# Requires: java 11+ on PATH; TLA2TOOLS_JAR (or TLC_JAR) set to tla2tools.jar.
set -euo pipefail

# Accept either $TLA2TOOLS_JAR (canonical name per spec) or $TLC_JAR (plan alias).
TLA_JAR="${TLA2TOOLS_JAR:-${TLC_JAR:-}}"

if [ -z "$TLA_JAR" ]; then
    echo "verify.sh: ERROR — jar not found." >&2
    echo "" >&2
    echo "Set TLA2TOOLS_JAR (or TLC_JAR) to the path of tla2tools.jar, e.g.:" >&2
    echo "  export TLA2TOOLS_JAR=/path/to/tla2tools.jar" >&2
    echo "" >&2
    echo "Download from: https://github.com/tlaplus/tlaplus/releases/latest" >&2
    exit 2
fi

if [ ! -f "$TLA_JAR" ]; then
    echo "verify.sh: ERROR — jar not found at: $TLA_JAR" >&2
    exit 2
fi

if ! command -v java >/dev/null 2>&1; then
    echo "verify.sh: ERROR — java not found on PATH." >&2
    echo "Install Java 11+ and ensure it is on PATH." >&2
    exit 2
fi

# Change to the directory containing this script (works even when called from repo root).
cd "$(dirname "$0")"

STAMP="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
OUT="results/${STAMP}.txt"
mkdir -p results

echo "Running TLC on Replication.tla / Replication.cfg ..."
echo "Output teed to: $OUT"

java -XX:+UseParallelGC -cp "$TLA_JAR" tlc2.TLC \
    -workers auto \
    -config Replication.cfg \
    Replication 2>&1 | tee "$OUT"

RC=${PIPESTATUS[0]}
echo ""
echo "TLC exit code: $RC"
exit "$RC"
