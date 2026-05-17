# KesselDB Sub-project 52 — `kessel` CLI + developer experience

**Date:** 2026-05-17  **Status:** shipped, tested, smoke-verified. 146 green.
**Theme:** close the usability gap for humans *and* agents.

## The gap

Until now the only way to talk to KesselDB was to **write Rust** against
`kessel-client`. That is a hard wall for evaluation, scripting, ops, and —
critically — for AI agents driving the database. "No gaps" means a person
or an agent can install, run, and query without writing code.

## Delivered

### `kessel` — the command-line client

A zero-dependency binary in the `kessel-client` crate. Designed to be
equally good for humans and agents: line-oriented, deterministic, with
**meaningful exit codes** so an agent never has to scrape text to know if
a statement succeeded.

```
kessel [--addr HOST:PORT] [--token TOKEN] ["SQL"]
```

- **one-shot**: `kessel "SELECT SUM(v) FROM t"` → runs, prints the result,
  exits `0` on success / `1` on a statement error or connection failure.
- **pipe**: `echo "..." | kessel` → one statement per line; `#`/`--`
  comment lines and blanks are ignored (so `.sql` files just work).
- **shell**: a TTY with no SQL arg → an interactive `kessel>` prompt;
  `quit`/`exit`/`\q` to leave.
- `--token` for authenticated servers; `--help` for usage.

End-to-end smoke (live server): table create, insert, `SELECT SUM`
(scalar decoded), piped multi-statement with comments, and a bad statement
returning exit code `1` — all verified.

### `format_result` (library, tested)

A pure, total `OpResult -> String` renderer (`kessel_client::format_result`)
shared by the CLI and safe for any script/tool. Scalar replies (the common
16-byte aggregate `i128`) are decoded inline; opaque row blobs report
their size and point at `DESCRIBE`. Unit-tested for every `OpResult`
variant (never panics, never empty).

### Documentation for humans and agents

- **`AGENTS.md`** (repo root): a concise, machine-first operating guide —
  what the repo is, exact build/test/run/CLI commands, the wire protocol,
  the project invariants (TDD, claims = tests), and where specs live. The
  first file an agent should read.
- **`docs/USAGE.md`**: new *Command-line client* section with copy-paste
  recipes (one-shot, pipe a file, interactive, authenticated).
- **`README.md`**: Quick Start now leads with the CLI ("query it in one
  command, no Rust required") before the library example.

## Test (1 new, 146 total)

`format_result_is_readable_for_every_variant` — every `OpResult` renders
to a non-empty, human-meaningful line; scalar aggregate decodes to its
integer; never panics. The CLI's connection path reuses the
already-tested `Client`/`connect_authed`.

## Honest framing

Typed, columnar pretty-printing of `SELECT *` rows still needs the schema
(`DESCRIBE`); the CLI reports row bytes and says so rather than guessing —
a named, non-gating follow-up, not a silent omission.
