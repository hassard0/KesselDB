# CLI — the `kessel` command-line client

The `kessel` binary is the line-oriented, JSON-capable client. It is the
fastest path for humans, scripts, ops, and agents — meaningful exit
codes (0 ok / 1 statement-or-connection error / 2 bad usage) mean you
do not have to scrape text to detect success.

See the full reference in [Usage guide (full) §2b. The `kessel` command-line
client](full-usage.md#2b-the-kessel-command-line-client) — interactive
shell commands, `--json` mode, pipe a `.sql` file, `--addr`/`--token`
remote auth.

For the binary wire protocol underneath, see
[Wire protocol](../reference/wire-protocol.md).
