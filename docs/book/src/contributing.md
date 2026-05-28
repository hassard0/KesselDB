# Contributing

Issues and PRs welcome. The project rule is simple and strict: **every
change is test-driven, the full suite stays green, and
documentation/claims never exceed what the tests prove.** Each unit of
work ships as one reviewed slice with its own spec under
`docs/superpowers/specs/`.

See [Agents guide](reference/agents-guide.md) for the full machine-first
operating rules (test-driven discipline, zero-external-dependency rule,
per-slice spec, determinism is sacred, etc.).

The CI workflow on every push runs `cargo test --workspace` plus the
feature-flagged matrix (`pg-gateway`, `http-gateway`). The release
workflow on `v*` tags builds Linux + Windows + macOS binaries and
uploads them to the GitHub Releases page.
