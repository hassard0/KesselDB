# Install

KesselDB is pure Rust with no external dependencies in the kernel and
no native build steps.

- **Prebuilt Linux x86_64 binary** — grab from the
  [releases page](https://github.com/hassard0/KesselDB/releases). Each
  release ships a server binary (`kesseldb`), a CLI binary (`kessel`),
  a bundle tarball, and `SHA256SUMS`.
- **Build from source** —
  ```bash
  git clone https://github.com/hassard0/KesselDB && cd KesselDB
  cargo build --release                                       # default — binary protocol only
  cargo build --release --features pg-gateway,http-gateway    # all wire surfaces
  ```
  Requires Rust stable 1.95+.

Full install + build matrix:
[Usage guide (full) §1](../usage/full-usage.md#1-install--build).
