# The determinism seam

Everything above storage in KesselDB is a pure function over an
**injected** clock, disk, and network (`kessel-io`). Production
injects real I/O; `kessel-sim` injects a seeded, fault-injecting fake.
The whole database runs deterministically from one `u64` seed — this is
what makes a from-scratch VSR reimplementation verifiable rather than
hopeful.

`kessel-sm`, `kessel-catalog`, and `kessel-codec` contain **zero** I/O,
clock, or RNG calls.

Full text:
[Architecture → The determinism seam](overview.md#the-determinism-seam-foundational).
