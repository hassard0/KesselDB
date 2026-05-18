# KesselDB performance

Honest numbers, the model behind them, and order-of-magnitude
projections for common cloud configurations.

> **What is measured vs projected.** The tables under *Measured* are
> real runs on two reference machines (described generically below).
> The *Cloud projections* table is **extrapolated from the measured
> single-core throughput plus the storage/network model** — it is
> **not** measured on those instances. Treat it as planning
> guidance, not a benchmark result.

## Reference machines

- **Reference server** — a 16‑core x86‑64 Linux box (≈3 GHz class),
  local NVMe SSD, loopback networking. Shared/old disk near capacity,
  so durable numbers are conservative.
- **Reference laptop** — an x86‑64 Windows 11 developer laptop.

No tuning, default build (zero external dependencies), single
deterministic writer thread.

## The model (why the projections are what they are)

KesselDB is a single deterministic state machine. That fixes how it
scales, and the projections fall straight out of it:

1. **Steady-state op throughput is single-core-bound.** One writer
   applies operations in order. Throughput tracks **per-core
   clock × IPC**, *not* vCPU count. More cores buy connection
   concurrency and read parallelism, not a higher write rate.
2. **Durable throughput is fsync-latency-bound.** Server-side group
   commit amortises one `fsync` over the whole in-flight batch, so
   effective durable rate ≈ *batch size ÷ fsync latency*. Fast local
   NVMe (~50–200 µs) → tens of thousands/s; network-attached volumes
   (~0.5–2 ms) → still thousands–tens of thousands/s **because** the
   batch grows under load.
3. **Latency is round-trip-bound.** `TCP_NODELAY` removes the
   Nagle/delayed-ACK stall (a ~40 ms/round-trip cliff on Linux);
   pipelining and group commit amortise the remaining RTT.
4. **Indexed and columnar reads are sub-linear** and CPU/cache-bound,
   so they track single-core performance and are largely independent
   of table size.

## Measured

Single connection / single thread unless noted.

| Path | Reference server | Notes |
|---|---|---|
| State-machine create (in-mem, 128 B) | ~215 K ops/s @ p50 ~2 µs | CPU-bound |
| Durable create, group commit (~1 K batch) | ~87 K ops/s | local NVMe |
| Concurrent durable, 8 clients | ~1,870 ops/s | group commit + `TCP_NODELAY`; near-full shared disk (conservative) |
| Pipelined batch, 1 connection | ~52,700 ops/s | N statements per round-trip |
| SQL compile (prepared-statement cache) | ~574 K → ~15 M stmt/s | cold → cached |
| Range/band query, range index (40 K rows, ~0.2 % selected) | ~35 ms → ~0.31 ms (**~112×**) | order-index narrowed; equals full-scan result (oracle-checked) |
| `MIN`/`MAX`, order-indexed column (40 K rows) | ~23 ms → **~5 µs** (**~4,600×**) | columnar fast-path: answered from the index extreme, no scan |

The columnar fast-path is also ~1,800× on the reference laptop
(~14 µs vs ~23 ms) — the absolute µs differs with single-core speed;
the *shape* (sub-linear, scan eliminated) does not.

Every figure is reproducible from the test suite / `kessel-bench`, and
each query accelerator is guarded by a randomized equivalence oracle
(the accelerated result is proven identical to the brute-force scan).

## Cloud projections (extrapolated — not measured)

Applying the model to representative instance families. Single-core
class is the dominant factor for CPU-bound paths; storage class for
durable writes. **Projection, not a benchmark.**

| Configuration | In-mem ops/s (1 core) | Durable ops/s (pipelined/concurrent) | Indexed/columnar reads |
|---|---|---|---|
| Modern compute-optimized VM (e.g. AWS c7i/c7g, GCP c3, Azure Fsv2), **local NVMe** | ~250–350 K | ~50–150 K (sub-ms fsync) | sub-linear; ~µs `MIN`/`MAX`, ~sub-ms band scans |
| General-purpose VM (AWS m7i, GCP n2, Azure Dsv5), **local NVMe** | ~180–280 K | ~40–120 K | same shape, ~per-core slower |
| Same, **network SSD** (EBS gp3, GCP pd-ssd, Azure Premium) | ~180–350 K | ~10–40 K (group commit hides ~0.5–2 ms fsync; rises with concurrency) | unaffected (read-side) |
| Burstable/small VM (AWS t-class, etc.) | ~80–150 K | ~5–20 K | unaffected in shape; lower absolute |

Reading the table:

- **In-mem / compile / read paths** scale with single-core speed and
  are roughly cloud-instance-independent beyond clock/IPC — pick the
  fastest single core, not the most vCPUs.
- **Durable writes** depend almost entirely on storage. Network SSD
  has higher per-`fsync` latency, but group commit makes the durable
  rate climb with offered concurrency, so a busy server still reaches
  tens of thousands/s. For the lowest write latency, prefer local
  NVMe (instance store) and accept its ephemerality, or replicate.
- **Columnar / indexed reads** (`MIN`/`MAX` via the order index,
  range/band narrowing, prepared-statement cache) are sub-linear and
  table-size-independent — the projections there are about absolute
  µs, not whether the optimisation applies.

## Reproducing

```bash
cargo test --workspace --release        # functional + equivalence oracles
cargo run -p kessel-bench --release -- --help
```

Numbers move with hardware; the **relationships** in the model
(single-core-bound ops, fsync-bound durability, sub-linear indexed
reads, scan-free `MIN`/`MAX`) hold across platforms.
