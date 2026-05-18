# KesselDB Sub-project 92 — corruptible `FaultVfs` + clean-prefix proof

**Date:** 2026-05-18  **Status:** **partial, honestly scoped.**
Delivers the reusable disk-fault-injection primitive and proves the
invariant VSR safety rests on. The *multi-node* disk-fault-during-
view-change harness remains tracked (this slice unblocks it) — it is
**not** claimed as done.

## What

`kessel_io::FaultVfs<V: Vfs>` — a VFS wrapper around any inner VFS
(typically `MemVfs`):

- one deterministic, externally-armed fault: hit the *n*-th
  `write_at` to a file whose name contains `target`, with
  `FaultKind::Torn` (persist only the first half — a short frame) or
  `FaultKind::Err` (return an I/O error so the caller's `?`
  propagates);
- the fault plan is shared by clone via `Rc<RefCell<FaultPlan>>`, so
  a test holds one handle and every disk the cluster opens obeys it;
- **pass-through until `arm`ed** — wrapping a VFS in `FaultVfs`
  changes nothing, so every existing test is unaffected (verified:
  full suite still green).

## Verified

`kessel-storage::wal_torn_write_recovers_clean_committed_prefix`:
write 10 clean ops, arm a torn WAL write, write 10 more; reopen
`Storage` from the same disk and assert the recovered set is
**exactly** the ops before the tear — every one of them present, and
*nothing* at or after it (no partial / garbage op ever surfaces) —
and that this is deterministic run-to-run. This is precisely the
invariant a replica's WAL must satisfy for VSR to safely state-
transfer the rest after a crash: a recovered node is a well-defined
*lagging* node, never a corrupt one.

Full workspace regression **198 green** (was 197; +1 this proof),
seed-7 / determinism corpus intact, `FaultVfs` inert by default.

## Honest boundary (why this is "partial")

The original goal — byte corruption injected *precisely during a
view change*, multi-node — is **not** delivered here. Building it
faithfully needs plumbing the cluster simulator does not have:

- crash is modelled as `crashed = true` (the node stops responding);
  it is **never reopened from disk**, so a torn WAL write is invisible
  unless we add a real *reopen `StateMachine` from the (truncated)
  Vfs and rejoin VSR* path;
- `StateMachine::apply` has **no op-number replay guard**, so a
  reopened node (SM at op K, VSR log empty) that is then fed the
  committed log from 0 by the primary would **double-apply** ops —
  state transfer would have to be snapshot-based or the recovered
  node must reconstruct its VSR op-number/commit from the recovered
  SM.

Rushing a harness without that plumbing would only *look* like a
during-view-change test while actually testing nothing (the op stays
in the memtable; nothing observes the durability gap). Per the
project's no-overclaim discipline this is scoped as a follow-up, with
`FaultVfs` delivered as the genuine, reusable building block it
needs. Tracked, not faked.
