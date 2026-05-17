//! kessel-vsr: Viewstamped Replication (crash-stop), ported from the
//! TigerBeetle / Oki-Liskov design, driven entirely through a deterministic
//! in-process bus so the simulator can reproduce a run from one seed.
//!
//! Scope (Sub-project 1 / M3): normal-case replication + group commit,
//! client table (exactly-once), primary-failover view change with log
//! recovery, and gap state-transfer. Real socket transport and membership
//! reconfiguration are explicitly deferred (documented in ARCHITECTURE.md);
//! the protocol is transport-agnostic so that swap is mechanical.

#![forbid(unsafe_code)]

use kessel_io::Vfs;
use kessel_proto::{ClientId, Op, OpResult};
use kessel_sm::StateMachine;
use std::collections::{HashMap, HashSet};

const PRIMARY_TIMEOUT_TICKS: u64 = 8;

#[derive(Clone)]
pub struct LogEntry {
    pub op_number: u64,
    pub client: ClientId,
    pub req: u64,
    pub op: Op,
}

#[derive(Clone)]
pub enum Msg {
    Request { client: ClientId, req: u64, op: Op },
    Prepare { view: u64, op_number: u64, client: ClientId, req: u64, op: Op, commit: u64 },
    PrepareOk { view: u64, op_number: u64, replica: usize },
    Commit { view: u64, commit: u64 },
    StartViewChange { view: u64, replica: usize },
    DoViewChange { view: u64, log: Vec<LogEntry>, commit: u64, normal_view: u64, replica: usize },
    StartView { view: u64, log: Vec<LogEntry>, commit: u64 },
    GetState { view: u64, after: u64, replica: usize },
    NewState { view: u64, suffix: Vec<LogEntry>, commit: u64 },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Normal,
    ViewChange,
}

/// One replica: a `StateMachine` wrapped in the VSR protocol.
pub struct Replica<V: Vfs> {
    pub idx: usize,
    n: usize,
    view: u64,
    normal_view: u64,
    status: Status,
    log: Vec<LogEntry>,
    commit: u64,
    sm: StateMachine<V>,
    client_table: HashMap<ClientId, (u64, OpResult)>,
    prepare_ok: HashMap<u64, HashSet<usize>>,
    svc_votes: HashMap<u64, HashSet<usize>>,
    dvc: HashMap<u64, Vec<Msg>>,
    ticks_idle: u64,
    pub crashed: bool,
}

/// Outgoing effects of handling one message/tick.
#[derive(Default)]
pub struct Out {
    pub msgs: Vec<(usize, Msg)>,
    pub replies: Vec<(ClientId, u64, OpResult)>,
}

impl<V: Vfs> Replica<V> {
    pub fn new(idx: usize, n: usize, sm: StateMachine<V>) -> Self {
        Replica {
            idx,
            n,
            view: 0,
            normal_view: 0,
            status: Status::Normal,
            log: Vec::new(),
            commit: 0,
            sm,
            client_table: HashMap::new(),
            prepare_ok: HashMap::new(),
            svc_votes: HashMap::new(),
            dvc: HashMap::new(),
            ticks_idle: 0,
            crashed: false,
        }
    }

    fn quorum(&self) -> usize {
        self.n / 2 + 1
    }
    fn primary_of(&self, view: u64) -> usize {
        (view as usize) % self.n
    }
    fn is_primary(&self) -> bool {
        self.primary_of(self.view) == self.idx
    }
    pub fn op_number(&self) -> u64 {
        self.log.len() as u64
    }
    pub fn committed(&self) -> u64 {
        self.commit
    }
    pub fn digest(&self) -> u32 {
        self.sm.digest()
    }

    /// Apply newly-committed entries (commit+1 ..= target) in order, exactly
    /// once, updating the client table so a post-failover primary can dedupe.
    fn apply_through(&mut self, target: u64, out: &mut Out) {
        let target = target.min(self.op_number());
        while self.commit < target {
            let e = self.log[self.commit as usize].clone();
            let result = self.sm.apply(e.op_number, e.op.clone());
            self.client_table.insert(e.client, (e.req, result.clone()));
            if self.is_primary() {
                out.replies.push((e.client, e.req, result));
            }
            self.commit += 1;
        }
    }

    fn broadcast(&self, m: Msg, out: &mut Out) {
        for r in 0..self.n {
            if r != self.idx {
                out.msgs.push((r, m.clone()));
            }
        }
    }

    pub fn handle(&mut self, from: usize, msg: Msg) -> Out {
        let mut out = Out::default();
        if self.crashed {
            return out;
        }
        match msg {
            Msg::Request { client, req, op } => self.on_request(client, req, op, &mut out),
            Msg::Prepare { view, op_number, client, req, op, commit } => {
                self.on_prepare(view, op_number, client, req, op, commit, &mut out)
            }
            Msg::PrepareOk { view, op_number, replica } => {
                self.on_prepare_ok(view, op_number, replica, &mut out)
            }
            Msg::Commit { view, commit } => self.on_commit_msg(view, commit, &mut out),
            Msg::StartViewChange { view, replica } => {
                self.on_svc(view, replica, &mut out)
            }
            Msg::DoViewChange { view, .. } => {
                self.dvc.entry(view).or_default().push(msg.clone());
                self.maybe_finish_view_change(view, &mut out);
            }
            Msg::StartView { view, log, commit } => {
                self.on_start_view(view, log, commit, &mut out)
            }
            Msg::GetState { view, after, replica } => {
                if view == self.view && self.op_number() > after {
                    let suffix = self.log[after as usize..].to_vec();
                    out.msgs.push((
                        replica,
                        Msg::NewState { view: self.view, suffix, commit: self.commit },
                    ));
                }
            }
            Msg::NewState { view, suffix, commit } => {
                if view == self.view {
                    for e in suffix {
                        if e.op_number == self.op_number() + 1 {
                            self.log.push(e);
                        }
                    }
                    self.apply_through(commit, &mut out);
                    if !self.is_primary() {
                        out.msgs.push((
                            self.primary_of(self.view),
                            Msg::PrepareOk {
                                view: self.view,
                                op_number: self.op_number(),
                                replica: self.idx,
                            },
                        ));
                    }
                }
            }
        }
        let _ = from;
        out
    }

    fn on_request(&mut self, client: ClientId, req: u64, op: Op, out: &mut Out) {
        if !self.is_primary() || self.status != Status::Normal {
            return; // client will retry / rotate to the real primary
        }
        if let Some((last, res)) = self.client_table.get(&client) {
            if req <= *last {
                out.replies.push((client, *last, res.clone()));
                return;
            }
        }
        let op_number = self.op_number() + 1;
        self.log.push(LogEntry { op_number, client, req, op: op.clone() });
        self.prepare_ok.entry(op_number).or_default().insert(self.idx);
        let m = Msg::Prepare {
            view: self.view,
            op_number,
            client,
            req,
            op,
            commit: self.commit,
        };
        self.broadcast(m, out);
    }

    #[allow(clippy::too_many_arguments)]
    fn on_prepare(
        &mut self,
        view: u64,
        op_number: u64,
        client: ClientId,
        req: u64,
        op: Op,
        commit: u64,
        out: &mut Out,
    ) {
        if view < self.view {
            return;
        }
        if view > self.view {
            // Behind: adopt the view, recover the log via state transfer.
            self.view = view;
            self.status = Status::Normal;
            self.normal_view = view;
        }
        self.ticks_idle = 0;
        if op_number == self.op_number() + 1 {
            self.log.push(LogEntry { op_number, client, req, op });
            out.msgs.push((
                self.primary_of(self.view),
                Msg::PrepareOk { view: self.view, op_number, replica: self.idx },
            ));
        } else if op_number <= self.op_number() {
            out.msgs.push((
                self.primary_of(self.view),
                Msg::PrepareOk { view: self.view, op_number, replica: self.idx },
            ));
        } else {
            out.msgs.push((
                self.primary_of(self.view),
                Msg::GetState { view: self.view, after: self.op_number(), replica: self.idx },
            ));
        }
        self.apply_through(commit, out);
    }

    fn on_prepare_ok(&mut self, view: u64, op_number: u64, replica: usize, out: &mut Out) {
        if view != self.view || !self.is_primary() || self.status != Status::Normal {
            return;
        }
        let acks = {
            let s = self.prepare_ok.entry(op_number).or_default();
            s.insert(replica);
            s.insert(self.idx);
            s.len()
        };
        if acks >= self.quorum() && op_number > self.commit {
            // Commit contiguously up to the highest quorum-acked op.
            let mut target = self.commit;
            while target < self.op_number() {
                let next = target + 1;
                let ok = self
                    .prepare_ok
                    .get(&next)
                    .map(|s| s.len() >= self.quorum())
                    .unwrap_or(false);
                if ok {
                    target = next;
                } else {
                    break;
                }
            }
            self.apply_through(target, out);
            self.broadcast(Msg::Commit { view: self.view, commit: self.commit }, out);
        }
    }

    fn on_commit_msg(&mut self, view: u64, commit: u64, out: &mut Out) {
        if view < self.view {
            return;
        }
        if view > self.view {
            self.view = view;
            self.status = Status::Normal;
            self.normal_view = view;
        }
        self.ticks_idle = 0;
        if commit > self.op_number() {
            out.msgs.push((
                self.primary_of(self.view),
                Msg::GetState { view: self.view, after: self.op_number(), replica: self.idx },
            ));
        }
        self.apply_through(commit, out);
    }

    // ---- view change ----

    fn start_view_change(&mut self, out: &mut Out) {
        self.view += 1;
        self.status = Status::ViewChange;
        self.svc_votes.entry(self.view).or_default().insert(self.idx);
        let v = self.view;
        self.broadcast(Msg::StartViewChange { view: v, replica: self.idx }, out);
        self.maybe_finish_svc(v, out);
    }

    fn on_svc(&mut self, view: u64, replica: usize, out: &mut Out) {
        if view < self.view {
            return;
        }
        if view > self.view {
            self.view = view;
            self.status = Status::ViewChange;
        }
        self.svc_votes.entry(view).or_default().insert(replica);
        self.svc_votes.entry(view).or_default().insert(self.idx);
        self.maybe_finish_svc(view, out);
    }

    fn maybe_finish_svc(&mut self, view: u64, out: &mut Out) {
        let votes = self.svc_votes.get(&view).map(|s| s.len()).unwrap_or(0);
        if self.status == Status::ViewChange
            && view == self.view
            && votes >= self.quorum()
        {
            let dest = self.primary_of(view);
            let dvc = Msg::DoViewChange {
                view,
                log: self.log.clone(),
                commit: self.commit,
                normal_view: self.normal_view,
                replica: self.idx,
            };
            if dest == self.idx {
                self.dvc.entry(view).or_default().push(dvc);
                self.maybe_finish_view_change(view, out);
            } else {
                out.msgs.push((dest, dvc));
            }
        }
    }

    fn maybe_finish_view_change(&mut self, view: u64, out: &mut Out) {
        if self.primary_of(view) != self.idx || view < self.view {
            return;
        }
        let msgs = match self.dvc.get(&view) {
            Some(v) if v.len() >= self.quorum() => v.clone(),
            _ => return,
        };
        // Pick the most up-to-date log: max (normal_view, op_number).
        let mut best_log: Vec<LogEntry> = Vec::new();
        let mut best_key = (0u64, 0u64);
        let mut max_commit = 0u64;
        for m in &msgs {
            if let Msg::DoViewChange { log, commit, normal_view, .. } = m {
                let key = (*normal_view, log.len() as u64);
                if key > best_key {
                    best_key = key;
                    best_log = log.clone();
                }
                max_commit = max_commit.max(*commit);
            }
        }
        self.view = view;
        self.normal_view = view;
        self.status = Status::Normal;
        self.log = best_log;
        self.prepare_ok.clear();
        self.broadcast(
            Msg::StartView { view, log: self.log.clone(), commit: max_commit },
            out,
        );
        self.apply_through(max_commit, out);
        self.ticks_idle = 0;
    }

    fn on_start_view(&mut self, view: u64, log: Vec<LogEntry>, commit: u64, out: &mut Out) {
        if view < self.view {
            return;
        }
        self.view = view;
        self.normal_view = view;
        self.status = Status::Normal;
        self.log = log;
        self.ticks_idle = 0;
        self.apply_through(commit, out);
        if !self.is_primary() {
            out.msgs.push((
                self.primary_of(view),
                Msg::PrepareOk {
                    view,
                    op_number: self.op_number(),
                    replica: self.idx,
                },
            ));
        }
    }

    /// One logical time tick: primary heartbeats; backups detect a dead
    /// primary and trigger a view change.
    pub fn tick(&mut self) -> Out {
        let mut out = Out::default();
        if self.crashed {
            return out;
        }
        if self.is_primary() && self.status == Status::Normal {
            self.broadcast(Msg::Commit { view: self.view, commit: self.commit }, &mut out);
            // Retransmit uncommitted prepares (drop recovery).
            for i in self.commit..self.op_number() {
                let e = self.log[i as usize].clone();
                self.broadcast(
                    Msg::Prepare {
                        view: self.view,
                        op_number: e.op_number,
                        client: e.client,
                        req: e.req,
                        op: e.op,
                        commit: self.commit,
                    },
                    &mut out,
                );
            }
        } else if self.status == Status::Normal {
            self.ticks_idle += 1;
            if self.ticks_idle >= PRIMARY_TIMEOUT_TICKS {
                self.ticks_idle = 0;
                self.start_view_change(&mut out);
            }
        }
        out
    }
}

/// Deterministic in-process replicated cluster driver with seeded fault
/// injection. Public so benchmarks and tests can both drive a real cluster.
pub mod sim {
    use super::*;
    use kessel_io::MemVfs;
    use kessel_proto::Rng;
    use std::collections::VecDeque;

    pub struct Cluster {
        rs: Vec<Replica<MemVfs>>,
        inbox: Vec<VecDeque<(usize, Msg)>>,
        replies: HashMap<(ClientId, u64), OpResult>,
        rng: Rng,
        drop_pct: u64,
    }

    impl Cluster {
        pub fn new(n: usize, seed: u64, drop_pct: u64) -> Self {
            let rs = (0..n)
                .map(|i| Replica::new(i, n, StateMachine::open(MemVfs::new()).unwrap()))
                .collect();
            Cluster {
                rs,
                inbox: (0..n).map(|_| VecDeque::new()).collect(),
                replies: HashMap::new(),
                rng: Rng::new(seed),
                drop_pct,
            }
        }

        fn route(&mut self, out: Out) {
            for (to, m) in out.msgs {
                if self.drop_pct > 0 && self.rng.below(100) < self.drop_pct {
                    continue; // simulated message loss (recovered by retransmit)
                }
                self.inbox[to].push_back((usize::MAX, m));
            }
            for (c, r, res) in out.replies {
                self.replies.entry((c, r)).or_insert(res);
            }
        }

        fn deliver_to(&mut self, target: usize, m: Msg) {
            self.inbox[target].push_back((usize::MAX, m));
        }

        /// Run until every (client,req) in `reqs` has a reply, or `max` steps.
        pub fn run(&mut self, reqs: &[(ClientId, u64, Op)], max: usize) -> usize {
            let n = self.rs.len();
            for step in 0..max {
                // clients (re)send to a rotating target until acked
                for (c, r, op) in reqs {
                    if !self.replies.contains_key(&(*c, *r)) {
                        let t = ((*c as usize) + step / 3) % n;
                        self.deliver_to(t, Msg::Request { client: *c, req: *r, op: op.clone() });
                    }
                }
                for i in 0..n {
                    while let Some((from, m)) = self.inbox[i].pop_front() {
                        let out = self.rs[i].handle(from, m);
                        self.route(out);
                    }
                }
                for i in 0..n {
                    let out = self.rs[i].tick();
                    self.route(out);
                }
                if reqs.iter().all(|(c, r, _)| self.replies.contains_key(&(*c, *r))) {
                    return step;
                }
            }
            usize::MAX
        }

        pub fn live_digests(&self) -> Vec<u32> {
            self.rs.iter().filter(|r| !r.crashed).map(|r| r.digest()).collect()
        }

        pub fn replica_count(&self) -> usize {
            self.rs.len()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::Cluster;
        use kessel_catalog::{encode_type_def, Field, FieldKind};
        use kessel_io::MemVfs;
        use kessel_proto::{ObjectId, Op};
        use kessel_sm::{encode_overflow_record, StateMachine};

    fn def() -> Op {
        Op::CreateType { def: encode_type_def("t", &[]) }
    }

    #[test]
    fn no_fault_replication_converges_and_is_linearizable() {
        let mut c = Cluster::new(3, 1, 0);
        // single client, sequential ops -> commit order == submission order
        let mut reqs = vec![(7u128, 1u64, def())];
        for i in 0..120u64 {
            reqs.push((
                7,
                i + 2,
                Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: vec![i as u8] },
            ));
        }
        assert_ne!(c.run(&reqs, 5000), usize::MAX, "must finish");

        // reference: apply the same committed order to a fresh SM
        let mut oracle = StateMachine::open(MemVfs::new()).unwrap();
        let mut on = 1u64;
        for (_, _, op) in &reqs {
            oracle.apply(on, op.clone());
            on += 1;
        }
        let d = c.live_digests();
        assert!(d.iter().all(|x| *x == d[0]), "replicas diverged: {d:?}");
        assert_eq!(d[0], oracle.digest(), "cluster != reference model");
    }

    #[test]
    fn deterministic_same_seed_same_state() {
        let build = || {
            let mut c = Cluster::new(3, 42, 0);
            let reqs = vec![
                (1u128, 1u64, def()),
                (1, 2, Op::Create { type_id: 1, id: ObjectId::from_u128(9), record: vec![1] }),
                (2, 1, Op::Create { type_id: 1, id: ObjectId::from_u128(8), record: vec![2] }),
            ];
            c.run(&reqs, 3000);
            c.live_digests()
        };
        assert_eq!(build(), build());
    }

    #[test]
    fn primary_crash_triggers_view_change_and_progress() {
        let mut c = Cluster::new(3, 5, 0);
        let warmup = vec![
            (1u128, 1u64, def()),
            (1, 2, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: vec![10] }),
        ];
        assert_ne!(c.run(&warmup, 2000), usize::MAX);
        let v_before = c.rs[0].view;

        c.rs[0].crashed = true; // kill the primary (replica 0, view 0)

        let mut more = Vec::new();
        for i in 0..40u64 {
            more.push((
                2u128,
                i + 1,
                Op::Create { type_id: 1, id: ObjectId::from_u128(100 + i as u128), record: vec![i as u8] },
            ));
        }
        assert_ne!(c.run(&more, 8000), usize::MAX, "must make progress after failover");

        assert!(c.rs[1].view > v_before, "a view change must have occurred");
        let d = c.live_digests(); // replicas 1 and 2
        assert_eq!(d.len(), 2);
        assert_eq!(d[0], d[1], "surviving replicas must converge after failover");
    }

    #[test]
    fn overflow_replicates_and_converges() {
        // Variable-length blobs ride inside Create records; the deterministic
        // op-derived handle must make every replica's overflow keyspace
        // identical (digest includes it).
        let mut c = Cluster::new(3, 11, 0);
        let def = Op::CreateType {
            def: encode_type_def(
                "doc",
                &[Field { field_id: 0, name: "body".into(), kind: FieldKind::OverflowRef, nullable: false }],
            ),
        };
        let fixed = vec![0u8; 32]; // OverflowRef-only type: record_size = 32
        let mut reqs = vec![(1u128, 1u64, def)];
        for i in 0..40u64 {
            let rec = encode_overflow_record(&fixed, &[(0, vec![i as u8; 500 + i as usize])]);
            reqs.push((
                1,
                i + 2,
                Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec },
            ));
        }
        assert_ne!(c.run(&reqs, 8000), usize::MAX, "overflow ops must commit");
        let d = c.live_digests();
        assert!(d.iter().all(|x| *x == d[0]), "overflow diverged: {d:?}");
    }

    #[test]
    fn converges_under_message_loss() {
        let mut c = Cluster::new(3, 9, 25); // drop 25% of messages
        let mut reqs = vec![(3u128, 1u64, def())];
        for i in 0..60u64 {
            reqs.push((
                3,
                i + 2,
                Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: vec![i as u8] },
            ));
        }
        assert_ne!(c.run(&reqs, 20000), usize::MAX, "must converge despite loss");
        let d = c.live_digests();
        assert!(d.iter().all(|x| *x == d[0]), "diverged under loss: {d:?}");
    }
    } // mod tests
} // pub mod sim
