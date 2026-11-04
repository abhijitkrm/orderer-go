# REPLICATION — replicated ingress log and failover (v0.2, implemented in orderer-rust)

**Status: implemented in `orderer-rust` only; not yet part of a tagged spec
and not yet ported.** §1-§12 are the design as proposed. The decisions in §11
are now resolved, and **§13 records what was built, the wire formats, what
was measured, and what is not covered.** Where §13 and an earlier section
differ, §13 is what the code does.

Extends `PIPELINE.md` and `JOURNAL.md`. Matching semantics stay matcher's
and are not restated.

---

## 1. Goal and non-goals

**Goal.** Survive the loss of a minority of nodes with no acknowledged
command lost, no command applied twice, and no divergence between nodes.
Failover completes without operator action.

**Why this fits.** The pipeline is already a deterministic state machine:
the same command sequence yields byte-identical events and snapshots in all
five ports. Replication therefore reduces to agreeing on **one totally
ordered command log**. Every node applies the same log and must reach the
same state.

**Non-goals (v1).**
- Dynamic membership. The node set is static configuration.
- Byzantine faults. Nodes crash, restart, lose messages and lose the tail
  of a file. They do not lie.
- Cross-datacenter tuning, leader leases for reads, and client sessions
  (step 2: wire protocol).
- More than one replicated group per pipeline (see §11, D1).

## 2. Model

```
producers ─▶ LEADER sequencer ─▶ replicated ingress log ─▶ commit
                                    │  append (term, iseq, cmd)
                         FOLLOWERS ◀┘  persist, then ack
commit ─▶ router ─▶ partitions ─▶ engines ─▶ egress     (on every node)
```

- A cluster has `N = 2f + 1` nodes (`N = 3` typical, tolerates `f = 1`).
  `N = 1` is valid and is the degenerate case: it behaves exactly like
  today's pipeline.
- Exactly one node is **leader** in a given **term**. Only the leader
  accepts producers and assigns `iseq`.
- The **ingress log** is the replicated object: a sequence of control and
  command entries in ingress order. This is the existing ingress order of
  `PIPELINE.md` §2, made durable and shared.
- Each node runs the unchanged pipeline *after* the log: router, partition
  engines, egress. Partition command journals stop being the source of
  truth in replicated mode (§6).

## 3. Log entries and terms

An entry is `(term, index, payload)`.

- `index` is the log position, 1-based and dense. For command entries
  `index` equals `iseq`. Control operations (`PIPELINE.md` §6) are entries
  too, so with controls present `index ≥ iseq`. **[DECIDE D5]** whether
  controls get their own `iseq`-free index or the log uses `index` only.
- `term` (u32) is the election epoch in which the leader created the entry.
- **Log matching.** If two logs hold an entry with the same `(index, term)`,
  the two logs are identical up to and including that index.
- **Binary record.** The 48-byte command record of `JOURNAL.md` §2.2
  version 2 has 4 reserved bytes at offset 44. Version 3 stores `term` there
  and extends the CRC-32C to bytes 0–39 plus 44–47. Control entries use
  `cmd = 0x10..0x1F` and the same record size.

## 4. Commit and apply

- An entry is **committed** once it is persisted (fsync completed) on a
  majority of nodes **in the leader's current term or earlier, with the
  Raft safety rule**: a leader commits entries from earlier terms only
  indirectly, by committing an entry of its own term after them.
- `commit_index` is monotone on every node and never moves backward.
- **Apply is gated by commit, not by local write.** This replaces the gate
  in `PIPELINE.md` §4 (`journaled_iseq` gates the engine) when `N > 1`:
  the engine applies entry `i` only when `commit_index ≥ i`. With `N = 1`,
  commit coincides with the local write, so §4 holds unchanged.
- Consequence: **no state is ever rolled back.** A node only applies
  committed entries, and committed entries are never lost, so there is no
  undo path to build or test. Uncommitted log tails may be truncated and
  rewritten; applied state may not.
- **Ack contract.** `durable_iseq` of `PIPELINE.md` §5 is redefined for
  `N > 1` as the highest `iseq` with `commit_index ≥ index`. An ack is
  therefore released only after a **quorum** has the command durable.
- **Cost.** Latency to ack is one network round trip plus a remote fsync,
  added to today's. This is inherent; the design batches (§9) to keep
  throughput.

## 5. Elections and fencing

- Raft-style: randomized election timeouts, `RequestVote` with the
  up-to-date check `(last_term, last_index)` lexicographic, one vote per
  term, persisted before replying.
- `term` and `voted_for` are persisted in a small metadata file with the
  same CRC and torn-write discipline as journals.
- **Fencing.** Every message carries the sender's term. A node rejects any
  message from a lower term. A deposed leader that learns of a higher term
  stops publishing immediately: later `publish` fails with a *not-leader*
  error carrying a leader hint. Nothing in flight is acknowledged after
  that, because acks require commit.
- A new leader appends a no-op entry in its term before accepting
  producers, which both commits earlier entries and establishes its
  authority.
- **Pre-vote** is required, so a partitioned node cannot inflate terms and
  depose a healthy leader on rejoin. **[DECIDE D4]**: whether pre-vote is
  v1 or v1.1.

## 6. Recovery, snapshots and catch-up

- On restart a node reads its metadata and ingress log, truncating any torn
  tail (`JOURNAL.md` §5.2), and rejoins as follower. It does not apply
  anything until it learns `commit_index`.
- **Checkpoints** (`JOURNAL.md` §6) keep their meaning: a durable snapshot
  at cut `N` allows deleting earlier segments. In replicated mode a
  checkpoint may only be taken at a cut ≤ `commit_index`.
- **Catch-up.** A follower whose needed prefix was already deleted receives
  the leader's latest snapshot, then the log suffix. Snapshot transfer is
  chunked, CRC-checked, and restartable.
- **State divergence detection.** At every checkpoint each node computes a
  SHA-256 over the canonical snapshot bytes (`matcher` `JOURNAL.md` §4,
  already byte-identical across ports) and sends `(cut, digest)` to the
  leader. A mismatch is a bug, not a recoverable fault. The mismatching node
  halts (fail-stop), the cluster keeps running if a quorum still agrees, and
  the event is surfaced loudly.

## 7. Egress and consumers

- Every node derives events deterministically. Only the **leader's** egress
  is live toward external consumers; followers run egress in
  discard-or-local mode.
- After a failover the new leader re-emits from its own applied state.
  Consumers de-duplicate on `(symbol, seq)`, which is dense per book and
  already part of the event contract (`PIPELINE.md` §7). This gives
  effectively-once delivery with no extra protocol.

## 8. Failure semantics (what a client can rely on)

| Situation | Guarantee |
|---|---|
| Leader crashes after the client got an ack | Command is committed on a quorum; the new leader applies it. Never lost. |
| Leader crashes before ack | Command may or may not be committed. The client does not know. Resolving this needs client-ID dedupe (step 2); the log carries what that needs. |
| Minority partition | Minority side cannot commit and refuses publishes; majority side continues. |
| Leader loses quorum | It stops acking and steps down after a timeout. |
| Follower loses disk tail | It truncates, catches up from leader. Cannot affect committed state. |
| Two nodes disagree on state | Impossible if deterministic; detected by §6 digests; offender halts. |

## 9. Performance expectations (to be measured, not assumed)

- A 48-byte entry at 30M entries/s is ~1.4 GB/s per follower link. A single
  replicated log therefore **bounds aggregate throughput by network and
  follower fsync bandwidth**, not by the cores. This is the design's main
  trade-off (D1).
- Mitigation: batch entries per message (target ≥ 4 KiB), pipeline
  `AppendEntries` without waiting for each ack, group-commit fsync on
  followers, and compress nothing (cost exceeds benefit at this size).
- Replication throughput targets must be measured on Linux with NVMe and a
  real NIC. The M1 numbers in `docs/RESULTS.md` are not valid for this
  (F_FULLFSYNC ~12 ms).

## 10. Verification plan (this is where the work is)

Writing consensus is the risky part, so correctness evidence comes first.

1. **Pure protocol core.** The protocol is a function: `(state, input) →
   (state', effects)`. Inputs: message received, timer fired, disk write
   completed, client proposal. Effects: send message, start disk write,
   set timer, apply entries. It performs no I/O and reads no clock.
2. **Deterministic simulation.** A single-threaded harness drives N cores
   with a seeded PRNG controlling: message drop / duplicate / reorder /
   delay, partitions, node crash and restart, torn and lost disk tails,
   slow or failing fsync, clock skew. It checks, after every step:
   - election safety: at most one leader per term;
   - log matching and leader completeness;
   - **committed prefix identical** on all nodes, never shrinking;
   - no entry applied twice, none applied before commit;
   - every acked command is in every later leader's log;
   - applied state digests equal at equal `commit_index`.
   Any failure prints the seed and replays exactly.
3. **Cross-port conformance (implemented: `vectors/replication/`).** The seeded input traces and expected effect
   traces become vectors in `vectors/replication/`. Every port's protocol
   core must produce byte-identical effects for the same input trace, the
   same technique already used for matching. This is how five ports stay
   consistent without five independent consensus bugs.
4. **Real-process failover test.** Extend `scripts/crash.sh`: kill -9 the
   leader under load, repeatedly, and check no acknowledged command is lost
   and all survivors' snapshots are identical.

## 11. Open decisions

- **D1 one log vs one log per partition.** *Recommendation: one log.* It
  preserves the total ingress order that barriers and snapshot cuts rely on
  (`PIPELINE.md` §6), and keeps recovery simple. Cost: replication bandwidth
  caps aggregate throughput (§9). Per-partition groups scale better but lose
  global cuts; revisit only if measurements show the cap matters.
- **D2 apply after commit** (recommended, §4) vs leader applies after local
  write and followers after commit. The latter lowers leader latency but
  needs a rollback path on leader change; rejected for v1.
- **D3 build vs reuse.** Implement the protocol core ourselves (needed for
  five ports and byte-level conformance vectors) vs wrap an existing
  library per language. *Recommendation: build, with the §10 tests as the
  justification for trusting it.*
- **D4 pre-vote in v1** (recommended) or later.
- **D5 control entries and `iseq`.** Separate index space vs shared (§3).
- **D6 static membership** for v1 (recommended).

## 12. Phasing

| Phase | Deliverable |
|---|---|
| R-A | This spec accepted; message wire format and vector format fixed |
| R-B | `orderer-rust`: pure protocol core + deterministic simulator, all §10.2 invariants green over a large seed sweep |
| R-C | Integrate: ingress log, commit-gated apply, `N = 1` regression-identical to today |
| R-D | TCP transport, snapshot transfer, real 3-node run |
| R-E | Failover under load (kill -9), digest checks, BENCH numbers on Linux |
| R-F | Ports: C++, Java, Go, TS against `vectors/replication/` |


---

## 13. As built (orderer-rust)

### 13.1 Decisions resolved

| | Decision |
|---|---|
| D1 | **One log per group**, and **several independent groups** for scale-out (§13.5). A group is a Raft group owning a set of symbols. Within a group the total ingress order holds; between groups there is none. |
| D2 | Apply after commit, as proposed. No state is ever rolled back. |
| D3 | Built, not wrapped. `orderer-consensus` is the protocol as a pure state machine; `orderer-cluster` is the runtime. |
| D4 | Pre-vote is in v1. |
| D5 | Controls are not yet log entries. A replicated cluster does not offer cluster-wide barrier/snapshot/shutdown controls; snapshots are per replica and driven by log index (§13.3). |
| D6 | Static membership. |

### 13.2 Protocol details beyond §1-§12

* **Acknowledgement is at commit, not apply.** A proposal is acknowledged
  once its entry is durable on a quorum. The state machine applies on its own
  thread (the *applier*); the consensus thread never calls it. A slow state
  machine therefore cannot starve ticks and heartbeats (a regression test
  makes one apply outlast the election timeout; it fails on the earlier
  inline-apply design). Producers are pushed back with `Busy` once the
  applier is `max_apply_backlog` entries behind.
* **Outcomes are explicit.** `Ok(index)` = committed. `NotLeader(hint)` and
  `Busy` = nothing was appended, retry. `Timeout` / `Unknown` = the entry may
  still commit; the client must not retry blindly (this is what the idempotent
  client IDs of the gateway step are for). `Superseded` = the index was
  committed with another leader's entry: lost.
* **Replication is pipelined.** The leader advances a follower's `next_index`
  optimistically after sending, so a burst of proposals is not resent; a lost
  message surfaces as a failed reply carrying a hint.
* **Divergence check.** At every `digest_every` log indexes each replica's
  applier computes a SHA-256 of its state (`StateMachine::digest`) and sends
  it to the others. A replica whose digest differs from a strict majority's
  halts itself (fail-stop). If no digest has a majority it halts too.
* **Log compaction and catch-up.** The applier snapshots at an applied index,
  stores the snapshot durably, and only then does the log drop the prefix it
  covers. A follower whose needed prefix is gone is sent the snapshot over the
  transport (chunked, length- and CRC-checked) and installs it
  (`Node::install_snapshot`).

### 13.3 On-disk and wire formats

All integers little-endian. CRC is CRC-32C (`JOURNAL.md` §2.2).

* **WAL** (`raft.wal`): 8-byte magic `ORDWAL\0\1`, then frames
  `len u32 | crc u32 | body`. One frame is one atomic consensus write: `term
  u32 | voted_for u8 (255 = none) | compact flag u8 [index u64, term u32] |
  from u64 | n u32 | n × (term u32, len u32, data)`. Replaying frames with the
  protocol core's own `Disk::apply` reproduces the state it handed to the
  disk. Recovery cuts a **torn tail** (incomplete or failing frame with
  nothing valid after it, including a **zero-filled tail**, which a SIGKILL
  during an append can leave) and **refuses to start** on damage followed by
  valid frames. A frame body shorter than 18 bytes is never valid, which is
  what makes zero fill detectable.
* **Snapshot** (`snap-<index>.snap`): `ORDS | index u64 | term u32 | len u64 |
  crc u32 | data`, written to a temp name, fsynced, renamed, directory fsynced.
* **Log entry payload**: empty (a leader's no-op) or `0x01 | n u32 | n × 32`
  bytes, each command being a journal record without `iseq`, checksum or
  padding (`symbol, cmd, side, otype, tif, order_id, price, qty`). One entry
  carries a whole batch, so consensus cost is per batch, not per command.
* **Transport frame**: `len u32 | crc u32 | kind u8 | body`. Kinds: message,
  snapshot begin / chunk / end, digest, hello. A frame over 64 MiB, with a bad
  checksum, a bad body, or from a node outside the cluster id closes the
  connection.
* **Client protocol**: the same envelope; requests `PROPOSE`, `STATUS`,
  `HASH`.

### 13.4 Verification that exists

* **Protocol core, simulated** (`orderer-consensus`): 3, 5 and 7 nodes, with
  and without pre-vote, message loss / duplication / reordering / delay,
  partitions, crashes with torn disk tails, slow fsync, compaction and
  snapshot catch-up. Checks election safety, log matching, leader
  completeness, one agreed committed sequence, no entry committed twice or
  never proposed, state-machine equality after healing. Tens of thousands of
  seeds clean (1,000 seeds: about 22,000 compactions and 3,000 snapshot
  installs). Five deliberate protocol bugs (commit on one ack, skip the vote
  up-to-date check, double vote, commit an old-term entry by counting) are
  caught, the last by a directed figure-8 test.
* **Real replicas over TCP and real files** (`orderer-cluster/tests`): 16
  cluster tests (election, ordering, redirect, leader crash with no
  acknowledged loss, repeated leader kills under concurrent load, isolated
  leader cannot commit, snapshot catch-up, whole-cluster restart, torn and
  corrupt WAL, digest divergence halts the odd node out, hostile connections,
  five nodes with two failures, no quorum no ack, slow state machine does not
  cost leadership), 6 client-protocol tests (connection reuse, frames split
  across the read timeout, malformed frames, leader following), 2 tests that
  run the real matching pipeline as the state machine and require its events
  and state to equal an unreplicated pipeline's through follower snapshot
  restore and leader failover, 2 sharded-group tests, and unit tests for the
  WAL (every possible torn prefix, zero tails, mid-file damage, rewrite), codec
  (truncations and random garbage never panic), snapshot store and SHA-256.
* **Real processes** (`orderer-rust/scripts/failover.sh`): three `orderer-node`
  processes with real fsync, a load generator, and `kill -9` of the current
  leader five or six times under load. Passes only if every acknowledged batch
  is present on every replica, in the same order, exactly once, and all
  replicas end at the same state digest. On the final code, six consecutive
  runs passed: 36 leader kills and 15,920 acknowledged batches (796,000
  commands). Early runs of
  this script found a real recovery bug (a zero-filled WAL tail misread as
  mid-file corruption; two of the first four runs failed on it).

### 13.5 Scale-out and what a single group can carry

One group is bounded by its leader: it writes every entry to its own WAL and
sends it to every follower. With 32-byte commands that is roughly `rate × 32 B`
of WAL and `rate × 32 B × (N-1)` of network egress at the leader. On a 10 GbE
NIC and N = 3 the leader's network alone caps one group near 19 M
commands/s (analytic; not measured, this machine has no NIC to saturate).
Independent groups (`orderer_cluster::shard`) multiply that: symbols are
assigned to groups by a hash chosen not to correlate with the pipeline's own
symbol-to-partition hash, `ShardedClient` splits a batch per group, and the
groups share nothing, so an outage in one does not stop another (tested).
The price is the global order: there is none between groups, only between the
commands of one symbol.

### 13.6 Measured (Apple M1, 8 cores, everything on one machine)

All replicas, producers and the OS share one 8-core laptop, one SSD and one
loopback interface, and a web browser was running. These are floors for what
dedicated hardware per replica would do, not ceilings, and the cross-group
scaling below is **not** demonstrated by them: the machine is the limit.

| Configuration | Result |
|---|---|
| 1 node, no-op state machine, no fsync | 46.9 M commands/s (1.5 GB/s of log) |
| 3 nodes, no-op, no fsync, 1000-command batches | 16.2 M commands/s (519 MB/s) |
| 3 nodes, no-op, **fsync on a quorum**, batch 100 / 1000 / 8000 | 1.3 / 8.3 / 12.3 M commands/s |
| 1 node, real matching pipeline (P = 2) | 31.6 M acked, 25.6 M applied |
| 3 nodes, real matching pipeline (P = 2) | 7.8 M acked, 6.6 M applied on every node |
| 1 / 2 / 3 groups, 3 nodes each, no-op | 16.2 / 16.5 / 18.3 M aggregate (box-limited) |
| ack latency, batch of 1, fsync on | about 10 ms (macOS full fsync) |

Two changes made most of the difference. CRC-32C was a byte-at-a-time loop at
388 MB/s and every byte is checksummed about seven times on its way through
WAL, sender, reader and follower WAL; a portable slicing-by-8 version reaches
1.7 GB/s and the optional `hwcrc` feature (runtime-detected CPU instruction)
5.3 GB/s. And applying inline starved heartbeats (§13.2).

**Against matcher.** matcher-rust applies about 100 M operations/s on one
core. The replicated path today carries 8-16 M commands/s per group (durable
and not) and the applier, a single thread decoding and publishing into the
pipeline, about 25 M. So one group is roughly a quarter to a sixth of one
matcher core, not equal to it. Reaching matcher's rate takes about five to
eight groups on dedicated hardware (or fewer with a wider applier and
smaller commands); the aggregate has been measured only on one machine, where
it does not scale.

### 13.7 Client safety, reads and transport authentication

* **Exactly-once submission (sessions).** A client with a session id sends
  each batch tagged `(client, seq)` (payload tag 2). Every replica wraps its
  state machine in `DedupSm`, which applies a batch only if `seq` is newer
  than the last applied for that client; the table is part of every snapshot
  and state digest, so replicas skip the same entries. `Client::propose_session`
  retries the *same* `(client, seq)` through lost connections and leader
  changes until it is definitely committed, so `Unknown` no longer reaches the
  caller. A session is one sequential stream: never send `seq + 1` before
  `seq` has an answer. In the process-level failover test this took unknown
  outcomes from 15 per run to 0, with every batch still applied exactly once.
* **Linearizable reads (log barrier).** `ReplicaHandle::barrier()` commits an
  empty entry and waits for the state machine to apply it. A leader that has
  been deposed cannot commit, so it cannot answer; a read after the barrier
  sees every write acknowledged before the call. It costs one log entry per
  read; a read-index optimisation (a heartbeat round instead of an entry) is
  future work.
* **Authenticated transport (not encrypted).** With a pre-shared cluster key
  (`ReplicaConfig::auth_key`, `ORDERER_KEY` for the binaries) replicas and
  clients run a challenge-response handshake (HMAC-SHA256) and every later
  frame carries a 128-bit MAC over direction, sequence number, kind and body.
  A peer without the key cannot join or be impersonated, and altered,
  replayed, dropped, reordered or reflected frames are rejected. Payloads are
  **not encrypted**; use a tunnel for secrecy. TLS itself would need a
  third-party dependency, which this workspace does not take.

### 13.8 Not covered

* Dynamic membership; confidentiality on the wire; a read-index fast path;
  key rotation (the key is static per process).
* Cluster-wide barrier / snapshot / shutdown controls, and any consistent
  snapshot across groups.
* A parallel applier. The applier is one thread; a snapshot also runs on it,
  so a large book pauses applying (not consensus) while it serialises.
* Throughput on real multi-machine hardware, with a real NIC and NVMe, and
  Linux fsync behaviour. Everything above is one laptop.
* The replication **runtime** (WAL, snapshots, transport, replica, client,
  sessions, authentication) exists only in orderer-rust. C++, Go, Java and
  TypeScript have the **protocol core** only (§13.9); each still needs its own
  storage, network and client layers.
* `unsafe` was added in one place, behind the `hwcrc` feature (runtime CPU
  detection, tested equal to the portable code): the third sanctioned site
  after `affinity` and `prefetch`.

### 13.9 Ports

The protocol core (`Node`: elections, replication, compaction, snapshot
install) is ported to **C++** (`orderer-cpp/include/orderer/consensus.hpp`),
**Go** (`orderer-go/consensus`), **Java** (`orderer-java` `Consensus`) and
**TypeScript** (`orderer-ts/src/consensus.ts`). Each replays all 28 traces in
`vectors/replication/` (28,000 recorded drain points: elections won,
pre-votes, replication, truncation, applies, compactions, snapshot requests,
installs, restores and crash-restart stanzas) and must reproduce every
recorded effect and state line byte for byte; each port's test also fails when
a vector line is altered. The ports have no simulator of their own: the
vectors carry what the Rust simulator exercised, and no more. A behaviour the
simulator never reached is not covered in any port.
