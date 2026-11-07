package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abhijitkrm/orderer-go/consensus"
)

// The replica runtime: one driver goroutine owns the protocol Node and
// executes its effects (WAL writes on an I/O goroutine, network sends, state
// machine applies on an applier goroutine, snapshots), as in orderer-rust
// (replica.rs; spec/REPLICATION.md 13). Acknowledgement is at commit; the
// state machine applies on its own goroutine so a slow one cannot starve
// heartbeats, and producers are pushed back with Busy when it falls behind.

// ReplicaConfig configures one replica.
type ReplicaConfig struct {
	ID        consensus.NodeID
	Members   []Member
	ClusterID uint64
	Dir       string
	Tick      time.Duration // protocol tick; default 10ms
	Consensus consensus.Config
	// CompactEvery snapshots and compacts the log every N applied entries; 0 = never.
	CompactEvery uint64
	// DigestEvery exchanges state digests every N entries to detect divergence; 0 = off.
	DigestEvery uint64
	// NoFsync skips fsync: tests and benchmarks only, it loses durability.
	NoFsync        bool
	ProposeTimeout time.Duration // default 5s
	MaxPending     int           // default 4096
	// MaxApplyBacklog is how far the state machine may lag the log before
	// proposals get Busy; default 8192.
	MaxApplyBacklog uint64
	AuthKey         []byte // pre-shared cluster key; nil = no authentication
}

// DefaultConfig is a ReplicaConfig with the runtime's defaults.
func DefaultConfig(id consensus.NodeID, members []Member, dir string) ReplicaConfig {
	return ReplicaConfig{ID: id, Members: members, ClusterID: 1, Dir: dir, Tick: 10 * time.Millisecond,
		Consensus:      consensus.Config{ElectionTicks: 30, HeartbeatTicks: 3, MaxBatch: 256, PreVote: true},
		ProposeTimeout: 5 * time.Second, MaxPending: 4096, MaxApplyBacklog: 8192}
}

// ProposeKind is the outcome of a proposal.
type ProposeKind int

const (
	// Committed on a quorum.
	ProposeOK ProposeKind = iota
	// NotLeader: try Hint (the leader this node last heard from), if any.
	ProposeNotLeader
	// Busy: back-pressure; retry shortly.
	ProposeBusy
	// Timeout: no outcome in time; it may still commit.
	ProposeTimeout
	// Superseded: another leader's entry took the slot; the command is not in the log.
	ProposeSuperseded
	// Unknown: the node stopped or lost leadership first; it may still commit.
	ProposeUnknown
	// Stopped: this replica is not running.
	ProposeStopped
)

// ProposeResult is a proposal's outcome.
type ProposeResult struct {
	Kind  ProposeKind
	Index consensus.Index
	Hint  int // NotLeader: the leader's id, or consensus.None
}

// OK reports whether the proposal committed.
func (r ProposeResult) OK() bool { return r.Kind == ProposeOK }

// Status is a replica's published state.
type Status struct {
	ID             consensus.NodeID
	Role           consensus.Role
	Term           consensus.Term
	Leader         int // consensus.None if unknown
	Commit         consensus.Index
	Applied        consensus.Index
	Base           consensus.Index
	LastIndex      consensus.Index
	Pending        int
	Halted         string // empty = running
	DigestsChecked uint64
}

// queue is an unbounded FIFO with one consumer.
type queue[T any] struct {
	mu    sync.Mutex
	items []T
	head  int
	sig   chan struct{}
}

func newQueue[T any]() *queue[T] { return &queue[T]{sig: make(chan struct{}, 1)} }

func (q *queue[T]) push(v T) {
	q.mu.Lock()
	q.items = append(q.items, v)
	q.mu.Unlock()
	select {
	case q.sig <- struct{}{}:
	default:
	}
}

func (q *queue[T]) tryPop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if q.head == len(q.items) {
		return zero, false
	}
	v := q.items[q.head]
	q.items[q.head] = zero
	q.head++
	if q.head == len(q.items) {
		q.items, q.head = q.items[:0], 0
	} else {
		if q.head > 1024 && q.head*2 > len(q.items) {
			n := copy(q.items, q.items[q.head:])
			clear(q.items[n:])
			q.items, q.head = q.items[:n], 0
		}
		select { // more waiting: keep the consumer awake
		case q.sig <- struct{}{}:
		default:
		}
	}
	return v, true
}

// popUntil waits for an item until deadline.
func (q *queue[T]) popUntil(deadline time.Time) (T, bool) {
	for {
		if v, ok := q.tryPop(); ok {
			return v, true
		}
		d := time.Until(deadline)
		if d <= 0 {
			var zero T
			return zero, false
		}
		t := time.NewTimer(d)
		select {
		case <-q.sig:
			t.Stop()
		case <-t.C:
		}
	}
}

func (q *queue[T]) pop() T {
	for {
		if v, ok := q.tryPop(); ok {
			return v
		}
		<-q.sig
	}
}

type hashAnswer struct {
	index consensus.Index
	d     Digest
	ok    bool
}

type evKind int

const (
	evInbound evKind = iota
	evPersistDone
	evPropose
	evHash
	evSnapshotStored
	evDigestComputed
	evFatal
	evStop
)

type event struct {
	kind      evKind
	in        inbound
	token     uint64
	payload   []byte
	reply     chan ProposeResult
	hashReply chan hashAnswer
	index     consensus.Index
	digest    Digest
	text      string
}

type jobKindA int

const (
	aApply jobKindA = iota
	aSnapshot
	aRestore
	aHash
	aStop
)

type applyJob struct {
	kind      jobKindA
	index     consensus.Index
	entry     consensus.Entry
	term      consensus.Term
	hashReply chan hashAnswer
}

type walItem struct {
	p    consensus.Persist
	stop bool
}

type pendingProposal struct {
	term     consensus.Term
	reply    chan ProposeResult
	deadline time.Time
}

// Replica is one running member of a replicated group.
type Replica struct {
	// Filter cuts links for tests.
	Filter *Filter
	cfg    ReplicaConfig
	sm     StateMachine
	wal    *Wal
	snaps  *SnapStore
	node   *consensus.Node
	tr     *Transport

	events *queue[event]
	walQ   *queue[walItem]
	jobs   *queue[applyJob]
	done   chan struct{} // closed when the driver has fully stopped

	stopped   atomic.Bool
	abort     atomic.Bool
	finished  atomic.Bool
	smApplied atomic.Uint64

	// Driver-goroutine state.
	pending         map[consensus.Index]pendingProposal
	compactTokens   map[uint64]consensus.Index
	digests         map[consensus.Index]map[consensus.NodeID]Digest
	digestsVerified uint64
	halted          string
	snapshotPending bool

	statusMu sync.Mutex
	status   Status
}

// StartReplica recovers from disk and starts the replica's goroutines.
func StartReplica(cfg ReplicaConfig, sm StateMachine, filter *Filter) (*Replica, error) {
	if filter == nil {
		filter = NewFilter()
	}
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	r := &Replica{Filter: filter, cfg: cfg, sm: sm,
		events: newQueue[event](), walQ: newQueue[walItem](), jobs: newQueue[applyJob](),
		done: make(chan struct{}), pending: map[consensus.Index]pendingProposal{},
		compactTokens: map[uint64]consensus.Index{}, digests: map[consensus.Index]map[consensus.NodeID]Digest{}}
	wal, disk, _, err := OpenWal(filepath.Join(cfg.Dir, "wal"))
	if err != nil {
		return nil, err
	}
	r.wal = wal
	fail := func(e error) (*Replica, error) { wal.Close(); return nil, e }
	if r.snaps, err = OpenSnapStore(filepath.Join(cfg.Dir, "snap")); err != nil {
		return fail(err)
	}
	if disk.Base > 0 {
		_, data, ok := r.snaps.Get(disk.Base)
		if !ok {
			return fail(fmt.Errorf("log base %d needs a snapshot that cannot be read", disk.Base))
		}
		if err := sm.Restore(disk.Base, data); err != nil {
			return fail(fmt.Errorf("state machine: %w", err))
		}
	}
	r.smApplied.Store(uint64(disk.Base))
	peers := make([]consensus.NodeID, len(cfg.Members))
	for i, m := range cfg.Members {
		peers[i] = m.ID
	}
	seed := uint64(time.Now().UnixNano()) ^ cfg.ClusterID ^ uint64(cfg.ID)<<32
	r.node = consensus.NewNode(cfg.ID, peers, cfg.Consensus, seed, disk)
	r.status = Status{ID: cfg.ID, Term: r.node.Term(), Leader: consensus.None, Commit: r.node.CommitIndex(),
		Applied: r.node.AppliedIndex(), Base: r.node.Base(), LastIndex: r.node.LastIndex()}
	r.tr, err = NewTransport(cfg.ID, cfg.Members, cfg.ClusterID,
		func(in inbound) { r.events.push(event{kind: evInbound, in: in}) }, r.snaps, filter, cfg.AuthKey)
	if err != nil {
		return fail(err)
	}
	walDone, applierDone := make(chan struct{}), make(chan struct{})
	go func() { r.walLoop(); close(walDone) }()
	go func() { r.applierLoop(); close(applierDone) }()
	go func() {
		r.run(walDone, applierDone)
		close(r.done)
	}()
	return r, nil
}

// Config is the configuration this replica runs with.
func (r *Replica) Config() ReplicaConfig { return r.cfg }

// Propose submits a pre-encoded log entry payload and blocks until it is committed.
func (r *Replica) Propose(payload []byte) ProposeResult { return <-r.ProposeAsync(payload) }

// ProposeAsync submits a payload; the result arrives on the returned channel.
func (r *Replica) ProposeAsync(payload []byte) <-chan ProposeResult {
	reply := make(chan ProposeResult, 1)
	if r.stopped.Load() || r.finished.Load() {
		reply <- ProposeResult{Kind: ProposeStopped, Hint: consensus.None}
		return reply
	}
	r.events.push(event{kind: evPropose, payload: payload, reply: reply})
	if r.finished.Load() {
		r.answerStragglers() // the driver ended while we queued
	}
	return reply
}

// Barrier is a linearizable read barrier: it commits an empty entry and waits
// for the state machine to apply it. A deposed leader cannot commit, so it
// cannot answer.
func (r *Replica) Barrier() ProposeResult {
	res := r.Propose(nil)
	if !res.OK() {
		return res
	}
	end := time.Now().Add(30 * time.Second)
	for r.smApplied.Load() < uint64(res.Index) {
		if time.Now().After(end) {
			return ProposeResult{Kind: ProposeTimeout, Hint: consensus.None}
		}
		time.Sleep(200 * time.Microsecond)
	}
	return res
}

// Status is the replica's latest published state.
func (r *Replica) Status() Status {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	return r.status
}

// StateHash is (applied index, state digest), if the state machine supports digests.
func (r *Replica) StateHash() (consensus.Index, Digest, bool) {
	if r.stopped.Load() || r.finished.Load() {
		return 0, Digest{}, false
	}
	reply := make(chan hashAnswer, 1)
	r.events.push(event{kind: evHash, hashReply: reply})
	if r.finished.Load() {
		r.answerStragglers()
	}
	a := <-reply
	return a.index, a.d, a.ok
}

// Stop is a graceful stop: queued WAL writes are flushed first.
func (r *Replica) Stop() { r.shutdown(false) }

// Crash simulates a power cut: goroutines stop at once; writes not yet fsynced are discarded.
func (r *Replica) Crash() { r.shutdown(true) }

func (r *Replica) shutdown(abort bool) {
	if !r.stopped.Swap(true) {
		if abort {
			r.abort.Store(true)
		}
		r.events.push(event{kind: evStop})
	}
	<-r.done
}

// ---- WAL goroutine ----

func (r *Replica) walLoop() {
	defer r.wal.Close()
	for {
		first := r.walQ.pop()
		if first.stop {
			return
		}
		if r.abort.Load() {
			return
		}
		batch := []consensus.Persist{first.p}
		quit := false
		for {
			more, ok := r.walQ.tryPop()
			if !ok {
				break
			}
			if more.stop {
				quit = true
				break
			}
			batch = append(batch, more.p)
		}
		compacted := false
		var err error
		for _, p := range batch {
			r.wal.Append(p)
			compacted = compacted || p.HasCompact
		}
		if err = r.wal.Sync(!r.cfg.NoFsync); err == nil {
			if r.abort.Load() {
				return
			}
			if compacted {
				err = r.wal.Rewrite()
			}
		}
		if err != nil {
			r.fatal("wal write failed: " + err.Error())
			return
		}
		r.events.push(event{kind: evPersistDone, token: batch[len(batch)-1].Token})
		if quit {
			return
		}
	}
}

// ---- applier goroutine ----

func (r *Replica) applierLoop() {
	for {
		j := r.jobs.pop()
		switch j.kind {
		case aApply:
			r.sm.Apply(j.index, j.entry)
			r.smApplied.Store(uint64(j.index))
			if r.cfg.DigestEvery > 0 && uint64(j.index)%r.cfg.DigestEvery == 0 {
				if d, ok := r.sm.Digest(j.index); ok {
					r.events.push(event{kind: evDigestComputed, index: j.index, digest: d})
				}
			}
		case aSnapshot:
			data := r.sm.Snapshot(j.index)
			if err := r.snaps.Put(j.index, j.term, data); err != nil {
				r.fatal(fmt.Sprintf("writing snapshot %d: %v", j.index, err))
			} else {
				r.events.push(event{kind: evSnapshotStored, index: j.index})
			}
		case aRestore:
			_, data, ok := r.snaps.Get(j.index)
			if !ok {
				r.fatal(fmt.Sprintf("reading snapshot %d", j.index))
				break
			}
			if err := r.sm.Restore(j.index, data); err != nil {
				r.fatal("restoring snapshot: " + err.Error())
				break
			}
			r.smApplied.Store(uint64(j.index))
		case aHash:
			at := consensus.Index(r.smApplied.Load())
			d, ok := r.sm.DeepDigest(at)
			j.hashReply <- hashAnswer{at, d, ok}
		case aStop:
			return
		}
	}
}

// answerStragglers answers anything still queued that expects a reply (the driver is gone).
func (r *Replica) answerStragglers() {
	for {
		e, ok := r.events.tryPop()
		if !ok {
			return
		}
		if e.reply != nil {
			e.reply <- ProposeResult{Kind: ProposeStopped, Hint: consensus.None}
		}
		if e.hashReply != nil {
			e.hashReply <- hashAnswer{}
		}
	}
}

func (r *Replica) fatal(why string) { r.events.push(event{kind: evFatal, text: why}) }

// ---- driver ----

func (r *Replica) run(walDone, applierDone <-chan struct{}) {
	nextTick := time.Now().Add(r.cfg.Tick)
	for running := true; running; {
		replicate := false
		if ev, ok := r.events.popUntil(nextTick); ok {
			running = r.handle(ev, &replicate)
			for i := 0; running && i < 4096; i++ {
				more, ok := r.events.tryPop()
				if !ok {
					break
				}
				running = r.handle(more, &replicate)
			}
		}
		if now := time.Now(); !now.Before(nextTick) {
			r.node.Tick()
			nextTick = now.Add(r.cfg.Tick)
			r.expire(now)
		}
		if replicate {
			r.node.Replicate()
		}
		r.pump()
		r.maybeCompact()
		r.pump()
		r.publishStatus()
		if r.halted != "" {
			break
		}
	}
	r.publishStatus()
	for _, p := range r.pending {
		p.reply <- ProposeResult{Kind: ProposeUnknown, Hint: consensus.None}
	}
	r.pending = map[consensus.Index]pendingProposal{}
	r.finished.Store(true)
	r.answerStragglers()
	r.tr.Shutdown()
	r.jobs.push(applyJob{kind: aStop})
	<-applierDone
	r.walQ.push(walItem{stop: true})
	<-walDone
}

func (r *Replica) handle(ev event, replicate *bool) bool {
	switch ev.kind {
	case evStop:
		return false
	case evPersistDone:
		r.node.Persisted(ev.token)
		for tok, from := range r.compactTokens {
			if tok <= ev.token {
				r.snaps.GC(from)
				delete(r.compactTokens, tok)
			}
		}
	case evInbound:
		in := ev.in
		switch in.kind {
		case inMsg:
			r.node.Step(in.from, in.msg)
		case inSnapshot:
			fmt.Fprintf(os.Stderr, "orderer-cluster node %d: installing snapshot %d from node %d\n", r.cfg.ID, in.index, in.from)
			r.node.InstallSnapshot(in.from, in.term, in.index, in.lastTerm)
		case inDigest:
			r.recordDigest(in.index, in.from, in.digest)
			r.checkDigests(in.index)
		}
	case evPropose:
		applied := uint64(r.node.AppliedIndex())
		behind := uint64(0)
		if sm := r.smApplied.Load(); applied > sm {
			behind = applied - sm
		}
		if len(r.pending) >= r.cfg.MaxPending || behind > r.cfg.MaxApplyBacklog {
			ev.reply <- ProposeResult{Kind: ProposeBusy, Hint: consensus.None}
		} else if idx, ok := r.node.Propose(ev.payload); ok {
			r.pending[idx] = pendingProposal{r.node.Term(), ev.reply, time.Now().Add(r.cfg.ProposeTimeout)}
			*replicate = true
		} else {
			ev.reply <- ProposeResult{Kind: ProposeNotLeader, Hint: r.node.Leader()}
		}
	case evHash:
		r.jobs.push(applyJob{kind: aHash, hashReply: ev.hashReply})
	case evSnapshotStored:
		r.snapshotPending = false
		if ev.index > r.node.Base() && ev.index <= r.node.AppliedIndex() {
			r.node.Compact(ev.index)
		}
	case evDigestComputed:
		r.recordDigest(ev.index, r.cfg.ID, ev.digest)
		for _, m := range r.cfg.Members {
			if m.ID != r.cfg.ID {
				r.tr.SendDigest(m.ID, ev.index, ev.digest)
			}
		}
		r.checkDigests(ev.index)
	case evFatal:
		r.halt(ev.text)
	}
	return true
}

func (r *Replica) recordDigest(i consensus.Index, from consensus.NodeID, d Digest) {
	m := r.digests[i]
	if m == nil {
		m = map[consensus.NodeID]Digest{}
		r.digests[i] = m
	}
	m[from] = d
}

func (r *Replica) expire(now time.Time) {
	for i, p := range r.pending {
		if !p.deadline.After(now) {
			p.reply <- ProposeResult{Kind: ProposeTimeout, Hint: consensus.None}
			delete(r.pending, i)
		}
	}
}

func (r *Replica) pump() {
	for {
		effects := r.node.TakeEffects()
		if len(effects) == 0 {
			return
		}
		for _, e := range effects {
			switch e.Kind {
			case consensus.EffSend:
				r.tr.Send(e.To, e.Msg)
			case consensus.EffPersist:
				if e.Persist.HasCompact {
					r.compactTokens[e.Persist.Token] = e.Persist.CompactIdx
				}
				r.walQ.push(walItem{p: e.Persist})
			case consensus.EffApply:
				r.committed(e.Index, e.Entry)
			case consensus.EffNeedSnapshot:
				r.tr.SendSnapshot(e.To, r.node.Term(), e.Index, e.Term)
			case consensus.EffRestore:
				r.jobs.push(applyJob{kind: aRestore, index: e.Index})
			}
		}
	}
}

// committed acknowledges an entry's proposer, then queues it for the state machine.
func (r *Replica) committed(index consensus.Index, entry consensus.Entry) {
	if p, ok := r.pending[index]; ok {
		if p.term == entry.Term {
			p.reply <- ProposeResult{Kind: ProposeOK, Index: index, Hint: consensus.None}
		} else {
			p.reply <- ProposeResult{Kind: ProposeSuperseded, Hint: consensus.None}
		}
		delete(r.pending, index)
	}
	r.jobs.push(applyJob{kind: aApply, index: index, entry: entry})
}

// checkDigests fail-stops if this node's state differs from the majority's.
func (r *Replica) checkDigests(index consensus.Index) {
	seen, ok := r.digests[index]
	if !ok {
		return
	}
	own, ok := seen[r.cfg.ID]
	if !ok {
		return
	}
	n := len(r.cfg.Members)
	quorum := n/2 + 1
	groups := map[Digest]int{}
	for _, d := range seen {
		groups[d]++
	}
	otherMajority := false
	for d, c := range groups {
		if c >= quorum && d != own {
			otherMajority = true
		}
	}
	switch {
	case groups[own] >= quorum:
		if len(seen) == n {
			r.digestsVerified++
			delete(r.digests, index)
		}
	case otherMajority:
		r.halt(fmt.Sprintf("state diverged from the majority at index %d (own digest %x)", index, own))
	case len(seen) == n:
		r.halt(fmt.Sprintf("state digests at index %d have no majority", index))
	}
	if r.cfg.DigestEvery > 0 {
		var keepFrom consensus.Index
		if span := consensus.Index(r.cfg.DigestEvery * 64); index > span {
			keepFrom = index - span
		}
		for i := range r.digests {
			if i < keepFrom {
				delete(r.digests, i)
			}
		}
	}
}

func (r *Replica) halt(why string) {
	fmt.Fprintf(os.Stderr, "orderer-cluster node %d: HALT: %s\n", r.cfg.ID, why)
	r.halted = why
}

func (r *Replica) maybeCompact() {
	if r.cfg.CompactEvery == 0 || r.halted != "" || r.snapshotPending {
		return
	}
	applied := r.node.AppliedIndex()
	if applied < r.node.Base() || uint64(applied-r.node.Base()) < r.cfg.CompactEvery {
		return
	}
	e, ok := r.node.EntryAt(applied)
	if !ok {
		return
	}
	// The applier snapshots after applying `applied`, then reports back; only
	// then does the log drop the prefix.
	r.snapshotPending = true
	r.jobs.push(applyJob{kind: aSnapshot, index: applied, term: e.Term})
}

func (r *Replica) publishStatus() {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	r.status = Status{ID: r.cfg.ID, Role: r.node.Role(), Term: r.node.Term(), Leader: r.node.Leader(),
		Commit: r.node.CommitIndex(), Applied: consensus.Index(r.smApplied.Load()), Base: r.node.Base(),
		LastIndex: r.node.LastIndex(), Pending: len(r.pending), Halted: r.halted, DigestsChecked: r.digestsVerified}
}

// ErrStopped is returned by helpers when the replica is not running.
var ErrStopped = errors.New("replica stopped")
