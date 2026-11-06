// Package consensus is the replication protocol core (spec/REPLICATION.md) as
// a pure state machine: no I/O, no clock. The caller feeds inputs (Tick, Step,
// Propose, Persisted, Compact, InstallSnapshot) and executes the effects it
// emits. It is a faithful port of orderer-rust's orderer-consensus; the
// conformance vectors in vectors/replication/ prove the two produce identical
// effects and state for the same inputs.
package consensus

import "sort"

type (
	NodeID uint8
	Term   uint32
	Index  uint64
)

// None marks "no node" in optional node fields (leader, vote).
const None = -1

// Rng is xorshift64* seeded through splitmix64, bit-identical to the Rust port.
type Rng struct{ s uint64 }

func NewRng(seed uint64) *Rng {
	z := seed + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	if z == 0 {
		z = 1
	}
	return &Rng{s: z}
}

func (r *Rng) next() uint64 {
	x := r.s
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.s = x
	return x * 0x2545F4914F6CDD1D
}

func (r *Rng) below(n uint64) uint64 { return r.next() % n }

type Entry struct {
	Term Term
	Data []byte
}

type MsgKind int

const (
	MsgVote MsgKind = iota
	MsgVoteReply
	MsgAppend
	MsgAppendReply
)

// Msg carries the fields of all four message kinds; Kind says which apply.
type Msg struct {
	Kind MsgKind
	Term Term
	// Vote
	LastIndex Index
	LastTerm  Term
	Pre       bool
	// VoteReply
	Granted bool
	// Append
	PrevIndex Index
	PrevTerm  Term
	Entries   []Entry
	Commit    Index
	// AppendReply
	Success bool
	Index   Index
}

// Persist is one atomic disk write: set the hard state, optionally discard the
// log prefix a snapshot covers, truncate the log to From-1, then append.
type Persist struct {
	Token      uint64
	Term       Term
	VotedFor   int // None or a node id
	From       Index
	Entries    []Entry
	HasCompact bool
	CompactIdx Index
	CompactTrm Term
}

type EffectKind int

const (
	EffSend EffectKind = iota
	EffPersist
	EffApply
	EffNeedSnapshot
	EffRestore
)

type Effect struct {
	Kind    EffectKind
	To      NodeID  // Send, NeedSnapshot
	Msg     Msg     // Send
	Persist Persist // Persist
	Index   Index   // Apply, NeedSnapshot, Restore
	Entry   Entry   // Apply
	Term    Term    // NeedSnapshot, Restore
}

// Disk is what survives a crash.
type Disk struct {
	Term     Term
	VotedFor int // None or a node id
	Log      []Entry
	Base     Index
	BaseTerm Term
}

type Config struct {
	ElectionTicks  uint32
	HeartbeatTicks uint32
	MaxBatch       int
	PreVote        bool
}

type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Follower", "PreCandidate", "Candidate", "Leader"}[r]
}

type write struct {
	token uint64
	upto  Index
	epoch uint64
	gated []Effect
}

// Node is one replica's protocol state.
type Node struct {
	id    NodeID
	peers []NodeID
	cfg   Config
	rng   *Rng

	term     Term
	votedFor int
	log      []Entry
	base     Index
	baseTerm Term

	role    Role
	leader  int
	commit  Index
	applied Index
	durable Index
	epoch   uint64
	writes  []write
	nextTok uint64

	votes      []bool
	nextIndex  []Index
	matchIndex []Index
	heard      []bool
	snapWait   []uint32

	electionElapsed  uint32
	electionTimeout  uint32
	heartbeatElapsed uint32

	out   []Effect
	fault string
}

// NewNode starts (or restarts after a crash) a node from what was on disk.
func NewNode(id NodeID, peers []NodeID, cfg Config, seed uint64, disk Disk) *Node {
	ps := append([]NodeID(nil), peers...)
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	n := len(ps)
	nd := &Node{
		id: id, peers: ps, cfg: cfg,
		rng:  NewRng(seed ^ (uint64(id) << 56)),
		term: disk.Term, votedFor: disk.VotedFor,
		log: append([]Entry(nil), disk.Log...), base: disk.Base, baseTerm: disk.BaseTerm,
		role: Follower, leader: None,
		commit: disk.Base, applied: disk.Base,
		durable: disk.Base + Index(len(disk.Log)),
		nextTok: 1,
		votes:   make([]bool, n), nextIndex: make([]Index, n), matchIndex: make([]Index, n),
		heard: make([]bool, n), snapWait: make([]uint32, n),
	}
	for i := range nd.nextIndex {
		nd.nextIndex[i] = 1
	}
	nd.resetTimeout()
	return nd
}

// ---- observers ----

func (n *Node) ID() NodeID          { return n.id }
func (n *Node) Role() Role          { return n.role }
func (n *Node) Term() Term          { return n.term }
func (n *Node) Leader() int         { return n.leader }
func (n *Node) CommitIndex() Index  { return n.commit }
func (n *Node) AppliedIndex() Index { return n.applied }
func (n *Node) Base() Index         { return n.base }
func (n *Node) LastIndex() Index    { return n.base + Index(len(n.log)) }
func (n *Node) Fault() string       { return n.fault }

// TakeEffects returns and clears the effects emitted so far.
func (n *Node) TakeEffects() []Effect {
	o := n.out
	n.out = nil
	return o
}

func (n *Node) entryAt(i Index) (Entry, bool) {
	if i > n.base && uint64(i-n.base-1) < uint64(len(n.log)) {
		return n.log[i-n.base-1], true
	}
	return Entry{}, false
}

// EntryAt is the log entry at index i, if the node still holds it.
func (n *Node) EntryAt(i Index) (Entry, bool) { return n.entryAt(i) }

// ---- inputs ----

func (n *Node) Tick() {
	for i, w := range n.snapWait {
		if w > 0 {
			n.snapWait[i] = w - 1
		}
	}
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			// Check-quorum: step down without contact from a majority.
			me := n.pos(n.id)
			heard := 0
			for i, h := range n.heard {
				if h || i == me {
					heard++
				}
			}
			if heard < n.quorum() {
				n.role = Follower
				n.leader = None
			}
			for i := range n.heard {
				n.heard[i] = false
			}
			n.resetTimeout()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.startElection()
	}
}

// Propose appends client data. Only the leader accepts: it returns the entry's
// index and true. Otherwise false (use Leader() for a redirect hint). Call
// Replicate afterwards.
func (n *Node) Propose(data []byte) (Index, bool) {
	if n.role != Leader {
		return 0, false
	}
	n.leaderAppend(Entry{Term: n.term, Data: data})
	return n.LastIndex(), true
}

func (n *Node) Replicate() {
	if n.role == Leader {
		n.broadcastAppend()
	}
}

// Compact is called once the host stored a snapshot of the state after
// applying index; the log prefix it covers is dropped.
func (n *Node) Compact(index Index) {
	if index <= n.base || index > n.applied {
		n.fault = "compact outside (base, applied]"
		return
	}
	t := n.termAt(index)
	n.log = append([]Entry(nil), n.log[index-n.base:]...)
	n.base = index
	n.baseTerm = t
	n.persistWith(n.LastIndex()+1, nil, true, index, t)
}

// InstallSnapshot is called once the host received and stored the leader's
// snapshot.
func (n *Node) InstallSnapshot(from NodeID, term Term, index Index, lastTerm Term) {
	if term < n.term {
		return
	}
	n.becomeFollower(term, int(from))
	replyTerm := n.term
	if index <= n.commit {
		n.sendGated(from, Msg{Kind: MsgAppendReply, Term: replyTerm, Success: true, Index: n.commit})
		return
	}
	if index <= n.LastIndex() && n.termAt(index) == lastTerm {
		n.log = append([]Entry(nil), n.log[index-n.base:]...)
	} else {
		n.log = nil
		n.durable = index
		n.epoch++
	}
	n.base = index
	n.baseTerm = lastTerm
	n.commit, n.applied = index, index
	n.persistWith(n.LastIndex()+1, nil, true, index, lastTerm)
	n.out = append(n.out, Effect{Kind: EffRestore, Index: index, Term: lastTerm})
	n.sendGated(from, Msg{Kind: MsgAppendReply, Term: replyTerm, Success: true, Index: index})
}

// Persisted reports that the disk finished the write with token (and all
// earlier ones).
func (n *Node) Persisted(token uint64) {
	for len(n.writes) > 0 && n.writes[0].token <= token {
		w := n.writes[0]
		n.writes = n.writes[1:]
		if w.epoch == n.epoch {
			d := n.durable
			if w.upto > d {
				d = w.upto
			}
			if li := n.LastIndex(); d > li {
				d = li
			}
			n.durable = d
		}
		n.out = append(n.out, w.gated...)
	}
	if n.role == Leader {
		n.tryCommit()
	}
}

func (n *Node) Step(from NodeID, m Msg) {
	switch m.Kind {
	case MsgVote:
		n.onVote(from, m.Term, m.LastIndex, m.LastTerm, m.Pre)
	case MsgVoteReply:
		n.onVoteReply(from, m.Term, m.Granted, m.Pre)
	case MsgAppend:
		n.onAppend(from, m.Term, m.PrevIndex, m.PrevTerm, m.Entries, m.Commit)
	case MsgAppendReply:
		n.onAppendReply(from, m.Term, m.Success, m.Index)
	}
}

// ---- helpers ----

func (n *Node) pos(id NodeID) int {
	for i, p := range n.peers {
		if p == id {
			return i
		}
	}
	return -1
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) lastTerm() Term {
	if len(n.log) == 0 {
		return n.baseTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAt(i Index) Term {
	if i == n.base {
		return n.baseTerm
	}
	if e, ok := n.entryAt(i); ok {
		return e.Term
	}
	return 0
}

func (n *Node) resetTimeout() {
	n.electionElapsed = 0
	t := uint64(n.cfg.ElectionTicks)
	n.electionTimeout = uint32(t + n.rng.below(t))
}

func (n *Node) others() []NodeID {
	var v []NodeID
	for _, p := range n.peers {
		if p != n.id {
			v = append(v, p)
		}
	}
	return v
}

func (n *Node) send(to NodeID, m Msg) {
	n.out = append(n.out, Effect{Kind: EffSend, To: to, Msg: m})
}

// sendGated sends once everything written so far is durable.
func (n *Node) sendGated(to NodeID, m Msg) {
	e := Effect{Kind: EffSend, To: to, Msg: m}
	if len(n.writes) > 0 {
		w := &n.writes[len(n.writes)-1]
		w.gated = append(w.gated, e)
	} else {
		n.out = append(n.out, e)
	}
}

func (n *Node) persist(from Index, entries []Entry) { n.persistWith(from, entries, false, 0, 0) }

func (n *Node) persistWith(from Index, entries []Entry, hasCompact bool, cidx Index, cterm Term) {
	tok := n.nextTok
	n.nextTok++
	upto := from - 1 + Index(len(entries))
	n.writes = append(n.writes, write{token: tok, upto: upto, epoch: n.epoch})
	n.out = append(n.out, Effect{Kind: EffPersist, Persist: Persist{
		Token: tok, Term: n.term, VotedFor: n.votedFor, From: from, Entries: entries,
		HasCompact: hasCompact, CompactIdx: cidx, CompactTrm: cterm,
	}})
}

func (n *Node) persistHardState() { n.persist(n.LastIndex()+1, nil) }

func (n *Node) truncateFrom(index Index) {
	if index <= n.commit {
		n.fault = "truncating a committed entry"
	}
	n.log = n.log[:index-n.base-1]
	if index-1 < n.durable {
		n.durable = index - 1
	}
	n.epoch++
}

func (n *Node) becomeFollower(term Term, leader int) {
	if term > n.term {
		n.term = term
		n.votedFor = None
		n.persistHardState()
	}
	n.role = Follower
	n.leader = leader
	n.resetTimeout()
}

func (n *Node) applyCommitted() {
	for n.applied < n.commit {
		n.applied++
		e, _ := n.entryAt(n.applied)
		n.out = append(n.out, Effect{Kind: EffApply, Index: n.applied, Entry: e})
	}
}

// ---- elections ----

func (n *Node) startElection() {
	if n.cfg.PreVote && len(n.peers) > 1 {
		n.role = PreCandidate
		n.leader = None
		for i := range n.votes {
			n.votes[i] = false
		}
		n.votes[n.pos(n.id)] = true
		n.resetTimeout()
		for _, p := range n.others() {
			n.send(p, Msg{Kind: MsgVote, Term: n.term + 1, LastIndex: n.LastIndex(), LastTerm: n.lastTerm(), Pre: true})
		}
	} else {
		n.campaign()
	}
}

func (n *Node) voteCount() int {
	c := 0
	for _, v := range n.votes {
		if v {
			c++
		}
	}
	return c
}

func (n *Node) campaign() {
	n.term++
	n.votedFor = int(n.id)
	n.role = Candidate
	n.leader = None
	for i := range n.votes {
		n.votes[i] = false
	}
	n.votes[n.pos(n.id)] = true
	n.persistHardState()
	n.resetTimeout()
	for _, p := range n.others() {
		n.sendGated(p, Msg{Kind: MsgVote, Term: n.term, LastIndex: n.LastIndex(), LastTerm: n.lastTerm()})
	}
	if n.voteCount() >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = int(n.id)
	next := n.LastIndex() + 1
	for i := range n.nextIndex {
		n.nextIndex[i] = next
		n.matchIndex[i] = 0
		n.heard[i] = false
	}
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.leaderAppend(Entry{Term: n.term}) // the term's no-op
	n.broadcastAppend()
}

func (n *Node) upToDate(lastIndex Index, lastTerm Term) bool {
	if lastTerm != n.lastTerm() {
		return lastTerm > n.lastTerm()
	}
	return lastIndex >= n.LastIndex()
}

func (n *Node) onVote(from NodeID, term Term, lastIndex Index, lastTerm Term, pre bool) {
	utd := n.upToDate(lastIndex, lastTerm)
	if pre {
		live := n.role == Leader || (n.leader != None && n.electionElapsed < n.electionTimeout)
		granted := term > n.term && utd && !live
		rt := n.term
		if granted {
			rt = term
		}
		n.send(from, Msg{Kind: MsgVoteReply, Term: rt, Granted: granted, Pre: true})
		return
	}
	if term < n.term {
		n.send(from, Msg{Kind: MsgVoteReply, Term: n.term})
		return
	}
	if term > n.term {
		n.becomeFollower(term, None)
	}
	free := n.votedFor == None || n.votedFor == int(from)
	granted := free && utd
	if granted {
		if n.votedFor != int(from) {
			n.votedFor = int(from)
			n.persistHardState()
		}
		n.resetTimeout()
	}
	n.sendGated(from, Msg{Kind: MsgVoteReply, Term: n.term, Granted: granted})
}

func (n *Node) onVoteReply(from NodeID, term Term, granted, pre bool) {
	if pre {
		if n.role != PreCandidate {
			return
		}
		if !granted {
			if term > n.term {
				n.becomeFollower(term, None)
			}
			return
		}
		if term != n.term+1 {
			return
		}
		n.votes[n.pos(from)] = true
		if n.voteCount() >= n.quorum() {
			n.campaign()
		}
		return
	}
	if term > n.term {
		n.becomeFollower(term, None)
		return
	}
	if n.role != Candidate || term < n.term || !granted {
		return
	}
	n.votes[n.pos(from)] = true
	if n.voteCount() >= n.quorum() {
		n.becomeLeader()
	}
}

// ---- replication ----

func (n *Node) leaderAppend(e Entry) {
	n.log = append(n.log, e)
	n.persist(n.LastIndex(), []Entry{e})
}

func (n *Node) broadcastAppend() {
	for _, p := range n.others() {
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(to NodeID) {
	i := n.pos(to)
	next := n.nextIndex[i]
	if next < 1 {
		next = 1
	}
	if next <= n.base {
		if n.snapWait[i] == 0 {
			n.snapWait[i] = n.cfg.ElectionTicks * 2
			n.out = append(n.out, Effect{Kind: EffNeedSnapshot, To: to, Index: n.base, Term: n.baseTerm})
		}
		return
	}
	prev := next - 1
	start := int(prev - n.base)
	end := start + n.cfg.MaxBatch
	if end > len(n.log) {
		end = len(n.log)
	}
	es := append([]Entry(nil), n.log[start:end]...)
	sent := Index(len(es))
	m := Msg{Kind: MsgAppend, Term: n.term, PrevIndex: prev, PrevTerm: n.termAt(prev), Entries: es, Commit: n.commit}
	n.nextIndex[i] = next + sent
	n.send(to, m)
}

func (n *Node) onAppend(from NodeID, term Term, prevIndex Index, prevTerm Term, entries []Entry, leaderCommit Index) {
	if term < n.term {
		n.send(from, Msg{Kind: MsgAppendReply, Term: n.term})
		return
	}
	n.becomeFollower(term, int(from))
	if prevIndex < n.base {
		skip := int(n.base - prevIndex)
		if skip > len(entries) {
			skip = len(entries)
		}
		entries = entries[skip:]
		prevIndex += Index(skip)
		if prevIndex < n.base {
			n.sendGated(from, Msg{Kind: MsgAppendReply, Term: n.term, Success: true, Index: n.base})
			return
		}
		prevTerm = n.baseTerm
	}
	if prevIndex > n.LastIndex() || n.termAt(prevIndex) != prevTerm {
		hint := n.LastIndex()
		if prevIndex > 0 && prevIndex-1 < hint {
			hint = prevIndex - 1
		} else if prevIndex == 0 {
			hint = 0
		}
		n.sendGated(from, Msg{Kind: MsgAppendReply, Term: n.term, Index: hint})
		return
	}
	idx := prevIndex
	firstNew := -1
	for k := range entries {
		idx++
		if idx <= n.LastIndex() {
			if n.termAt(idx) != entries[k].Term {
				n.truncateFrom(idx)
				firstNew = k
				break
			}
		} else {
			firstNew = k
			break
		}
	}
	if firstNew >= 0 {
		fromIndex := prevIndex + 1 + Index(firstNew)
		fresh := append([]Entry(nil), entries[firstNew:]...)
		n.log = append(n.log, fresh...)
		n.persist(fromIndex, fresh)
	}
	matched := prevIndex + Index(len(entries))
	if leaderCommit > n.commit {
		c := leaderCommit
		if matched < c {
			c = matched
		}
		n.commit = c
		n.applyCommitted()
	}
	n.sendGated(from, Msg{Kind: MsgAppendReply, Term: n.term, Success: true, Index: matched})
}

func (n *Node) onAppendReply(from NodeID, term Term, success bool, index Index) {
	if term > n.term {
		n.becomeFollower(term, None)
		return
	}
	if n.role != Leader || term < n.term {
		return
	}
	i := n.pos(from)
	n.heard[i] = true
	if success {
		if index > n.matchIndex[i] {
			n.matchIndex[i] = index
		}
		if index+1 > n.nextIndex[i] {
			n.nextIndex[i] = index + 1
		}
		n.tryCommit()
		if n.nextIndex[i] <= n.LastIndex() {
			n.sendAppend(from)
		}
	} else {
		v := n.nextIndex[i] - 1
		if index+1 < v {
			v = index + 1
		}
		if v < 1 {
			v = 1
		}
		n.nextIndex[i] = v
		n.sendAppend(from)
	}
}

func (n *Node) tryCommit() {
	me := n.pos(n.id)
	m := make([]Index, len(n.peers))
	for i := range m {
		if i == me {
			m[i] = n.durable
		} else {
			m[i] = n.matchIndex[i]
		}
	}
	sort.Slice(m, func(a, b int) bool { return m[a] > m[b] })
	idx := m[n.quorum()-1]
	// Only current-term entries are committed by counting (Raft figure 8).
	if idx > n.commit && n.termAt(idx) == n.term {
		n.commit = idx
		n.applyCommitted()
	}
}
