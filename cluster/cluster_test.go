package cluster

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// End to end: real replicas over TCP with real files (mirrors orderer-rust's
// orderer-cluster/tests and orderer-cpp's cluster_e2e_test).

func payload(i uint64) []byte { return putU64(nil, i) }
func idOf(p []byte) uint64    { return le.Uint64(p) }

// shared is a RecorderSm whose state the test can read while the applier runs.
type shared struct {
	mu         sync.Mutex
	sm         RecorderSm
	corruptAt  consensus.Index
	applyDelay atomic.Int64 // ms
}

func (s *shared) payloads() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ps := s.sm.Applied()
	return ps
}

func (s *shared) count() int { return len(s.payloads()) }

type probe struct{ s *shared }

func (p probe) Apply(i consensus.Index, e consensus.Entry) {
	if d := p.s.applyDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	if p.s.corruptAt == i && len(e.Data) > 0 {
		d := append([]byte(nil), e.Data...)
		d[0] ^= 0xff
		e.Data = d
	}
	p.s.sm.Apply(i, e)
}
func (p probe) Snapshot(i consensus.Index) []byte {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	return p.s.sm.Snapshot(i)
}
func (p probe) Restore(i consensus.Index, d []byte) error {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	return p.s.sm.Restore(i, d)
}
func (p probe) DeepDigest(i consensus.Index) (Digest, bool) { return p.Digest(i) }
func (p probe) Digest(i consensus.Index) (Digest, bool) {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	return p.s.sm.Digest(i)
}

func freeAddrs(t testing.TB, n int) []string {
	t.Helper()
	var ls []net.Listener
	var out []string
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		out = append(out, l.Addr().String())
	}
	for _, l := range ls {
		l.Close()
	}
	return out
}

type testCluster struct {
	t       testing.TB
	dir     string
	members []Member
	nodes   []*Replica
	shared  []*shared
	tune    func(*ReplicaConfig)
	dedup   bool
}

func newCluster(t testing.TB, n int, tune func(*ReplicaConfig), dedup bool) *testCluster {
	c := &testCluster{t: t, dir: t.TempDir(), tune: tune, dedup: dedup}
	for i, a := range freeAddrs(t, n) {
		c.members = append(c.members, Member{consensus.NodeID(i), a})
		c.nodes = append(c.nodes, nil)
		c.shared = append(c.shared, &shared{})
	}
	for i := range c.members {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, r := range c.nodes {
			if r != nil {
				r.Crash()
			}
		}
	})
	return c
}

func (c *testCluster) config(i int) ReplicaConfig {
	cfg := DefaultConfig(consensus.NodeID(i), c.members, filepath.Join(c.dir, fmt.Sprintf("n%d", i)))
	cfg.Tick = 5 * time.Millisecond
	cfg.Consensus = consensus.Config{ElectionTicks: 20, HeartbeatTicks: 2, MaxBatch: 256, PreVote: true}
	cfg.NoFsync = true
	cfg.ProposeTimeout = 3 * time.Second
	if c.tune != nil {
		c.tune(&cfg)
	}
	return cfg
}

func (c *testCluster) start(i int) {
	var sm StateMachine = probe{c.shared[i]}
	if c.dedup {
		sm = NewDedupSm(sm)
	}
	r, err := StartReplica(c.config(i), sm, nil)
	if err != nil {
		c.t.Fatalf("start %d: %v", i, err)
	}
	c.nodes[i] = r
}

func (c *testCluster) crash(i int) { c.nodes[i].Crash(); c.nodes[i] = nil }
func (c *testCluster) stop(i int)  { c.nodes[i].Stop(); c.nodes[i] = nil }
func (c *testCluster) restart(i int) {
	c.shared[i] = &shared{}
	c.start(i)
}

func (c *testCluster) leader() int {
	best, bt := -1, consensus.Term(0)
	for i, r := range c.nodes {
		if r == nil {
			continue
		}
		if s := r.Status(); s.Role == consensus.Leader && (best < 0 || s.Term > bt) {
			best, bt = i, s.Term
		}
	}
	return best
}

func (c *testCluster) waitLeader(within time.Duration) int {
	c.t.Helper()
	end := time.Now().Add(within)
	for {
		if l := c.leader(); l >= 0 {
			return l
		}
		if time.Now().After(end) {
			c.t.Fatalf("no leader within %v", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// propose goes via whichever node leads, retrying redirects. Timeout and Unknown are returned.
func (c *testCluster) propose(p []byte) ProposeResult {
	end := time.Now().Add(10 * time.Second)
	for {
		l := c.leader()
		if l < 0 {
			if time.Now().After(end) {
				return ProposeResult{Kind: ProposeStopped}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		r := c.nodes[l].Propose(p)
		if r.Kind == ProposeNotLeader || r.Kind == ProposeBusy || r.Kind == ProposeStopped {
			if time.Now().After(end) {
				return r
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		return r
	}
}

func (c *testCluster) mustPropose(p []byte) {
	c.t.Helper()
	if r := c.propose(p); !r.OK() {
		c.t.Fatalf("proposal failed: %+v", r)
	}
}

func (c *testCluster) waitApplied(who []int, atLeast int, within time.Duration) {
	c.t.Helper()
	end := time.Now().Add(within)
	for {
		ok := true
		for _, i := range who {
			ok = ok && c.shared[i].count() >= atLeast
		}
		if ok {
			return
		}
		if time.Now().After(end) {
			c.t.Fatalf("nodes did not apply %d", atLeast)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func equalPayloads(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// waitConverged waits until every running node has applied the same, stable sequence.
func (c *testCluster) waitConverged(within time.Duration) [][]byte {
	c.t.Helper()
	end := time.Now().Add(within)
	snap := func() [][][]byte {
		var v [][][]byte
		for i, r := range c.nodes {
			if r != nil {
				v = append(v, c.shared[i].payloads())
			}
		}
		return v
	}
	same := func(v [][][]byte) bool {
		for _, x := range v {
			if !equalPayloads(x, v[0]) {
				return false
			}
		}
		return true
	}
	for {
		a := snap()
		if same(a) {
			time.Sleep(100 * time.Millisecond)
			b := snap()
			if same(b) && len(b) == len(a) && (len(a) == 0 || equalPayloads(a[0], b[0])) {
				if len(a) == 0 {
					return nil
				}
				return a[0]
			}
		}
		if time.Now().After(end) {
			c.t.Fatal("replicas did not converge")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func seq(from, to uint64) [][]byte {
	var v [][]byte
	for i := from; i < to; i++ {
		v = append(v, payload(i))
	}
	return v
}

func (c *testCluster) expect(got [][]byte, from, to uint64, why string) {
	c.t.Helper()
	if !equalPayloads(got, seq(from, to)) {
		c.t.Fatalf("%s: got %d entries, want %d..%d", why, len(got), from, to)
	}
}

func ids3() []consensus.NodeID { return []consensus.NodeID{0, 1, 2} }

func TestElectsALeaderAndReplicatesInOrder(t *testing.T) {
	c := newCluster(t, 3, nil, false)
	c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 500; i++ {
		c.mustPropose(payload(i))
	}
	c.expect(c.waitConverged(15*time.Second), 0, 500, "replicated log")
}

func TestASingleNodeClusterWorks(t *testing.T) {
	c := newCluster(t, 1, nil, false)
	c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 100; i++ {
		c.mustPropose(payload(i))
	}
	if n := len(c.waitConverged(10 * time.Second)); n != 100 {
		t.Fatalf("%d entries", n)
	}
}

func TestFollowersRefuseProposalsAndPointToTheLeader(t *testing.T) {
	c := newCluster(t, 3, nil, false)
	l := c.waitLeader(5 * time.Second)
	c.mustPropose(payload(0))
	c.waitApplied([]int{0, 1, 2}, 1, 10*time.Second)
	f := 0
	if l == 0 {
		f = 1
	}
	if r := c.nodes[f].Propose(payload(1)); r.Kind != ProposeNotLeader || r.Hint != l {
		t.Fatalf("no redirect: %+v (leader %d)", r, l)
	}
}

func TestLeaderCrashLosesNoAcknowledgedCommand(t *testing.T) {
	c := newCluster(t, 3, nil, false)
	c.waitLeader(5 * time.Second)
	acked, maybe := map[uint64]bool{}, map[uint64]bool{}
	run := func(a, b uint64) {
		for i := a; i < b; i++ {
			if c.propose(payload(i)).OK() {
				acked[i] = true
			} else {
				maybe[i] = true
			}
		}
	}
	run(0, 200)
	old := c.leader()
	c.crash(old)
	run(200, 600)
	c.restart(old)
	got := c.waitConverged(20 * time.Second)
	seen := map[uint64]bool{}
	var prev uint64
	for _, p := range got {
		id := idOf(p)
		if seen[id] {
			t.Fatalf("command %d applied twice", id)
		}
		seen[id] = true
		if id < prev {
			t.Fatal("commands reordered")
		}
		prev = id
	}
	for a := range acked {
		if !seen[a] {
			t.Fatalf("acknowledged command %d lost", a)
		}
	}
	for g := range seen {
		if !acked[g] && !maybe[g] {
			t.Fatalf("%d was never proposed", g)
		}
	}
}

func TestAnIsolatedLeaderCannotCommitAndItsUncommittedEntriesVanish(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.ProposeTimeout = 700 * time.Millisecond }, false)
	old := c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 20; i++ {
		c.mustPropose(payload(i))
	}
	c.waitApplied([]int{0, 1, 2}, 20, 10*time.Second)
	c.nodes[old].Filter.Isolate(consensus.NodeID(old), ids3())
	if r := c.nodes[old].Propose(payload(1000)); r.Kind != ProposeTimeout && r.Kind != ProposeNotLeader && r.Kind != ProposeSuperseded {
		t.Fatalf("an isolated leader must not acknowledge: %+v", r)
	}
	// The majority elects a new leader and commits.
	end, newl := time.Now().Add(10*time.Second), -1
	for newl < 0 && time.Now().Before(end) {
		for i := 0; i < 3; i++ {
			if i != old && c.nodes[i].Status().Role == consensus.Leader {
				newl = i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newl < 0 {
		t.Fatal("majority did not elect a leader")
	}
	for i := uint64(20); i < 60; i++ {
		for {
			p := c.nodes[newl].Propose(payload(i))
			if p.OK() {
				break
			}
			if p.Kind != ProposeBusy && p.Kind != ProposeNotLeader {
				t.Fatalf("majority side failed: %+v", p)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	c.nodes[old].Filter.Heal()
	c.expect(c.waitConverged(15*time.Second), 0, 60, "entry 1000 must not survive")
}

func TestALaggingNodeCatchesUpBySnapshot(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.CompactEvery = 40 }, false)
	l := c.waitLeader(5 * time.Second)
	lag := 0
	if l == 0 {
		lag = 1
	}
	for i := uint64(0); i < 10; i++ {
		c.mustPropose(payload(i))
	}
	c.waitApplied([]int{0, 1, 2}, 10, 10*time.Second)
	c.stop(lag)
	for i := uint64(10); i < 400; i++ {
		c.mustPropose(payload(i))
	}
	end := time.Now().Add(10 * time.Second)
	for c.nodes[l].Status().Base == 0 && time.Now().Before(end) {
		time.Sleep(20 * time.Millisecond)
	}
	if c.nodes[l].Status().Base == 0 {
		t.Fatal("leader never compacted")
	}
	c.restart(lag)
	c.expect(c.waitConverged(20*time.Second), 0, 400, "after snapshot catch-up")
	if c.nodes[lag].Status().Base == 0 {
		t.Fatal("caught up without a snapshot install")
	}
}

func TestAWholeClusterRestartRecoversFromDisk(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.CompactEvery = 50; cfg.NoFsync = false }, false)
	c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 300; i++ {
		c.mustPropose(payload(i))
	}
	c.waitConverged(10 * time.Second)
	for i := 0; i < 3; i++ {
		c.stop(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.waitLeader(10 * time.Second)
	for i := uint64(300); i < 400; i++ {
		c.mustPropose(payload(i))
	}
	c.expect(c.waitConverged(15*time.Second), 0, 400, "after full restart")
}

func TestATornWalTailRecoversAndACorruptWalRefusesToStart(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.NoFsync = false }, false)
	l := c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 100; i++ {
		c.mustPropose(payload(i))
	}
	c.waitConverged(10 * time.Second)
	f := 0
	if l == 0 {
		f = 1
	}
	c.crash(f)
	wal := filepath.Join(c.dir, fmt.Sprintf("n%d", f), "wal", "raft.wal")
	// Half a frame of garbage, as a torn write leaves.
	fh, err := os.OpenFile(wal, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh.Write([]byte{0x40, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3})
	fh.Close()
	c.restart(f)
	for i := uint64(100); i < 150; i++ {
		c.mustPropose(payload(i))
	}
	c.expect(c.waitConverged(10*time.Second), 0, 150, "after torn-tail recovery")
	// Now real damage: flip a byte early in the file, with valid frames after it.
	c.stop(f)
	data, _ := os.ReadFile(wal)
	data[40] ^= 0xff
	os.WriteFile(wal, data, 0o644)
	if r, err := StartReplica(c.config(f), &RecorderSm{}, nil); err == nil {
		r.Crash()
		t.Fatal("a node with mid-log damage must not join")
	}
}

func TestStateDivergenceHaltsTheOddNodeOut(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.DigestEvery = 16 }, false)
	c.stop(2)
	c.shared[2] = &shared{corruptAt: 5}
	c.start(2)
	c.waitLeader(5 * time.Second)
	// If the corrupted node happens to lead, it halts itself mid-run and
	// proposals in flight to it have no definite answer; the majority carries on.
	acked := 0
	for i := uint64(0); i < 200; i++ {
		if c.propose(payload(i)).OK() {
			acked++
		}
	}
	if acked < 150 {
		t.Fatalf("only %d of 200 acknowledged", acked)
	}
	end, why := time.Now().Add(10*time.Second), ""
	for time.Now().Before(end) {
		if why = c.nodes[2].Status().Halted; why != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(why, "diverged") {
		t.Fatalf("divergence went undetected: %q", why)
	}
	if c.nodes[0].Status().Halted != "" || c.nodes[1].Status().Halted != "" {
		t.Fatal("a healthy node halted")
	}
}

func TestMatchingStatePassesDigestChecks(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.DigestEvery = 16 }, false)
	c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 200; i++ {
		c.mustPropose(payload(i))
	}
	c.waitConverged(10 * time.Second)
	end := time.Now().Add(5 * time.Second)
	for c.nodes[0].Status().DigestsChecked == 0 && time.Now().Before(end) {
		time.Sleep(20 * time.Millisecond)
	}
	if c.nodes[0].Status().DigestsChecked == 0 {
		t.Fatal("no digest was ever verified")
	}
	for i := 0; i < 3; i++ {
		if h := c.nodes[i].Status().Halted; h != "" {
			t.Fatalf("node %d halted: %s", i, h)
		}
	}
}

func TestHostileConnectionsDoNotDisturbTheCluster(t *testing.T) {
	c := newCluster(t, 3, nil, false)
	c.waitLeader(5 * time.Second)
	target := c.members[0].Addr
	junk := [][]byte{
		bytes.Repeat([]byte{0xff}, 64),
		{0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0, 1},
		{10, 0, 0, 0, 0, 0, 0, 0, 6, 9, 1, 2, 3, 4, 5, 6, 7, 8},
		{},
	}
	for _, j := range junk {
		if s, err := net.DialTimeout("tcp", target, 500*time.Millisecond); err == nil {
			s.Write(j)
			s.Close()
		}
	}
	for i := uint64(0); i < 100; i++ {
		c.mustPropose(payload(i))
	}
	if n := len(c.waitConverged(10 * time.Second)); n != 100 {
		t.Fatalf("%d entries", n)
	}
}

func TestFiveNodesSurviveTwoFailuresAndThreeCannotCommitWithTwoDown(t *testing.T) {
	t.Run("five", func(t *testing.T) {
		c := newCluster(t, 5, nil, false)
		c.waitLeader(5 * time.Second)
		for i := uint64(0); i < 100; i++ {
			c.mustPropose(payload(i))
		}
		l := c.leader()
		c.crash(l)
		other := 0
		for other == l {
			other++
		}
		c.crash(other)
		c.waitLeader(15 * time.Second)
		for i := uint64(100); i < 200; i++ {
			c.mustPropose(payload(i))
		}
		c.expect(c.waitConverged(15*time.Second), 0, 200, "after two failures")
	})
	t.Run("noquorum", func(t *testing.T) {
		c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.ProposeTimeout = 500 * time.Millisecond }, false)
		l := c.waitLeader(5 * time.Second)
		c.mustPropose(payload(0))
		for i := 0; i < 3; i++ {
			if i != l {
				c.crash(i)
			}
		}
		if r := c.nodes[l].Propose(payload(1)); r.Kind != ProposeTimeout {
			t.Fatalf("no quorum, no ack: %+v", r)
		}
		if !equalPayloads(c.shared[l].payloads(), seq(0, 1)) {
			t.Fatal("an unacknowledged entry was applied")
		}
	})
}

func TestASlowLeaderStateMachineDoesNotCostItLeadership(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) {
		cfg.MaxPending = 100000
		cfg.ProposeTimeout = 30 * time.Second
		cfg.Consensus.ElectionTicks = 40
		if cfg.ID == 0 {
			cfg.Consensus.ElectionTicks = 8 // node 0 wins the first election
		}
	}, false)
	if l := c.waitLeader(5 * time.Second); l != 0 {
		t.Fatalf("node 0 should lead, %d does", l)
	}
	// The slow machine is node 0's; each apply outlasts the followers' election timeout.
	c.shared[0].applyDelay.Store(700)
	term0 := c.nodes[0].Status().Term
	var chans []<-chan ProposeResult
	for i := uint64(0); i < 3; i++ {
		chans = append(chans, c.nodes[0].ProposeAsync(payload(i)))
	}
	for _, ch := range chans {
		if r := <-ch; !r.OK() {
			t.Fatalf("proposal: %+v", r)
		}
	}
	if s := c.nodes[0].Status(); s.Role != consensus.Leader || s.Term != term0 {
		t.Fatal("leadership was lost while the state machine was slow")
	}
	c.waitApplied([]int{1, 2}, 3, 5*time.Second)
	c.waitApplied([]int{0}, 3, 30*time.Second)
}

func TestAClusterWithOneSharedKeyWorksAndAWrongKeyCannotJoin(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.AuthKey = []byte("right") }, false)
	l := c.waitLeader(5 * time.Second)
	for i := uint64(0); i < 100; i++ {
		c.mustPropose(payload(i))
	}
	if n := len(c.waitConverged(10 * time.Second)); n != 100 {
		t.Fatalf("%d entries", n)
	}
	f := 0
	if l == 0 {
		f = 1
	}
	c.stop(f)
	cfg := c.config(f)
	cfg.AuthKey = []byte("wrong")
	rogue, err := StartReplica(cfg, &RecorderSm{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rogue.Crash()
	time.Sleep(1500 * time.Millisecond)
	if rogue.Status().Commit != 0 {
		t.Fatal("a node with the wrong key received log entries")
	}
	term := c.nodes[l].Status().Term
	for i := uint64(100); i < 140; i++ {
		c.mustPropose(payload(i))
	}
	if c.nodes[l].Status().Term != term {
		t.Fatal("an outsider disturbed the cluster")
	}
}

func cmdBatch(id uint64) []orderer.SymCmd {
	return []orderer.SymCmd{{Sym: 1, Cmd: matcher.NewLimit(id, matcher.Bid, 10000, 1, matcher.Gtc)}}
}

func TestTheClientProtocolSessionsAndLinearizableReads(t *testing.T) {
	c := newCluster(t, 3, nil, true)
	l := c.waitLeader(5 * time.Second)
	addrs := freeAddrs(t, 3)
	var pairs []Member
	for i := range addrs {
		s, err := NewClientServer(addrs[i], c.nodes[i], nil)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Shutdown()
		pairs = append(pairs, Member{consensus.NodeID(i), addrs[i]})
	}
	// Followers first: the client must find the leader by hint.
	var order []Member
	for i := range pairs {
		if i != l {
			order = append(order, pairs[i])
		}
	}
	order = append(order, pairs[l])
	client := NewClient(order).WithSession(7)
	defer client.Close()
	for i := uint64(1); i <= 20; i++ {
		if r := client.ProposeSession(cmdBatch(i)); !r.OK() {
			t.Fatalf("session batch %d: %+v", i, r)
		}
	}
	// The same session batch committed twice (a retry) takes effect once.
	dup := EncodeBatch(cmdBatch(999), nil, &SessionKey{Client: 8, Seq: 1})
	c.mustPropose(dup)
	c.mustPropose(dup)
	c.waitConverged(10 * time.Second)
	with999 := 0
	for _, p := range c.shared[0].payloads() {
		cmds, _ := DecodeBatch(p)
		for _, sc := range cmds {
			if sc.Cmd.OrderID == 999 {
				with999++
			}
		}
	}
	if with999 != 1 {
		t.Fatalf("a retried batch took effect %d times", with999)
	}
	// Status, hash and barrier over the wire; a follower cannot serve a barrier.
	conn := Connect(addrs[l], nil)
	if conn == nil {
		t.Fatal("cannot connect")
	}
	defer conn.Close()
	for i := 0; i < 3; i++ {
		if st, ok := conn.Status(); !ok || !strings.Contains(st, "role=Leader") {
			t.Fatalf("status: %q", st)
		}
	}
	if _, _, ok := conn.Hash(); !ok {
		t.Fatal("hash")
	}
	if _, ok := conn.Barrier(); !ok {
		t.Fatal("barrier")
	}
	f := 0
	if l == 0 {
		f = 1
	}
	fc := Connect(addrs[f], nil)
	if fc == nil {
		t.Fatal("cannot connect to follower")
	}
	defer fc.Close()
	if _, ok := fc.Barrier(); ok {
		t.Fatal("a follower served a barrier")
	}
	// A client without a session cannot use ProposeSession.
	if r := NewClient(pairs).ProposeSession(cmdBatch(1)); r.Kind != ClientProtocol {
		t.Fatalf("plain client: %+v", r)
	}
}

func TestADeposedLeaderCannotServeAStaleRead(t *testing.T) {
	c := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.ProposeTimeout = 600 * time.Millisecond }, false)
	old := c.waitLeader(5 * time.Second)
	c.mustPropose(payload(1))
	c.waitApplied([]int{0, 1, 2}, 1, 10*time.Second)
	c.nodes[old].Filter.Isolate(consensus.NodeID(old), ids3())
	end, newl := time.Now().Add(10*time.Second), -1
	for newl < 0 && time.Now().Before(end) {
		for i := 0; i < 3; i++ {
			if i != old && c.nodes[i].Barrier().OK() {
				newl = i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newl < 0 {
		t.Fatal("no new leader served a barrier")
	}
	if !c.nodes[newl].Propose(payload(2)).OK() || !c.nodes[newl].Barrier().OK() {
		t.Fatal("new leader cannot commit")
	}
	if c.shared[old].count() != 1 {
		t.Fatal("the isolated node applied entries it could not have received")
	}
	if c.nodes[old].Barrier().OK() {
		t.Fatal("a deposed leader served a barrier")
	}
}
