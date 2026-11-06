package cluster

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// The matching pipeline as the replicated state machine: replicas must hold
// byte-identical state, including after a snapshot of real book state is
// installed.

type pipeCluster struct {
	*testCluster
	sms []*PipelineSm
}

func newPipeCluster(t *testing.T, n int, tune func(*ReplicaConfig)) *pipeCluster {
	c := &testCluster{t: t, dir: t.TempDir(), tune: tune}
	pc := &pipeCluster{testCluster: c, sms: make([]*PipelineSm, n)}
	for i, a := range freeAddrs(t, n) {
		c.members = append(c.members, Member{consensus.NodeID(i), a})
		c.nodes = append(c.nodes, nil)
		c.shared = append(c.shared, &shared{})
	}
	for i := range c.members {
		pc.start(i)
	}
	t.Cleanup(func() {
		for i, r := range c.nodes {
			if r != nil {
				r.Crash()
			}
			if pc.sms[i] != nil {
				pc.sms[i].Close()
			}
		}
	})
	return pc
}

func (pc *pipeCluster) start(i int) {
	sm, err := NewCountingPipelineSm(2, matcher.DefaultConfig())
	if err != nil {
		pc.t.Fatal(err)
	}
	pc.sms[i] = sm
	r, err := StartReplica(pc.config(i), NewDedupSm(sm), nil)
	if err != nil {
		pc.t.Fatalf("start %d: %v", i, err)
	}
	pc.nodes[i] = r
}

func (pc *pipeCluster) stop(i int) {
	pc.nodes[i].Stop()
	pc.nodes[i] = nil
	pc.sms[i].Close()
	pc.sms[i] = nil
}

// randomBatch is n commands over a few symbols: limits on both sides that cross, some cancels.
func randomBatch(rng *rand.Rand, next *uint64, n int) []orderer.SymCmd {
	var cmds []orderer.SymCmd
	for i := 0; i < n; i++ {
		*next++
		sym := uint32(1 + rng.Intn(8))
		if rng.Intn(10) == 0 && *next > 20 {
			cmds = append(cmds, orderer.SymCmd{Sym: sym, Cmd: matcher.Cancel(*next - uint64(1+rng.Intn(15)))})
			continue
		}
		side := matcher.Bid
		if rng.Intn(2) == 0 {
			side = matcher.Ask
		}
		cmds = append(cmds, orderer.SymCmd{Sym: sym, Cmd: matcher.NewLimit(*next, side, 10000+int64(rng.Intn(9))-4, 1+uint64(rng.Intn(9)), matcher.Gtc)})
	}
	return cmds
}

// hashes waits for every running replica to apply `want` and returns their digests.
func (pc *pipeCluster) hashes(want consensus.Index, within time.Duration) []Digest {
	pc.t.Helper()
	end := time.Now().Add(within)
	for {
		var out []Digest
		ok := true
		for _, r := range pc.nodes {
			if r == nil {
				continue
			}
			idx, d, have := r.StateHash()
			if !have || idx < want {
				ok = false
				break
			}
			if idx != want {
				pc.t.Fatalf("applied %d past the expected %d", idx, want)
			}
			out = append(out, d)
		}
		if ok {
			return out
		}
		if time.Now().After(end) {
			pc.t.Fatal("replicas did not reach the expected index")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPipelineReplicasHoldIdenticalState(t *testing.T) {
	pc := newPipeCluster(t, 3, func(cfg *ReplicaConfig) { cfg.DigestEvery = 32 })
	pc.waitLeader(5 * time.Second)
	rng, next := rand.New(rand.NewSource(1)), uint64(0)
	var last ProposeResult
	for i := 0; i < 200; i++ {
		last = pc.propose(EncodeBatch(randomBatch(rng, &next, 20), nil, nil))
		if !last.OK() {
			t.Fatalf("batch %d: %+v", i, last)
		}
	}
	h := pc.hashes(last.Index, 10*time.Second)
	for i := range h {
		if h[i] != h[0] {
			t.Fatalf("replica %d state differs", i)
		}
	}
	end := time.Now().Add(5 * time.Second)
	for pc.nodes[0].Status().DigestsChecked == 0 && time.Now().Before(end) {
		time.Sleep(20 * time.Millisecond)
	}
	for i, r := range pc.nodes {
		if s := r.Status(); s.Halted != "" || (i == 0 && s.DigestsChecked == 0) {
			t.Fatalf("node %d: halted=%q verified=%d", i, s.Halted, s.DigestsChecked)
		}
	}
}

func TestALaggingPipelineReplicaCatchesUpFromASnapshotOfRealBookState(t *testing.T) {
	pc := newPipeCluster(t, 3, func(cfg *ReplicaConfig) { cfg.CompactEvery = 40 })
	l := pc.waitLeader(5 * time.Second)
	lag := 0
	if l == 0 {
		lag = 1
	}
	rng, next := rand.New(rand.NewSource(2)), uint64(0)
	var last ProposeResult
	send := func(n int) {
		for i := 0; i < n; i++ {
			if last = pc.propose(EncodeBatch(randomBatch(rng, &next, 15), nil, nil)); !last.OK() {
				t.Fatalf("batch: %+v", last)
			}
		}
	}
	send(10)
	pc.stop(lag)
	send(300)
	end := time.Now().Add(10 * time.Second)
	for pc.nodes[l].Status().Base == 0 && time.Now().Before(end) {
		time.Sleep(20 * time.Millisecond)
	}
	if pc.nodes[l].Status().Base == 0 {
		t.Fatal("leader never compacted")
	}
	pc.start(lag)
	// Another batch so the leader is still working when the follower rejoins.
	send(5)
	h := pc.hashes(last.Index, 20*time.Second)
	for i := range h {
		if h[i] != h[0] {
			t.Fatal(fmt.Sprintf("replica %d state differs after snapshot install", i))
		}
	}
	if pc.nodes[lag].Status().Base == 0 {
		t.Fatal("caught up without a snapshot install")
	}
}

func TestPipelineStateSurvivesAWholeClusterRestartFromDisk(t *testing.T) {
	pc := newPipeCluster(t, 3, func(cfg *ReplicaConfig) { cfg.CompactEvery = 30; cfg.NoFsync = false })
	pc.waitLeader(5 * time.Second)
	rng, next := rand.New(rand.NewSource(3)), uint64(0)
	var last ProposeResult
	for i := 0; i < 150; i++ {
		if last = pc.propose(EncodeBatch(randomBatch(rng, &next, 10), nil, nil)); !last.OK() {
			t.Fatalf("batch: %+v", last)
		}
	}
	before := pc.hashes(last.Index, 10*time.Second)
	for i := 0; i < 3; i++ {
		pc.stop(i)
	}
	for i := 0; i < 3; i++ {
		pc.start(i)
	}
	pc.waitLeader(10 * time.Second)
	// The leader's no-op advances the log by one; compare after it applies.
	after := pc.hashes(last.Index+1, 10*time.Second)
	for i := range after {
		if after[i] != before[0] {
			t.Fatalf("replica %d recovered different state", i)
		}
	}
}
