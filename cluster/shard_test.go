package cluster

import (
	"fmt"
	"testing"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

func TestGroupAssignmentMatchesTheOtherLanguages(t *testing.T) {
	// Computed independently (Python) from the splitmix64 definition; the Rust
	// and C++ maps use the same function.
	syms := []uint32{0, 1, 2, 3, 7, 100, 4095, 65535, 4294967295}
	for groups, want := range map[uint32][]uint32{
		4: {3, 1, 2, 1, 3, 0, 3, 2, 0},
		3: {1, 2, 1, 0, 0, 2, 2, 1, 2},
	} {
		m := NewShardMap(groups)
		for i, s := range syms {
			if got := m.GroupOf(s); got != want[i] {
				t.Errorf("groups=%d sym=%d: %d, want %d", groups, s, got, want[i])
			}
		}
	}
}

func TestEverySymbolHasOneStableGroupAndGroupsAreBalanced(t *testing.T) {
	m := NewShardMap(4)
	var counts [4]int
	for s := uint32(0); s < 40000; s++ {
		g := m.GroupOf(s)
		if g != m.GroupOf(s) {
			t.Fatal("unstable")
		}
		counts[g]++
	}
	for _, c := range counts {
		if c < 9000 || c > 11000 {
			t.Fatalf("unbalanced: %v", counts)
		}
	}
}

func TestAGroupsSymbolsStillSpreadOverItsPartitions(t *testing.T) {
	// If the group hash correlated with the pipeline's partition hash, a
	// group's symbols would pile onto one partition.
	m := NewShardMap(2)
	var grid [2][2]int
	for s := uint32(0); s < 20000; s++ {
		grid[m.GroupOf(s)][orderer.HashPartition(s, 2)]++
	}
	for _, row := range grid {
		for _, c := range row {
			if c < 4000 || c > 6000 {
				t.Fatalf("skewed: %v", grid)
			}
		}
	}
}

func TestSplitKeepsEachSymbolsOrderAndLosesNothing(t *testing.T) {
	m := NewShardMap(3)
	var cmds []orderer.SymCmd
	for i := uint64(0); i < 300; i++ {
		cmds = append(cmds, orderer.SymCmd{Sym: uint32(i % 17), Cmd: matcher.NewLimit(i+1, matcher.Bid, 100, 1, matcher.Gtc)})
	}
	total := 0
	for g, part := range m.Split(cmds) {
		total += len(part)
		var prev uint64
		for _, c := range part {
			if m.GroupOf(c.Sym) != uint32(g) {
				t.Fatal("command in the wrong group")
			}
			if c.Cmd.OrderID <= prev {
				t.Fatal("order within a group changed")
			}
			prev = c.Cmd.OrderID
		}
	}
	if total != len(cmds) {
		t.Fatalf("split lost commands: %d of %d", total, len(cmds))
	}
}

// shardGroup is one replicated group with client ports.
type shardGroup struct {
	*testCluster
	clients []Member
	servers []*ClientServer
}

func (g *shardGroup) serve(i int) {
	s, err := NewClientServer(g.clients[i].Addr, g.nodes[i], nil)
	if err != nil {
		g.t.Fatal(err)
	}
	g.servers[i] = s
}

// restartNode brings node i back with a client port bound to the new replica.
func (g *shardGroup) restartNode(i int) {
	g.servers[i].Shutdown()
	g.restart(i)
	g.serve(i)
}

func newShardGroup(t *testing.T, cluster uint64) *shardGroup {
	tc := newCluster(t, 3, func(cfg *ReplicaConfig) { cfg.ClusterID = cluster }, true)
	g := &shardGroup{testCluster: tc}
	g.servers = make([]*ClientServer, 3)
	for i, a := range freeAddrs(t, 3) {
		g.clients = append(g.clients, Member{consensus.NodeID(i), a})
		g.serve(i)
	}
	t.Cleanup(func() {
		for _, s := range g.servers {
			s.Shutdown()
		}
	})
	return g
}

func (g *shardGroup) client(session uint64) *Client {
	c := NewClient(g.clients).WithSession(session)
	c.RetryBudget = 3 * time.Second
	return c
}

// applied is every command node 0 applied, in order.
func (g *shardGroup) applied(node int) []orderer.SymCmd {
	var out []orderer.SymCmd
	for _, p := range g.shared[node].payloads() {
		cmds, _ := DecodeBatch(p)
		out = append(out, cmds...)
	}
	return out
}

func symbolBatch(next *uint64, syms []uint32, n int) []orderer.SymCmd {
	var out []orderer.SymCmd
	for i := 0; i < n; i++ {
		*next++
		out = append(out, orderer.SymCmd{Sym: syms[i%len(syms)], Cmd: matcher.NewLimit(*next, matcher.Bid, 10000, 1, matcher.Gtc)})
	}
	return out
}

func TestGroupsHoldOnlyTheirSymbolsInPerSymbolOrder(t *testing.T) {
	g := []*shardGroup{newShardGroup(t, 11), newShardGroup(t, 12)}
	for _, x := range g {
		x.waitLeader(5 * time.Second)
	}
	sc := NewShardedClient([]*Client{g[0].client(1), g[1].client(1)})
	defer sc.Close()
	syms := []uint32{}
	for s := uint32(0); s < 12; s++ {
		syms = append(syms, s)
	}
	var next uint64
	sent := map[uint32][]uint64{}
	for i := 0; i < 40; i++ {
		b := symbolBatch(&next, syms, 25)
		for _, c := range b {
			sent[c.Sym] = append(sent[c.Sym], c.Cmd.OrderID)
		}
		for gi, o := range sc.Propose(b) {
			if o.Sent && !o.Result.OK() {
				t.Fatalf("group %d: %+v", gi, o.Result)
			}
		}
	}
	for gi, x := range g {
		x.waitConverged(10 * time.Second)
		got := map[uint32][]uint64{}
		for _, c := range x.applied(0) {
			if sc.Map().GroupOf(c.Sym) != uint32(gi) {
				t.Fatalf("group %d holds symbol %d of another group", gi, c.Sym)
			}
			got[c.Sym] = append(got[c.Sym], c.Cmd.OrderID)
		}
		for sym, ids := range got {
			if fmt.Sprint(ids) != fmt.Sprint(sent[sym]) {
				t.Fatalf("group %d symbol %d: order or content differs", gi, sym)
			}
		}
	}
}

func TestAnOutageInOneGroupDoesNotStopTheOther(t *testing.T) {
	g := []*shardGroup{newShardGroup(t, 21), newShardGroup(t, 22)}
	for _, x := range g {
		x.waitLeader(5 * time.Second)
	}
	m := NewShardMap(2)
	var mine [2][]uint32
	for s := uint32(0); len(mine[0]) < 4 || len(mine[1]) < 4; s++ {
		gi := m.GroupOf(s)
		mine[gi] = append(mine[gi], s)
	}
	// Take group 1 down entirely.
	for i := 0; i < 3; i++ {
		g[1].crash(i)
	}
	c0 := g[0].client(1)
	defer c0.Close()
	var next uint64
	for i := 0; i < 50; i++ {
		if r := c0.Propose(EncodeBatch(symbolBatch(&next, mine[0][:4], 10), nil, nil)); !r.OK() {
			t.Fatalf("healthy group failed while its sibling was down: %+v", r)
		}
	}
	// Group 1 comes back, and its data is intact and usable.
	for i := 0; i < 3; i++ {
		g[1].restartNode(i)
	}
	g[1].waitLeader(10 * time.Second)
	if r := g[1].client(2).Propose(EncodeBatch(symbolBatch(&next, mine[1][:4], 10), nil, nil)); !r.OK() {
		t.Fatalf("restarted group: %+v", r)
	}
}
