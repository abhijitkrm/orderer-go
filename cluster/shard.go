package cluster

import (
	orderer "github.com/abhijitkrm/orderer-go"
)

// Scale-out: several independent replicated groups, each owning a disjoint set
// of symbols (spec/REPLICATION.md §9, decision D1; port of orderer-rust's
// shard.rs).
//
// One replicated log is bounded by what its leader can replicate. Sharding
// multiplies that bound: every group has its own leader, log, followers and
// fsync stream, so groups share nothing and fail independently. What it costs
// is the single global ingress order: the commands of one symbol are totally
// ordered (they all go to one group, which is the matcher's whole contract),
// but there is no order between symbols in different groups.

// ShardMap says which group owns a symbol. The mix is deliberately different
// from the pipeline's own symbol-to-partition hash, so the symbols of one
// group still spread evenly over that group's partitions.
type ShardMap struct{ groups uint32 }

// NewShardMap maps symbols onto groups >= 1 groups.
func NewShardMap(groups uint32) ShardMap {
	if groups < 1 {
		panic("at least one group")
	}
	return ShardMap{groups}
}

// Groups is the number of groups.
func (m ShardMap) Groups() uint32 { return m.groups }

// GroupOf is the group that owns sym (splitmix64 finalizer, same as the Rust and C++ ports).
func (m ShardMap) GroupOf(sym uint32) uint32 {
	z := uint64(sym) + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	return uint32(z % uint64(m.groups))
}

// Split divides a batch into one sub-batch per group. Each symbol's commands
// keep their relative order.
func (m ShardMap) Split(cmds []orderer.SymCmd) [][]orderer.SymCmd {
	out := make([][]orderer.SymCmd, m.groups)
	for _, c := range cmds {
		g := m.GroupOf(c.Sym)
		out[g] = append(out[g], c)
	}
	return out
}

// ShardOutcome is one group's result for a split batch.
type ShardOutcome struct {
	Sent   bool // false: the batch had no commands for this group
	Result ClientResult
}

// ShardedClient is one leader-following Client per group.
type ShardedClient struct {
	m      ShardMap
	groups []*Client
}

// NewShardedClient wraps one client per group; the group count fixes the map.
func NewShardedClient(groups []*Client) *ShardedClient {
	return &ShardedClient{m: NewShardMap(uint32(len(groups))), groups: groups}
}

// Map is the symbol-to-group assignment.
func (c *ShardedClient) Map() ShardMap { return c.m }

// Propose submits a batch split by group and reports each group's outcome. A
// failure in one group does not undo another's commit: the caller sees exactly
// which sub-batches are committed.
func (c *ShardedClient) Propose(cmds []orderer.SymCmd) []ShardOutcome {
	parts := c.m.Split(cmds)
	out := make([]ShardOutcome, len(parts))
	for g, part := range parts {
		if len(part) == 0 {
			continue
		}
		out[g] = ShardOutcome{Sent: true, Result: c.groups[g].Propose(EncodeBatch(part, nil, nil))}
	}
	return out
}

// Close closes every group's connections.
func (c *ShardedClient) Close() {
	for _, g := range c.groups {
		g.Close()
	}
}
