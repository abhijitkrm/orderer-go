package orderer

import (
	"fmt"
	"strings"
)

// MaxPartitions bounds a pipeline's partition count (spec/ROUTING.md).
const MaxPartitions = 1024

// HashPartition is spec/ROUTING.md §2.
func HashPartition(sym uint32, partitions uint32) uint32 {
	h := uint64(sym) * 0x9E3779B97F4A7C15
	return uint32(((h >> 32) * uint64(partitions)) >> 32)
}

// PartitionMap is a fixed map: table overrides, hash for the rest.
type PartitionMap struct {
	p      uint32
	dense  []uint32
	sparse map[uint32]uint32
}

const denseSyms = 4096

// NewPartitionMap builds a map for partitions with optional table overrides.
func NewPartitionMap(partitions uint32, table [][2]uint32) (*PartitionMap, error) {
	if partitions < 1 || partitions > MaxPartitions {
		return nil, fmt.Errorf("partitions must be 1..=%d, got %d", MaxPartitions, partitions)
	}
	m := &PartitionMap{p: partitions, dense: make([]uint32, denseSyms), sparse: map[uint32]uint32{}}
	for s := range m.dense {
		m.dense[s] = HashPartition(uint32(s), partitions)
	}
	for _, e := range table {
		sym, part := e[0], e[1]
		if part >= partitions {
			return nil, fmt.Errorf("symbol %d: partition %d out of range for %d partitions", sym, part, partitions)
		}
		if _, dup := m.sparse[sym]; dup {
			return nil, fmt.Errorf("symbol %d listed twice", sym)
		}
		if sym < denseSyms {
			m.dense[sym] = part
		}
		m.sparse[sym] = part
	}
	return m, nil
}

// ParsePartitionTable parses an orderer-partition-map/1 file for a pipeline of partitions.
func ParsePartitionTable(text string, partitions uint32) (*PartitionMap, error) {
	var table [][2]uint32
	first := true
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if first {
			first = false
			if f, _ := Get(line, "format"); f != "orderer-partition-map/1" {
				return nil, fmt.Errorf("not an orderer-partition-map/1 header")
			}
			pp, ok := U64(line, "partitions")
			if !ok {
				return nil, fmt.Errorf("partition map header lacks partitions")
			}
			if pp != uint64(partitions) {
				return nil, fmt.Errorf("partition map is for %d partitions, pipeline has %d", pp, partitions)
			}
			continue
		}
		s, ok := U64(line, "symbol")
		if !ok || s > 0xFFFFFFFF {
			return nil, fmt.Errorf("bad symbol in: %s", line)
		}
		q, ok := U64(line, "partition")
		if !ok || q > 0xFFFFFFFF {
			return nil, fmt.Errorf("bad partition in: %s", line)
		}
		table = append(table, [2]uint32{uint32(s), uint32(q)})
	}
	if first {
		return nil, fmt.Errorf("empty partition map")
	}
	return NewPartitionMap(partitions, table)
}

func (m *PartitionMap) Partitions() uint32 { return m.p }

func (m *PartitionMap) Partition(sym uint32) uint32 {
	if sym < denseSyms {
		return m.dense[sym]
	}
	if len(m.sparse) > 0 {
		if p, ok := m.sparse[sym]; ok {
			return p
		}
	}
	return HashPartition(sym, m.p)
}
