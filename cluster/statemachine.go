package cluster

import (
	"crypto/sha256"
	"errors"
	"sort"

	"github.com/abhijitkrm/orderer-go/consensus"
)

// StateMachine is the deterministic state machine the replicated log drives.
// It is called only from the replica's applier goroutine, in log order.
type StateMachine interface {
	// Apply applies committed entry index. No-op entries arrive with empty data.
	Apply(index consensus.Index, e consensus.Entry)
	// Snapshot serialises the state after applying index.
	Snapshot(index consensus.Index) []byte
	// Restore replaces the state with a snapshot taken at index.
	Restore(index consensus.Index, data []byte) error
	// Digest is a digest of the state, for divergence detection; ok=false means unsupported.
	Digest(index consensus.Index) (Digest, bool)
}

type recorded struct {
	index consensus.Index
	data  []byte
}

// RecorderSm records every non-empty entry. Snapshot is the list
// `index u64 | len u32 | data`; the digest is SHA-256 of that list. Identical
// to orderer-rust's RecorderSm.
type RecorderSm struct{ applied []recorded }

func (r *RecorderSm) Apply(index consensus.Index, e consensus.Entry) {
	if len(e.Data) > 0 {
		r.applied = append(r.applied, recorded{index, e.Data})
	}
}

func (r *RecorderSm) serialize() []byte {
	var out []byte
	for _, a := range r.applied {
		out = putU64(out, uint64(a.index))
		out = putU32(out, uint32(len(a.data)))
		out = append(out, a.data...)
	}
	return out
}

func (r *RecorderSm) Snapshot(consensus.Index) []byte { return r.serialize() }

func (r *RecorderSm) Restore(_ consensus.Index, d []byte) error {
	var v []recorded
	for at := 0; at < len(d); {
		if len(d)-at < 12 {
			return errors.New("short recorder snapshot")
		}
		i, l := le.Uint64(d[at:]), int(le.Uint32(d[at+8:]))
		if at+12+l > len(d) {
			return errors.New("short recorder snapshot")
		}
		v = append(v, recorded{consensus.Index(i), append([]byte(nil), d[at+12:at+12+l]...)})
		at += 12 + l
	}
	r.applied = v
	return nil
}

func (r *RecorderSm) Digest(consensus.Index) (Digest, bool) {
	return sha256.Sum256(r.serialize()), true
}

// Applied is every recorded entry's index and payload.
func (r *RecorderSm) Applied() (indexes []consensus.Index, payloads [][]byte) {
	for _, a := range r.applied {
		indexes, payloads = append(indexes, a.index), append(payloads, a.data)
	}
	return
}

// DedupSm applies a session's batch only when seq is newer than the last
// applied for that client, so a retry that landed in the log twice takes
// effect once. The table is part of every snapshot and digest.
type DedupSm struct {
	inner      StateMachine
	last       map[uint64]uint64
	duplicates uint64
}

// NewDedupSm wraps inner.
func NewDedupSm(inner StateMachine) *DedupSm {
	return &DedupSm{inner: inner, last: map[uint64]uint64{}}
}

func (d *DedupSm) Apply(index consensus.Index, e consensus.Entry) {
	if s, ok := SessionOf(e.Data); ok {
		if s.Seq <= d.last[s.Client] {
			d.duplicates++
			return
		}
		d.last[s.Client] = s.Seq
	}
	d.inner.Apply(index, e)
}

func (d *DedupSm) tableBytes() []byte {
	clients := make([]uint64, 0, len(d.last))
	for c := range d.last {
		clients = append(clients, c)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	out := putU32(nil, uint32(len(clients)))
	for _, c := range clients {
		out = putU64(out, c)
		out = putU64(out, d.last[c])
	}
	return out
}

func (d *DedupSm) Snapshot(index consensus.Index) []byte {
	out := d.tableBytes()
	in := d.inner.Snapshot(index)
	out = putU64(out, uint64(len(in)))
	return append(out, in...)
}

func (d *DedupSm) Restore(index consensus.Index, b []byte) error {
	if len(b) < 4 {
		return errors.New("short dedup snapshot")
	}
	count := uint64(le.Uint32(b))
	tableEnd := 4 + count*16
	if uint64(len(b)) < tableEnd+8 {
		return errors.New("short dedup snapshot")
	}
	last := make(map[uint64]uint64, count)
	for i := uint64(0); i < count; i++ {
		last[le.Uint64(b[4+i*16:])] = le.Uint64(b[12+i*16:])
	}
	n := le.Uint64(b[tableEnd:])
	if n > uint64(len(b))-tableEnd-8 {
		return errors.New("short dedup snapshot")
	}
	if err := d.inner.Restore(index, b[tableEnd+8:tableEnd+8+n]); err != nil {
		return err
	}
	d.last = last
	return nil
}

func (d *DedupSm) Digest(index consensus.Index) (Digest, bool) {
	in, ok := d.inner.Digest(index)
	if !ok {
		return Digest{}, false
	}
	return sha256.Sum256(append(d.tableBytes(), in[:]...)), true
}

// Duplicates counts batches skipped as retries.
func (d *DedupSm) Duplicates() uint64 { return d.duplicates }
