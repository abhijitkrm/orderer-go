package orderer

import (
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// Ring slot types and pluggable per-partition event consumers (spec/PIPELINE.md §7).

// Control operations ride the rings so they cut every partition at the same
// point of the ingress order (spec/PIPELINE.md §6).
type Control uint8

const (
	CtlNone Control = iota
	CtlBarrier
	CtlSnapshot
	CtlShutdown
	CtlCheckpoint
)

// CmdMsg is the ingress / inbox slot.
type CmdMsg struct {
	Iseq, TPub, Arg uint64 // iseq: commands' ingress seq, controls' cut; TPub: ns since epoch (0 = untimed)
	Symbol          uint32
	Ctl             Control
	Cmd             matcher.Command
}

// EvtMsg is the outbox slot: one event (or a control passing through to egress).
type EvtMsg struct {
	Iseq, Seq, TPub, Arg uint64
	Symbol               uint32
	Ctl                  Control
	Ev                   matcher.Event
}

// EgressCtx is what a partition's egress instances know.
type EgressCtx struct {
	Partition, Partitions uint32
	Epoch                 time.Time
	durable               *atomic.Uint64
}

// DurableIseq is the highest iseq covered by a completed fsync (max uint64 without journals).
func (c *EgressCtx) DurableIseq() uint64 { return c.durable.Load() }

// NowNs is ns since the pipeline epoch.
func (c *EgressCtx) NowNs() uint64 { return uint64(time.Since(c.Epoch)) }

// Egress is one partition's consumer of events.
type Egress interface {
	OnEvent(m *EvtMsg) // every event, partition order; m is the ring slot: copy to keep
	OnBatchEnd()       // after a ring batch / before a drain completes
	OnIdle()           // while idle: release gated work
	OnShutdown() error // once, after every event
}

// Checkpointer is an optional Egress hook: a checkpoint with cut `cut`
// passed the partition (1.2). The event journal starts a new segment there.
type Checkpointer interface {
	OnCheckpoint(cut uint64)
}

// EgressBase supplies no-op hooks for embedding.
type EgressBase struct{}

func (EgressBase) OnBatchEnd()       {}
func (EgressBase) OnIdle()           {}
func (EgressBase) OnShutdown() error { return nil }

// EgressFactory creates one Egress per partition.
type EgressFactory func(ctx *EgressCtx) Egress

// ---- Collect: canonical lines per partition (harnesses, tests) -------------------------

// CollectHandle holds what a Collect gathered, per partition.
type CollectHandle struct {
	mu   sync.Mutex
	bufs [][]byte
}

// Take returns and clears each partition's lines.
func (h *CollectHandle) Take() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.bufs))
	for i, b := range h.bufs {
		out[i] = string(b)
		h.bufs[i] = h.bufs[i][:0]
	}
	return out
}

// Listing is the spec/HARNESS.md §3 listing: partition 0's lines, then 1's, …
func (h *CollectHandle) Listing() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var sb strings.Builder
	for _, b := range h.bufs {
		sb.Write(b)
	}
	return sb.String()
}

type collectEgress struct {
	EgressBase
	p      uint32
	tagged bool
	local  []byte
	h      *CollectHandle
}

func (e *collectEgress) OnEvent(m *EvtMsg) {
	if e.tagged {
		m.Ev.WriteCanonicalSym(m.Seq, m.Symbol, &e.local)
	} else {
		m.Ev.WriteCanonical(m.Seq, &e.local)
	}
	e.local = append(e.local, '\n')
}

func (e *collectEgress) OnBatchEnd() {
	if len(e.local) == 0 {
		return
	}
	e.h.mu.Lock()
	e.h.bufs[e.p] = append(e.h.bufs[e.p], e.local...)
	e.h.mu.Unlock()
	e.local = e.local[:0]
}

func (e *collectEgress) OnShutdown() error { e.OnBatchEnd(); return nil }

// Collect gathers canonical lines per partition, symbol-tagged or not.
func Collect(tagged bool) (EgressFactory, *CollectHandle) {
	h := &CollectHandle{}
	return func(ctx *EgressCtx) Egress {
		h.mu.Lock()
		for uint32(len(h.bufs)) < ctx.Partitions {
			h.bufs = append(h.bufs, nil)
		}
		h.mu.Unlock()
		return &collectEgress{p: ctx.Partition, tagged: tagged, local: make([]byte, 0, 1<<16), h: h}
	}, h
}

// ---- Callback ----------------------------------------------------------------------------

type callbackEgress struct {
	EgressBase
	p uint32
	f func(uint32, *EvtMsg)
}

func (e *callbackEgress) OnEvent(m *EvtMsg) { e.f(e.p, m) }

// Callback calls f(partition, msg) for every event (msg is the ring slot: copy to keep).
func Callback(f func(partition uint32, m *EvtMsg)) EgressFactory {
	return func(ctx *EgressCtx) Egress { return &callbackEgress{p: ctx.Partition, f: f} }
}

// ---- Acks: durability-gated delivery ------------------------------------------------------

type ackEgress struct {
	ctx     *EgressCtx
	f       func(uint32, *EvtMsg)
	pending []EvtMsg
	head    int
}

func (e *ackEgress) release() {
	if e.head == len(e.pending) {
		return
	}
	d := e.ctx.DurableIseq()
	for e.head < len(e.pending) && e.pending[e.head].Iseq <= d {
		e.f(e.ctx.Partition, &e.pending[e.head])
		e.head++
	}
	if e.head == len(e.pending) {
		e.pending, e.head = e.pending[:0], 0
	}
}

func (e *ackEgress) OnEvent(m *EvtMsg) { e.pending = append(e.pending, *m) }
func (e *ackEgress) OnBatchEnd()       { e.release() }
func (e *ackEgress) OnIdle()           { e.release() }
func (e *ackEgress) OnShutdown() error { e.release(); return nil }

// Acks calls f(partition, msg) only once the causing command is durable (spec/PIPELINE.md §5).
func Acks(f func(partition uint32, m *EvtMsg)) EgressFactory {
	return func(ctx *EgressCtx) Egress { return &ackEgress{ctx: ctx, f: f} }
}

// ---- Metrics: counts + end-to-end latency ---------------------------------------------------

// PartitionMetrics is one partition's counts and latency samples.
type PartitionMetrics struct {
	Partition                uint32
	Events, Commands, Trades uint64
	Latencies                []uint64 // ns, arrival order
}

// MetricsHandle collects every partition's metrics at shutdown.
type MetricsHandle struct {
	mu  sync.Mutex
	out []PartitionMetrics
}

func (h *MetricsHandle) Results() []PartitionMetrics {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]PartitionMetrics(nil), h.out...)
}

type metricsEgress struct {
	EgressBase
	ctx      *EgressCtx
	m        PartitionMetrics
	lastIseq uint64
	h        *MetricsHandle
}

func (e *metricsEgress) OnEvent(m *EvtMsg) {
	e.m.Events++
	if m.Ev.Kind == matcher.EvTrade {
		e.m.Trades++
	}
	if m.Iseq != e.lastIseq {
		e.lastIseq = m.Iseq
		e.m.Commands++
		if m.TPub != 0 && len(e.m.Latencies) < cap(e.m.Latencies) {
			now := e.ctx.NowNs()
			lat := uint64(0)
			if now > m.TPub {
				lat = now - m.TPub
			}
			e.m.Latencies = append(e.m.Latencies, lat)
		}
	}
}

func (e *metricsEgress) OnShutdown() error {
	e.h.mu.Lock()
	e.h.out = append(e.h.out, e.m)
	e.h.mu.Unlock()
	return nil
}

// Metrics takes one latency sample per command, at its first event: now - TPub
// (spec/BENCH.md §2.2 step 5). Samples are preallocated per partition.
func Metrics(sampleCapacity int) (EgressFactory, *MetricsHandle) {
	h := &MetricsHandle{}
	return func(ctx *EgressCtx) Egress {
		return &metricsEgress{ctx: ctx, m: PartitionMetrics{Partition: ctx.Partition,
			Latencies: make([]uint64, 0, sampleCapacity)}, h: h}
	}, h
}

// ---- EventDigest: a rolling hash of each partition's event stream -----------------------

// ChainStep is one step of the event-chain hash: rotl((h ^ x) * K, 31). Pinned
// exactly (spec/REPLICATION.md 13.16): replicas of different languages compare
// these values.
func ChainStep(h, x uint64) uint64 {
	return bits.RotateLeft64((h^x)*0x9E3779B97F4A7C15, 31)
}

// ChainEvent folds one event into a chain: the causing command's iseq, the
// symbol and event kind, the per-book sequence, then the kind's fields.
// Reasons are coded explicitly and signed prices as two's-complement uint64.
func ChainEvent(h uint64, m *EvtMsg) uint64 {
	e := &m.Ev
	var kind uint64
	switch e.Kind {
	case matcher.EvAccepted:
		kind = 1
	case matcher.EvRejected:
		kind = 2
	case matcher.EvTrade:
		kind = 3
	case matcher.EvClosed:
		kind = 4
	default:
		kind = 5
	}
	h = ChainStep(h, m.Iseq)
	h = ChainStep(h, uint64(m.Symbol)<<8|kind)
	h = ChainStep(h, m.Seq)
	switch e.Kind {
	case matcher.EvAccepted:
		h = ChainStep(h, e.OrderID)
		return ChainStep(h, e.LeavesQty)
	case matcher.EvRejected:
		var code uint64
		switch matcher.RejectReason(e.Reason) {
		case matcher.InvalidQty:
			code = 0
		case matcher.InvalidPrice:
			code = 1
		case matcher.DuplicateOrderID:
			code = 2
		case matcher.UnknownOrderID:
			code = 3
		case matcher.PostOnlyWouldCross:
			code = 4
		case matcher.FokCannotFill:
			code = 5
		default:
			code = 6
		}
		h = ChainStep(h, e.OrderID)
		return ChainStep(h, code)
	case matcher.EvTrade:
		h = ChainStep(h, e.Maker)
		h = ChainStep(h, e.Taker)
		h = ChainStep(h, uint64(e.Price))
		return ChainStep(h, e.Qty)
	case matcher.EvClosed:
		var code uint64
		switch matcher.CloseReason(e.Reason) {
		case matcher.Filled:
			code = 0
		case matcher.Cancelled:
			code = 1
		default:
			code = 2
		}
		h = ChainStep(h, e.OrderID)
		return ChainStep(h, code)
	default:
		h = ChainStep(h, e.OrderID)
		h = ChainStep(h, uint64(e.Price))
		return ChainStep(h, e.Qty)
	}
}

// EventDigest keeps one rolling hash per partition over the events it emits.
// Replicas fed the same commands emit the same events, so their chains agree and
// a divergence in behaviour makes them differ. Reading is O(partitions): no
// snapshot of the books. It does not see state that has not yet influenced an
// event. It is an EgressFactory; register it with Builder.Egress(d.Factory()).
type EventDigest struct {
	slots []paddedU64
}

// NewEventDigest starts each partition's chain at the given value (zeros for a
// fresh pipeline, the snapshot's values after a restore).
func NewEventDigest(partitions uint32, initial []uint64) *EventDigest {
	d := &EventDigest{slots: make([]paddedU64, partitions)}
	for i := range d.slots {
		if i < len(initial) {
			d.slots[i].v.Store(initial[i])
		}
	}
	return d
}

// Values is every partition's chain, as of the last pipeline drain.
func (d *EventDigest) Values() []uint64 {
	out := make([]uint64, len(d.slots))
	for i := range d.slots {
		out[i] = d.slots[i].v.Load()
	}
	return out
}

// Factory is the egress factory to register with the builder.
func (d *EventDigest) Factory() EgressFactory {
	return func(ctx *EgressCtx) Egress {
		slot := &d.slots[ctx.Partition].v
		return &digestEgress{h: slot.Load(), slot: slot}
	}
}

type digestEgress struct {
	h    uint64
	slot *atomic.Uint64
}

func (e *digestEgress) OnEvent(m *EvtMsg) { e.h = ChainEvent(e.h, m) }

// Published at batch ends and before a drain completes, not per event.
func (e *digestEgress) OnBatchEnd()       { e.slot.Store(e.h) }
func (e *digestEgress) OnIdle()           {}
func (e *digestEgress) OnShutdown() error { e.slot.Store(e.h); return nil }
