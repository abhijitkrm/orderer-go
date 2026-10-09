package orderer

// Rings, goroutines and control (spec/PIPELINE.md).
//
//	Handle.Publish ─▶ ingress (multi-producer) ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress
//	                                                      └─▶ …            (journal + apply)
//
// router (1): sole ingress consumer; stamps iseq, routes, broadcasts controls,
// commits every inbox once per batch.
// engine[p]: encodes each command's journal record into its partition's
// ChunkWriter, then applies it (journal-before-apply, in-goroutine), staging
// events into outbox[p].
// egress (grouped): runs the plugs, marks drain epochs.
// I/O goroutines (one per journal file, inside ChunkWriter): write + fsync.
// Router, engine and egress goroutines lock their OS threads.

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abhijitkrm/orderer-go/disruptor"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// ErrorKind classifies a pipeline error.
type ErrorKind uint8

const (
	ErrClosed ErrorKind = iota
	ErrFull
	ErrFailed
	ErrConfig
	ErrIO
)

// Error is a pipeline error.
type Error struct {
	Kind ErrorKind
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// Status is a publish outcome: Ok means sequenced and will be applied — not durable.
type Status uint8

const (
	Ok Status = iota
	Closed
	Full
)

// Waits is the wait strategy per stage (not observable).
type Waits struct{ Router, Engine, Egress disruptor.WaitStrategy }

func RelaxedWaits() Waits {
	b := disruptor.BackoffWait()
	return Waits{b, b, b}
}

// LowLatencyWaits busy-spins router and engines; egress backs off. The bench configuration.
func LowLatencyWaits() Waits {
	return Waits{disruptor.BusySpinWait(), disruptor.BusySpinWait(), disruptor.BackoffWait()}
}

// Initial is the starting state after recovery.
type Initial struct {
	Cores    []MatchingCore
	NextIseq uint64
}

// Snapshot is a merged matcher-snap/1 snapshot plus its cut (spec/JOURNAL.md §4).
type Snapshot struct {
	Body       string
	Iseq       uint64
	Partitions uint32
}

func (s *Snapshot) Meta() string {
	return fmt.Sprintf("{\"format\":\"orderer-meta/1\",\"iseq\":%d,\"partitions\":%d}\n", s.Iseq, s.Partitions)
}

// MetaPath is the sidecar path for a snapshot at p.
func MetaPath(p string) string { return p + ".meta" }

// Write writes the body to path and the cut to path.meta.
func (s *Snapshot) Write(path string) error {
	if err := os.WriteFile(path, []byte(s.Body), 0o644); err != nil {
		return err
	}
	return os.WriteFile(MetaPath(path), []byte(s.Meta()), 0o644)
}

// ---- shared state ---------------------------------------------------------------------------

type inFlight struct {
	_ [64]byte
	v atomic.Bool
	_ [63]byte
}

type snapState struct {
	blocks    []Block
	remaining uint32
	cut       uint64
}

type paddedU64 struct {
	_ [64]byte
	v atomic.Uint64
	_ [56]byte
}

type sharedState struct {
	partitions         uint32
	book               matcher.BookConfig
	epoch              time.Time
	timestamps, closed atomic.Bool
	failed             atomic.Bool
	failMu             sync.Mutex
	failure            string
	handlesMu          sync.Mutex
	handles            map[*inFlight]struct{}
	nextEpoch, nextOp  atomic.Uint64
	egressEpoch        []paddedU64
	snapsMu            sync.Mutex
	snapsCond          *sync.Cond
	snaps              map[uint64]*snapState
	flushed, durable   []*atomic.Uint64
	alertsMu           sync.Mutex
	alerts             []func()
	journal            *JournalConfig
	counters           []*engineCounters
	io                 []*IoStats
	ingressCtl         disruptor.RingControl[CmdMsg]
	inboxCtl           []disruptor.RingControl[CmdMsg]
	outboxCtl          []disruptor.RingControl[EvtMsg]
}

func (sh *sharedState) addAlert(a func()) {
	sh.alertsMu.Lock()
	sh.alerts = append(sh.alerts, a)
	sh.alertsMu.Unlock()
}

func (sh *sharedState) alertAll() {
	sh.alertsMu.Lock()
	for _, a := range sh.alerts {
		a()
	}
	sh.alertsMu.Unlock()
}

func (sh *sharedState) fail(m string) {
	sh.failMu.Lock()
	if sh.failure == "" {
		sh.failure = m
	}
	sh.failMu.Unlock()
	sh.failed.Store(true)
	sh.alertAll()
	sh.snapsMu.Lock()
	sh.snapsCond.Broadcast()
	sh.snapsMu.Unlock()
}

func (sh *sharedState) check() error {
	if sh.failed.Load() {
		sh.failMu.Lock()
		defer sh.failMu.Unlock()
		return &Error{ErrFailed, "pipeline failed: " + sh.failure}
	}
	return nil
}

func (sh *sharedState) nowNs() uint64 {
	if !sh.timestamps.Load() {
		return 0
	}
	if ns := uint64(time.Since(sh.epoch)); ns > 0 {
		return ns
	}
	return 1
}

// waitUntil spins → yields → short sleeps until done, failing fast.
func waitUntil(sh *sharedState, done func() bool) error {
	for step := 0; !done(); step++ {
		if err := sh.check(); err != nil {
			return err
		}
		switch {
		case step < 64:
		case step < 256:
			runtime.Gosched()
		default:
			time.Sleep(50 * time.Microsecond)
		}
	}
	return nil
}

// ---- Handle ---------------------------------------------------------------------------------

// Handle publishes commands. Use one per goroutine (Pipeline.Handle makes
// more). Each registers an in-flight flag so shutdown can wait out publishes
// that began before it closed the pipeline: a publish that returns Ok is
// always applied. Close unregisters it.
type Handle struct {
	ingress disruptor.MultiProducer[CmdMsg]
	sh      *sharedState
	flag    *inFlight
}

func newHandle(ingress disruptor.MultiProducer[CmdMsg], sh *sharedState) *Handle {
	h := &Handle{ingress: ingress, sh: sh, flag: &inFlight{}}
	sh.handlesMu.Lock()
	sh.handles[h.flag] = struct{}{}
	sh.handlesMu.Unlock()
	return h
}

func (h *Handle) enter() bool {
	h.flag.v.Store(true) // sequentially consistent: the store is ordered before the load
	if h.sh.closed.Load() {
		h.flag.v.Store(false)
		return false
	}
	return true
}

func (h *Handle) exit() { h.flag.v.Store(false) }

// Publish sequences one command (blocks while ingress is full).
func (h *Handle) Publish(sym uint32, cmd matcher.Command) Status {
	if !h.enter() {
		return Closed
	}
	t := h.sh.nowNs()
	r := h.ingress.Publish(func(m *CmdMsg) { *m = CmdMsg{TPub: t, Symbol: sym, Cmd: cmd} })
	h.exit()
	if r == disruptor.Ok {
		return Ok
	}
	return Closed
}

// TryPublish sequences one command, or returns Full without waiting.
func (h *Handle) TryPublish(sym uint32, cmd matcher.Command) Status {
	if !h.enter() {
		return Closed
	}
	t := h.sh.nowNs()
	r := h.ingress.TryPublish(func(m *CmdMsg) { *m = CmdMsg{TPub: t, Symbol: sym, Cmd: cmd} })
	h.exit()
	switch r {
	case disruptor.Ok:
		return Ok
	case disruptor.Full:
		return Full
	}
	return Closed
}

// SymCmd is one command for a symbol.
type SymCmd struct {
	Sym uint32
	Cmd matcher.Command
}

// PublishBatch publishes many commands, one claim per chunk (consecutive iseqs within a chunk).
func (h *Handle) PublishBatch(cmds []SymCmd) Status {
	if !h.enter() {
		return Closed
	}
	chunk := h.ingress.Size()
	if chunk > 256 {
		chunk = 256
	}
	r := disruptor.Ok
	for off := 0; off < len(cmds) && r == disruptor.Ok; off += chunk {
		part := cmds[off:min(off+chunk, len(cmds))]
		t := h.sh.nowNs()
		r = h.ingress.PublishBatch(len(part), func(i int, m *CmdMsg) {
			*m = CmdMsg{TPub: t, Symbol: part[i].Sym, Cmd: part[i].Cmd}
		})
	}
	h.exit()
	if r == disruptor.Ok {
		return Ok
	}
	return Closed
}

// Close unregisters the handle.
func (h *Handle) Close() {
	h.sh.handlesMu.Lock()
	delete(h.sh.handles, h.flag)
	h.sh.handlesMu.Unlock()
}

// ---- Builder --------------------------------------------------------------------------------

// Builder configures a Pipeline.
type Builder struct {
	core                   CoreFactory
	book                   matcher.BookConfig
	partitions             uint32
	pmap                   *PartitionMap
	ingress, inbox, outbox int
	waits                  Waits
	journal                *JournalConfig
	egress                 []EgressFactory
	timestamps             bool
	egressThreads          int
	initial                *Initial
	checkpointEvery        time.Duration
}

// NewBuilder starts a pipeline over cores made by core (NewFifoCore, NewNoopCore, …).
func NewBuilder(core CoreFactory) *Builder {
	// ring sizes tuned in orderer-rust phase 6: L2-friendly, ~4x lower queueing latency
	return &Builder{core: core, book: matcher.DefaultConfig(), partitions: 1,
		ingress: 1 << 14, inbox: 1 << 12, outbox: 1 << 13, waits: RelaxedWaits(), egressThreads: 1}
}

func (b *Builder) BookConfig(c matcher.BookConfig) *Builder { b.book = c; return b }
func (b *Builder) Partitions(p uint32) *Builder             { b.partitions, b.pmap = p, nil; return b }
func (b *Builder) PartitionMap(m *PartitionMap) *Builder {
	b.partitions, b.pmap = m.Partitions(), m
	return b
}
func (b *Builder) RingSizes(ingress, inbox, outbox int) *Builder {
	b.ingress, b.inbox, b.outbox = ingress, inbox, outbox
	return b
}
func (b *Builder) Waits(w Waits) *Builder           { b.waits = w; return b }
func (b *Builder) Journal(j JournalConfig) *Builder { b.journal = &j; return b }
func (b *Builder) Egress(f EgressFactory) *Builder  { b.egress = append(b.egress, f); return b }
func (b *Builder) Timestamps(on bool) *Builder      { b.timestamps = on; return b }

// CheckpointEvery takes a checkpoint (spec/JOURNAL.md §6) every d from a
// background goroutine. Needs journals; Shutdown stops it first; a failed
// checkpoint fails the pipeline.
func (b *Builder) CheckpointEvery(d time.Duration) *Builder { b.checkpointEvery = d; return b }

func (b *Builder) Initial(i Initial) *Builder { b.initial = &i; return b }

// EgressThreads sets the goroutines running the partitions' egress plugs (partition p → p % n).
func (b *Builder) EgressThreads(n int) *Builder {
	b.egressThreads = max(n, 1)
	return b
}

// ---- Pipeline -------------------------------------------------------------------------------

// Pipeline is a running pipeline. Shutdown (or Close) stops it.
type Pipeline struct {
	sh       *sharedState
	pmap     *PartitionMap
	wg       sync.WaitGroup
	handle   *Handle
	ingress  disruptor.MultiProducer[CmdMsg]
	shut     bool
	ckptStop chan struct{}
	ckptDone chan struct{}
}

func (p *Pipeline) Handle() *Handle                               { return newHandle(p.ingress, p.sh) }
func (p *Pipeline) Publish(s uint32, c matcher.Command) Status    { return p.handle.Publish(s, c) }
func (p *Pipeline) TryPublish(s uint32, c matcher.Command) Status { return p.handle.TryPublish(s, c) }
func (p *Pipeline) PublishBatch(cmds []SymCmd) Status             { return p.handle.PublishBatch(cmds) }
func (p *Pipeline) Partitions() uint32                            { return p.sh.partitions }
func (p *Pipeline) PartitionOf(s uint32) uint32                   { return p.pmap.Partition(s) }
func (p *Pipeline) BookConfig() matcher.BookConfig                { return p.sh.book }
func (p *Pipeline) SetTimestamps(on bool)                         { p.sh.timestamps.Store(on) }
func (p *Pipeline) DurableIseq(part uint32) uint64                { return p.sh.durable[part].Load() }

// Stats is a point-in-time view of the pipeline (see stats.go).
func (p *Pipeline) Stats() PipelineStats {
	sh := p.sh
	depth := func(pub, con int64) uint64 {
		if pub > con {
			return uint64(pub - con)
		}
		return 0
	}
	st := PipelineStats{IngressDepth: depth(sh.ingressCtl.Published(), sh.ingressCtl.Consumed())}
	for i := uint32(0); i < sh.partitions; i++ {
		flushed := uint64(math.MaxUint64)
		if sh.journal != nil {
			flushed = sh.flushed[i].Load()
		}
		io := sh.io[i]
		st.Partitions = append(st.Partitions, PartitionStats{
			Partition:    i,
			InboxDepth:   depth(sh.inboxCtl[i].Published(), sh.inboxCtl[i].Consumed()),
			OutboxDepth:  depth(sh.outboxCtl[i].Published(), sh.outboxCtl[i].Consumed()),
			Commands:     sh.counters[i].commands.Load(),
			Events:       sh.counters[i].events.Load(),
			FlushedIseq:  flushed,
			DurableIseq:  sh.durable[i].Load(),
			Fsyncs:       io.Fsyncs.Load(),
			FsyncNsTotal: io.FsyncNsTotal.Load(),
			FsyncNsMax:   io.FsyncNsMax.Load(),
		})
	}
	return st
}

// Drain is a barrier: it returns once every command published before the
// call has been applied and delivered to every egress plug.
func (p *Pipeline) Drain() error {
	epoch := p.sh.nextEpoch.Add(1)
	if err := p.publishCtl(CtlBarrier, epoch); err != nil {
		return err
	}
	return waitUntil(p.sh, func() bool {
		for i := range p.sh.egressEpoch {
			if p.sh.egressEpoch[i].v.Load() < epoch {
				return false
			}
		}
		return true
	})
}

// Snapshot is a consistent snapshot of every book, cut at this point of the ingress order.
func (p *Pipeline) Snapshot() (*Snapshot, error) { return p.snapshotOp(CtlSnapshot) }

// Checkpoint is a snapshot cut at this point of the ingress order; every
// journal rotates onto a new segment at the cut; the snapshot is written
// durably into the journal directory; older segments and checkpoints are
// removed (spec/JOURNAL.md §6).
func (p *Pipeline) Checkpoint() (*Snapshot, error) {
	cfg := p.sh.journal
	if cfg == nil {
		return nil, &Error{ErrConfig, "checkpoint needs journals"}
	}
	s, err := p.snapshotOp(CtlCheckpoint)
	if err != nil {
		return nil, err
	}
	if err := p.Drain(); err != nil { // every egress has rotated its event journal
		return nil, err
	}
	path := CheckpointPath(cfg.Dir, s.Iseq)
	for _, step := range []func() error{
		func() error { return writeDurably(path, []byte(s.Body)) },
		func() error { return writeDurably(MetaPath(path), []byte(s.Meta())) },
		func() error { return removeSegmentsBelow(cfg.Dir, cfg.Format, s.Iseq) },
		func() error { return removeCheckpointsBelow(cfg.Dir, s.Iseq) },
	} {
		if err := step(); err != nil {
			return nil, &Error{ErrIO, err.Error()}
		}
	}
	return s, nil
}

func (p *Pipeline) snapshotOp(ctl Control) (*Snapshot, error) {
	sh := p.sh
	op := sh.nextOp.Add(1)
	sh.snapsMu.Lock()
	sh.snaps[op] = &snapState{remaining: sh.partitions}
	sh.snapsMu.Unlock()
	if err := p.publishCtl(ctl, op); err != nil {
		return nil, err
	}
	sh.snapsMu.Lock()
	for !sh.failed.Load() && sh.snaps[op].remaining != 0 {
		sh.snapsCond.Wait()
	}
	st := sh.snaps[op]
	delete(sh.snaps, op)
	sh.snapsMu.Unlock()
	if err := sh.check(); err != nil {
		return nil, err
	}
	sort.Slice(st.blocks, func(i, j int) bool { return st.blocks[i].Symbol < st.blocks[j].Symbol })
	var body strings.Builder
	body.WriteString(SnapshotHeader(sh.book))
	for _, b := range st.blocks {
		body.WriteString(b.Text)
	}
	return &Snapshot{Body: body.String(), Iseq: st.cut, Partitions: sh.partitions}, nil
}

// Shutdown stops accepting commands, drains everything sequenced and stops
// every goroutine. Idempotent. Returns an ErrFailed error if any stage failed.
func (p *Pipeline) Shutdown() error {
	sh := p.sh
	if p.shut {
		return sh.check()
	}
	p.shut = true
	if p.ckptStop != nil { // a checkpoint in progress finishes first
		close(p.ckptStop)
		<-p.ckptDone
	}
	sh.closed.Store(true)
	sh.handlesMu.Lock()
	flags := make([]*inFlight, 0, len(sh.handles))
	for f := range sh.handles {
		flags = append(flags, f)
	}
	sh.handlesMu.Unlock()
	_ = waitUntil(sh, func() bool {
		for _, f := range flags {
			if f.v.Load() {
				return false
			}
		}
		return true
	})
	if !sh.failed.Load() {
		r := p.ingress.Publish(func(m *CmdMsg) { *m = CmdMsg{Ctl: CtlShutdown} })
		if r != disruptor.Ok {
			sh.fail("ingress alerted before shutdown")
		}
	}
	p.wg.Wait()
	sh.alertAll()
	return sh.check()
}

// Close is Shutdown.
func (p *Pipeline) Close() error { return p.Shutdown() }

func (p *Pipeline) publishCtl(c Control, arg uint64) error {
	if err := p.sh.check(); err != nil {
		return err
	}
	if p.sh.closed.Load() {
		return &Error{ErrClosed, "pipeline closed"}
	}
	if p.ingress.Publish(func(m *CmdMsg) { *m = CmdMsg{Arg: arg, Ctl: c} }) != disruptor.Ok {
		return &Error{ErrClosed, "pipeline closed"}
	}
	return nil
}

type egressPart struct {
	p        uint32
	outbox   *disruptor.Consumer[EvtMsg]
	plugs    []Egress
	ctx      *EgressCtx
	lastIseq uint64
	stopped  bool
}

// segmenter opens a partition's next journal segment (spec/JOURNAL.md §6 step 2).
type segmenter struct {
	dir           string
	format        JournalFormat
	kind          JournalKind
	p, partitions uint32
	book          matcher.BookConfig
}

func (s *segmenter) rotate(w *ChunkWriter, cut uint64) error {
	f, err := openSegment(s.dir, s.format, s.kind, s.p, s.partitions, s.book, cut)
	if err != nil {
		return err
	}
	w.rotate(f)
	return nil
}

// evtJournal is the event journal as the first plug of its partition.
type evtJournal struct {
	w           *ChunkWriter
	f           JournalFormat
	seg         segmenter
	counters    *engineCounters
	nCommands   uint64
	nEvents     uint64
	lastHandoff time.Time
}

func (e *evtJournal) OnCheckpoint(cut uint64) {
	if err := e.seg.rotate(e.w, cut); err != nil {
		panic("event journal rotate: " + err.Error())
	}
}

func (e *evtJournal) OnEvent(m *EvtMsg) { e.w.pushEvt(e.f, m.Seq, m.Symbol, &m.Ev) }
func (e *evtJournal) OnBatchEnd()       {}
func (e *evtJournal) OnIdle() {
	if e.w.pending() > 0 && time.Since(e.lastHandoff) >= 50*time.Microsecond {
		e.w.handOff()
		e.lastHandoff = time.Now()
	}
}
func (e *evtJournal) OnShutdown() error { return e.w.finish() }

func pow2(n int) bool { return n >= 2 && n&(n-1) == 0 }

// Build starts the pipeline.
func (b *Builder) Build() (*Pipeline, error) {
	if b.checkpointEvery > 0 && b.journal == nil {
		return nil, &Error{ErrConfig, "CheckpointEvery needs journals"}
	}
	pmap := b.pmap
	if pmap == nil {
		m, err := NewPartitionMap(b.partitions, nil)
		if err != nil {
			return nil, &Error{ErrConfig, err.Error()}
		}
		pmap = m
	}
	P := pmap.Partitions()
	if !pow2(b.ingress) || !pow2(b.inbox) || !pow2(b.outbox) {
		return nil, &Error{ErrConfig, "ring sizes must be powers of two >= 2"}
	}
	var cores []MatchingCore
	nextIseq := uint64(1)
	if b.initial != nil {
		if uint32(len(b.initial.Cores)) != P {
			return nil, &Error{ErrConfig, "recovered cores != partitions"}
		}
		cores = b.initial.Cores
		nextIseq = max(b.initial.NextIseq, 1)
	} else {
		for i := uint32(0); i < P; i++ {
			cores = append(cores, b.core(b.book))
		}
	}
	journaled := b.journal != nil
	startWm := nextIseq - 1
	sh := &sharedState{partitions: P, book: b.book, epoch: time.Now(), handles: map[*inFlight]struct{}{},
		egressEpoch: make([]paddedU64, P), snaps: map[uint64]*snapState{}}
	sh.snapsCond = sync.NewCond(&sh.snapsMu)
	sh.timestamps.Store(b.timestamps)
	sh.journal = b.journal
	for i := uint32(0); i < P; i++ {
		f, d := &atomic.Uint64{}, &atomic.Uint64{}
		f.Store(startWm)
		if journaled {
			d.Store(startWm)
		} else {
			d.Store(math.MaxUint64)
		}
		sh.flushed, sh.durable = append(sh.flushed, f), append(sh.durable, d)
		sh.counters, sh.io = append(sh.counters, &engineCounters{}), append(sh.io, &IoStats{})
	}

	// journals first, so I/O errors surface from Build
	cmdW, evtW := make([]*ChunkWriter, P), make([]*ChunkWriter, P)
	if journaled {
		jc := b.journal
		var err error
		if err = os.MkdirAll(jc.Dir, 0o755); err == nil && !jc.Append {
			err = clearJournalDir(jc.Dir, jc.Format)
		}
		if err == nil {
			for i := uint32(0); i < P && err == nil; i++ {
				var f *os.File
				if f, err = openJournal(jc, KindCmd, i, P, b.book); err != nil {
					break
				}
				cmdW[i] = newChunkWriter(f, &jc.Fsync, sh.flushed[i], sh.durable[i], sh.io[i])
				if jc.Events {
					if f, err = openJournal(jc, KindEvt, i, P, b.book); err != nil {
						break
					}
					evtW[i] = newChunkWriter(f, nil, &atomic.Uint64{}, &atomic.Uint64{}, &IoStats{})
				}
			}
		}
		if err != nil {
			for _, w := range append(cmdW, evtW...) {
				if w != nil {
					w.finish()
				}
			}
			return nil, &Error{ErrIO, err.Error()}
		}
	}

	pl := &Pipeline{sh: sh, pmap: pmap}
	inboxes := make([]*disruptor.SingleProducer[CmdMsg], P)
	nEgress := min(b.egressThreads, int(P))
	groups := make([][]*egressPart, nEgress)
	var fmtp *JournalFormat
	if journaled {
		fmtp = &b.journal.Format
	}
	for i := uint32(0); i < P; i++ {
		ib := disruptor.NewRingBuilder[CmdMsg](b.inbox)
		ib.Consumer(b.waits.Engine)
		inbox, icons := ib.BuildSingle()
		ictl := inbox.Control()
		sh.inboxCtl = append(sh.inboxCtl, ictl)
		sh.addAlert(ictl.Alert)
		inboxes[i] = inbox

		ob := disruptor.NewRingBuilder[EvtMsg](b.outbox)
		ob.Consumer(b.waits.Egress)
		outbox, ocons := ob.BuildSingle()
		octl := outbox.Control()
		sh.outboxCtl = append(sh.outboxCtl, octl)
		sh.addAlert(octl.Alert)

		eng := &engine{sh: sh, inbox: icons[0], out: outbox, core: cores[i], journal: cmdW[i], fmt: fmtp, counters: sh.counters[i]}
		if journaled {
			eng.seg = segmenter{b.journal.Dir, b.journal.Format, KindCmd, i, P, b.book}
		}
		pl.spawn("engine", eng.run)

		ctx := &EgressCtx{Partition: i, Partitions: P, Epoch: sh.epoch, durable: sh.durable[i]}
		part := &egressPart{p: i, outbox: ocons[0], ctx: ctx}
		if evtW[i] != nil {
			part.plugs = append(part.plugs, &evtJournal{w: evtW[i], f: b.journal.Format, lastHandoff: time.Now(),
				seg: segmenter{b.journal.Dir, b.journal.Format, KindEvt, i, P, b.book}})
		}
		for _, f := range b.egress {
			part.plugs = append(part.plugs, f(ctx))
		}
		groups[int(i)%nEgress] = append(groups[int(i)%nEgress], part)
	}
	for _, g := range groups {
		g := g
		pl.spawn("egress", func() error { return egressLoop(sh, g, journaled) })
	}
	rb := disruptor.NewRingBuilder[CmdMsg](b.ingress)
	rb.Consumer(b.waits.Router)
	ingress, rcons := rb.BuildMulti()
	gctl := ingress.Control()
	sh.ingressCtl = gctl
	sh.addAlert(gctl.Alert)
	pl.ingress = ingress
	pl.spawn("router", func() error { routerLoop(rcons[0], inboxes, pmap, nextIseq); return nil })
	pl.handle = newHandle(ingress, sh)
	if b.checkpointEvery > 0 {
		pl.ckptStop, pl.ckptDone = make(chan struct{}), make(chan struct{})
		go pl.checkpointLoop(b.checkpointEvery)
	}
	return pl, nil
}

// checkpointLoop checkpoints every d until Shutdown stops it.
func (p *Pipeline) checkpointLoop(d time.Duration) {
	defer close(p.ckptDone)
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-p.ckptStop:
			return
		case <-t.C:
		}
		if p.sh.failed.Load() {
			return
		}
		if _, err := p.Checkpoint(); err != nil {
			var pe *Error
			if !errors.As(err, &pe) || (pe.Kind != ErrClosed && pe.Kind != ErrFailed) {
				p.sh.fail("checkpoint: " + err.Error())
			}
			return
		}
	}
}

// spawn runs body on its own locked OS thread; a returned error or panic fails the pipeline.
func (p *Pipeline) spawn(name string, body func() error) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer func() {
			if r := recover(); r != nil {
				p.sh.fail(fmt.Sprintf("%s thread failed: %v", name, r))
			}
		}()
		if err := body(); err != nil {
			p.sh.fail(fmt.Sprintf("%s thread failed: %v", name, err))
		}
	}()
}

func routerLoop(ingress *disruptor.Consumer[CmdMsg], inboxes []*disruptor.SingleProducer[CmdMsg], pmap *PartitionMap,
	nextIseq uint64) {
	iseq := nextIseq - 1
	stop := false
	h := func(m *CmdMsg, _ int64, eob bool) {
		if m.Ctl == CtlNone {
			iseq++
			if s := inboxes[pmap.Partition(m.Symbol)].Stage(); s != nil {
				*s = *m
				s.Iseq = iseq
			}
		} else {
			for _, ib := range inboxes {
				if s := ib.Stage(); s != nil {
					*s = *m
					s.Iseq = iseq
				}
			}
			if m.Ctl == CtlShutdown {
				stop = true
			}
		}
		if eob {
			for _, ib := range inboxes {
				ib.Commit()
			}
		}
	}
	for !stop && ingress.WaitPoll(h) {
	}
	for _, ib := range inboxes {
		ib.Commit()
	}
}

// engine is one partition's engine: journal-before-apply, then stage events.
type engine struct {
	sh          *sharedState
	inbox       *disruptor.Consumer[CmdMsg]
	out         *disruptor.SingleProducer[EvtMsg]
	core        MatchingCore
	journal     *ChunkWriter
	fmt         *JournalFormat
	seg         segmenter
	iseq, tPub  uint64
	stop, force bool
	counters    *engineCounters
	nCommands   uint64
	nEvents     uint64
}

// Emit stages one event into the outbox.
func (e *engine) Emit(sym uint32, seq uint64, ev *matcher.Event) {
	e.nEvents++
	s := e.out.Stage()
	if s == nil {
		panic("outbox alerted")
	}
	*s = EvtMsg{Iseq: e.iseq, Seq: seq, TPub: e.tPub, Symbol: sym, Ev: *ev}
}

func (e *engine) onCmd(m *CmdMsg, _ int64, eob bool) {
	if m.Ctl == CtlNone {
		if e.journal != nil { // journal-before-apply
			e.journal.pushCmd(*e.fmt, m.Iseq, m.Symbol, &m.Cmd)
		}
		e.iseq, e.tPub = m.Iseq, m.TPub
		e.nCommands++
		e.core.Apply(m.Symbol, &m.Cmd, e)
	} else {
		// the new segment starts at this cut, before the snapshot is reported
		if m.Ctl == CtlCheckpoint && e.journal != nil {
			if err := e.seg.rotate(e.journal, m.Iseq); err != nil {
				panic("journal rotate: " + err.Error())
			}
		}
		if m.Ctl == CtlSnapshot || m.Ctl == CtlCheckpoint {
			blocks := e.core.SnapshotBlocks(nil)
			e.sh.snapsMu.Lock()
			if st := e.sh.snaps[m.Arg]; st != nil {
				st.blocks = append(st.blocks, blocks...)
				st.cut = m.Iseq
				st.remaining--
			}
			e.sh.snapsCond.Broadcast()
			e.sh.snapsMu.Unlock()
		}
		if m.Ctl == CtlShutdown {
			e.stop = true
			if e.journal != nil {
				if err := e.journal.finish(); err != nil {
					e.sh.fail("journal: " + err.Error())
				}
			}
		} else {
			e.force = true
		}
		if s := e.out.Stage(); s != nil {
			*s = EvtMsg{Iseq: m.Iseq, Arg: m.Arg, Ctl: m.Ctl}
		}
	}
	if eob {
		e.out.Commit()
	}
}

func (e *engine) run() error {
	lastHandoff := time.Now()
	h := e.onCmd
	for {
		e.force = false
		n := e.inbox.Poll(h)
		if n > 0 {
			e.counters.commands.Store(e.nCommands)
			e.counters.events.Store(e.nEvents)
		}
		if e.stop || e.inbox.IsAlerted() {
			break
		}
		if e.journal != nil && e.journal.pending() > 0 &&
			(e.force || (n == 0 && time.Since(lastHandoff) >= 50*time.Microsecond)) {
			e.journal.handOff()
			lastHandoff = time.Now()
		}
		if n == 0 {
			e.inbox.Idle()
		} else {
			e.inbox.ResetIdle()
		}
	}
	e.out.Commit()
	return nil
}

func egressLoop(sh *sharedState, parts []*egressPart, journaled bool) error {
	idleOn := 0
	var ep *egressPart
	stop := false
	h := func(m *EvtMsg, _ int64, eob bool) {
		switch m.Ctl {
		case CtlNone:
			ep.lastIseq = m.Iseq
			for _, pl := range ep.plugs {
				pl.OnEvent(m)
			}
		case CtlBarrier:
			for _, pl := range ep.plugs {
				pl.OnBatchEnd()
				pl.OnIdle()
			}
			sh.egressEpoch[ep.p].v.Store(m.Arg)
		case CtlShutdown:
			stop = true
		case CtlCheckpoint:
			for _, pl := range ep.plugs {
				if c, ok := pl.(Checkpointer); ok {
					c.OnCheckpoint(m.Iseq)
				}
			}
		}
		if eob {
			for _, pl := range ep.plugs {
				pl.OnBatchEnd()
			}
		}
	}
	for {
		total, live := 0, 0
		for i, part := range parts {
			if part.stopped {
				continue
			}
			live++
			idleOn = i
			ep, stop = part, false
			n := part.outbox.Poll(h)
			total += n
			if stop {
				if journaled {
					d, last := part.ctx.durable, part.lastIseq
					if err := waitUntil(sh, func() bool { return d.Load() >= last }); err != nil {
						return err
					}
				}
				var first error
				for _, pl := range part.plugs {
					pl.OnIdle()
					if err := pl.OnShutdown(); err != nil && first == nil {
						first = err
					}
				}
				part.stopped = true
				if first != nil {
					return first
				}
			} else if n == 0 {
				for _, pl := range part.plugs {
					pl.OnIdle()
				}
			}
		}
		if live == 0 {
			return nil
		}
		for _, part := range parts {
			if part.outbox.IsAlerted() {
				return nil
			}
		}
		if total == 0 {
			parts[idleOn].outbox.Idle()
		} else {
			parts[idleOn].outbox.ResetIdle()
		}
	}
}
