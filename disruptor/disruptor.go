// Package disruptor is the LMAX Disruptor in Go (orderer-rust's
// orderer-disruptor, ported).
//
// Protocol: write a slot only under an unpublished claim (granted once every
// gating consumer has passed seq - size); read it only after a load of the
// cursor / availability flag the writer stored after writing, and only until
// the reader's own sequence (stored after reading) passes it. Slots are
// values in one preallocated slice, rewritten in place, so the ring never
// allocates. Go's sync/atomic operations are sequentially consistent, which
// is at least the acquire/release the protocol needs.
package disruptor

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Initial is every sequence's starting value: nothing published or consumed.
const Initial int64 = -1

// Sequence is a counter padded to sit alone on its cache lines (128 B each side).
type Sequence struct {
	_ [128]byte
	v atomic.Int64
	_ [120]byte
}

func newSequence() *Sequence {
	s := &Sequence{}
	s.v.Store(Initial)
	return s
}

// Get loads the sequence.
func (s *Sequence) Get() int64 { return s.v.Load() }

// Set stores the sequence.
func (s *Sequence) Set(v int64) { s.v.Store(v) }

func minOf(seqs []*Sequence, floor int64) int64 {
	m := floor
	for _, s := range seqs {
		if v := s.v.Load(); v < m {
			m = v
		}
	}
	return m
}

// ---- wait strategies -------------------------------------------------------------

// WaitKind selects how an idle consumer waits.
type WaitKind uint8

const (
	BusySpin WaitKind = iota
	Yield
	Backoff
	Blocking
)

// WaitStrategy is a WaitKind plus its tuning.
type WaitStrategy struct {
	Kind             WaitKind
	Spin, Yields     int
	ParkMin, ParkMax time.Duration
}

func BusySpinWait() WaitStrategy { return WaitStrategy{Kind: BusySpin} }
func YieldWait() WaitStrategy    { return WaitStrategy{Kind: Yield, Spin: 100} }
func BackoffWait() WaitStrategy {
	return WaitStrategy{Kind: Backoff, Spin: 256, Yields: 64, ParkMin: 20 * time.Microsecond, ParkMax: time.Millisecond}
}
func BlockingWait() WaitStrategy { return WaitStrategy{Kind: Blocking, Spin: 100} }

// notifier wakes Blocking waiters (bounded 1 ms wait, so a missed signal only delays).
type notifier struct {
	waiters atomic.Int32
	mu      sync.Mutex
	ch      chan struct{}
}

func (n *notifier) signal() {
	if n.waiters.Load() != 0 {
		n.wakeAll()
	}
}

func (n *notifier) wakeAll() {
	n.mu.Lock()
	if n.ch != nil {
		close(n.ch)
		n.ch = nil
	}
	n.mu.Unlock()
}

func (n *notifier) block(t *time.Timer) {
	n.mu.Lock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	ch := n.ch
	n.mu.Unlock()
	n.waiters.Add(1)
	t.Reset(time.Millisecond)
	select {
	case <-ch:
		if !t.Stop() {
			<-t.C
		}
	case <-t.C:
	}
	n.waiters.Add(-1)
}

type waiter struct {
	s     WaitStrategy
	step  int
	park  time.Duration
	timer *time.Timer
}

func newWaiter(s WaitStrategy) waiter {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return waiter{s: s, timer: t}
}

func (w *waiter) reset() { w.step = 0 }

func (w *waiter) idle(n *notifier) {
	s := &w.s
	switch s.Kind {
	case BusySpin:
		// Yield the P every 16K idle spins (a few hundred µs): a goroutine
		// that never yields keeps its P until async preemption (10 ms), which
		// starves goroutines returning from syscalls (journal fsyncs) once
		// spinners fill GOMAXPROCS. Gosched returns at once when nothing else
		// is runnable. Yielding every 1K spins cost the journal-off rows.
		if w.step++; w.step&(1<<14-1) == 0 {
			runtime.Gosched()
		} else {
			spinHint()
		}
	case Yield:
		if w.step < s.Spin {
			w.step++
			spinHint()
		} else {
			runtime.Gosched()
		}
	case Backoff:
		switch {
		case w.step < s.Spin:
			w.step++
			spinHint()
		case w.step < s.Spin+s.Yields:
			w.step++
			runtime.Gosched()
		default:
			if w.step == s.Spin+s.Yields {
				w.step++
				w.park = s.ParkMin
			}
			time.Sleep(w.park)
			if w.park *= 2; w.park > s.ParkMax {
				w.park = s.ParkMax
			}
		}
	case Blocking:
		if w.step < s.Spin {
			w.step++
			spinHint()
		} else {
			n.block(w.timer)
		}
	}
}

var spinSink atomic.Int32

// spinHint is a short pause (Go has no portable pause instruction).
func spinHint() {
	for i := 0; i < 8; i++ {
		spinSink.Load()
	}
}

// ---- shared ring state -----------------------------------------------------------

// ProducerKind is Single (one producer, batch commits) or Multi (claims by fetch-add).
type ProducerKind uint8

const (
	Single ProducerKind = iota
	Multi
)

// Publish is a publish outcome.
type Publish uint8

const (
	Ok Publish = iota
	Full
	Alerted
)

type shared[T any] struct {
	slots       []T
	size, mask  int64
	shift       uint
	kind        ProducerKind
	cursor      *Sequence      // Single: published. Multi: claimed.
	available   []atomic.Int32 // Multi: lap of each slot's last publish
	gatingCache *Sequence
	gating      []*Sequence
	notifier    notifier
	alerted     atomic.Bool
}

func (s *shared[T]) slot(q int64) *T      { return &s.slots[q&s.mask] }
func (s *shared[T]) minGating() int64     { return minOf(s.gating, math.MaxInt64) }
func (s *shared[T]) lap(q int64) int32    { return int32(q >> s.shift) }
func (s *shared[T]) setAvailable(q int64) { s.available[q&s.mask].Store(s.lap(q)) }
func (s *shared[T]) isAvailable(q int64) bool {
	return s.available[q&s.mask].Load() == s.lap(q)
}

func (s *shared[T]) publishedUpto(lo, limit int64) int64 {
	hi := s.cursor.Get()
	if hi > limit {
		hi = limit
	}
	if s.kind == Single {
		return hi
	}
	for q := lo; q <= hi; q++ {
		if !s.isAvailable(q) {
			return q - 1
		}
	}
	return hi
}

func (s *shared[T]) publishedHighWater() int64 {
	if s.kind == Single {
		return s.cursor.Get()
	}
	floor := s.minGating()
	if c := s.cursor.Get(); c < floor {
		floor = c
	}
	return s.publishedUpto(floor+1, math.MaxInt64)
}

func (s *shared[T]) alert() {
	s.alerted.Store(true)
	s.notifier.wakeAll()
}

func producerBackoff(step *int) {
	if *step < 64 {
		*step++
		spinHint()
	} else {
		runtime.Gosched()
	}
}

// RingControl alerts a ring and reports its watermarks.
type RingControl[T any] struct{ s *shared[T] }

func (c RingControl[T]) Alert()           { c.s.alert() }
func (c RingControl[T]) IsAlerted() bool  { return c.s.alerted.Load() }
func (c RingControl[T]) Published() int64 { return c.s.publishedHighWater() }
func (c RingControl[T]) Consumed() int64  { return c.s.minGating() }

// ---- producers -------------------------------------------------------------------

// SingleProducer is the only producer of a Single ring: Stage + Commit
// publish a whole batch with one store.
type SingleProducer[T any] struct {
	s                           *shared[T]
	next, published, cachedGate int64
}

func (p *SingleProducer[T]) Size() int   { return int(p.s.size) }
func (p *SingleProducer[T]) Staged() int { return int(p.next - p.published) }

func (p *SingleProducer[T]) hasRoom(n int64) bool {
	wrap := p.next + n - p.s.size
	if wrap <= p.cachedGate {
		return true
	}
	p.cachedGate = p.s.minGating()
	return wrap <= p.cachedGate
}

func (p *SingleProducer[T]) waitRoom(n int64) Publish {
	if p.hasRoom(n) {
		return Ok
	}
	p.Commit() // consumers can't free space they can't see
	step := 0
	for !p.hasRoom(n) {
		if p.s.alerted.Load() {
			return Alerted
		}
		producerBackoff(&step)
	}
	return Ok
}

// Stage claims the next slot for the caller to fill; invisible until Commit.
// nil once the ring is alerted.
func (p *SingleProducer[T]) Stage() *T {
	if p.waitRoom(1) != Ok {
		return nil
	}
	p.next++
	return p.s.slot(p.next)
}

// Commit publishes everything staged.
func (p *SingleProducer[T]) Commit() {
	if p.next != p.published {
		p.published = p.next
		p.s.cursor.Set(p.next)
		p.s.notifier.signal()
	}
}

func (p *SingleProducer[T]) Publish(fill func(*T)) Publish {
	slot := p.Stage()
	if slot == nil {
		return Alerted
	}
	fill(slot)
	p.Commit()
	return Ok
}

func (p *SingleProducer[T]) TryPublish(fill func(*T)) Publish {
	if !p.hasRoom(1) {
		return Full
	}
	return p.Publish(fill)
}

func (p *SingleProducer[T]) PublishBatch(n int, fill func(int, *T)) Publish {
	if p.waitRoom(int64(n)) != Ok {
		return Alerted
	}
	for i := 0; i < n; i++ {
		p.next++
		fill(i, p.s.slot(p.next))
	}
	p.Commit()
	return Ok
}

func (p *SingleProducer[T]) Control() RingControl[T] { return RingControl[T]{p.s} }

// MultiProducer is a producer of a Multi ring; share it across goroutines.
type MultiProducer[T any] struct{ s *shared[T] }

func (p MultiProducer[T]) Size() int { return int(p.s.size) }

func (p MultiProducer[T]) roomFor(hi int64) bool {
	wrap := hi - p.s.size
	if wrap <= p.s.gatingCache.Get() {
		return true
	}
	g := p.s.minGating()
	p.s.gatingCache.Set(g)
	return wrap <= g
}

func (p MultiProducer[T]) fill(lo, hi int64, f func(int, *T)) {
	for q := lo; q <= hi; q++ {
		f(int(q-lo), p.s.slot(q))
	}
	for q := lo; q <= hi; q++ {
		p.s.setAvailable(q)
	}
	p.s.notifier.signal()
}

func (p MultiProducer[T]) PublishBatch(n int, f func(int, *T)) Publish {
	hi := p.s.cursor.v.Add(int64(n))
	step := 0
	for !p.roomFor(hi) {
		if p.s.alerted.Load() {
			return Alerted
		}
		producerBackoff(&step)
	}
	p.fill(hi-int64(n)+1, hi, f)
	return Ok
}

func (p MultiProducer[T]) Publish(f func(*T)) Publish {
	return p.PublishBatch(1, func(_ int, t *T) { f(t) })
}

// TryPublishBatch claims by CAS only if it fits: a fetch-add claim could not be backed out.
func (p MultiProducer[T]) TryPublishBatch(n int, f func(int, *T)) Publish {
	for {
		if p.s.alerted.Load() {
			return Alerted
		}
		cur := p.s.cursor.Get()
		hi := cur + int64(n)
		if !p.roomFor(hi) {
			return Full
		}
		if p.s.cursor.v.CompareAndSwap(cur, hi) {
			p.fill(cur+1, hi, f)
			return Ok
		}
	}
}

func (p MultiProducer[T]) TryPublish(f func(*T)) Publish {
	return p.TryPublishBatch(1, func(_ int, t *T) { f(t) })
}

func (p MultiProducer[T]) Control() RingControl[T] { return RingControl[T]{p.s} }

// ---- consumers -------------------------------------------------------------------

// Consumer is one consumer: a barrier (cursor + upstream consumers) plus its own watermark.
type Consumer[T any] struct {
	s        *shared[T]
	deps     []*Sequence
	seq      *Sequence
	next     int64
	maxBatch int64
	waiter   waiter
}

func (c *Consumer[T]) Sequence() *Sequence { return c.seq }
func (c *Consumer[T]) SetMaxBatch(n int)   { c.maxBatch = int64(n) }
func (c *Consumer[T]) IsAlerted() bool     { return c.s.alerted.Load() }

func (c *Consumer[T]) available() int64 {
	limit := c.next + c.maxBatch - 1
	if len(c.deps) == 0 {
		return c.s.publishedUpto(c.next, limit)
	}
	return minOf(c.deps, limit)
}

// Poll handles everything available (up to the batch cap) without waiting.
func (c *Consumer[T]) Poll(h func(ev *T, seq int64, endOfBatch bool)) int {
	avail := c.available()
	if avail < c.next {
		return 0
	}
	for q := c.next; q <= avail; q++ {
		h(c.s.slot(q), q, q == avail)
	}
	n := int(avail - c.next + 1)
	c.next = avail + 1
	c.seq.Set(avail)
	c.s.notifier.signal()
	return n
}

// WaitPoll waits for at least one event, then polls; false once alerted and empty.
func (c *Consumer[T]) WaitPoll(h func(ev *T, seq int64, endOfBatch bool)) bool {
	for {
		if c.Poll(h) > 0 {
			c.waiter.reset()
			return true
		}
		if c.s.alerted.Load() {
			return false
		}
		c.waiter.idle(&c.s.notifier)
	}
}

func (c *Consumer[T]) Idle()      { c.waiter.idle(&c.s.notifier) }
func (c *Consumer[T]) ResetIdle() { c.waiter.reset() }

// ---- builder ---------------------------------------------------------------------

// RingBuilder declares consumers and their upstream dependencies, then
// builds producer and consumers at once. The producer gates on terminal consumers.
type RingBuilder[T any] struct {
	size  int
	deps  [][]int
	waits []WaitStrategy
}

var ErrSize = errors.New("ring size must be a power of two")

func NewRingBuilder[T any](size int) *RingBuilder[T] {
	if size < 1 || size&(size-1) != 0 {
		panic(ErrSize)
	}
	return &RingBuilder[T]{size: size}
}

// Consumer declares a consumer (BackoffWait unless given) after its dependencies.
func (b *RingBuilder[T]) Consumer(w WaitStrategy, dependsOn ...int) int {
	for _, d := range dependsOn {
		if d >= len(b.deps) {
			panic("declare dependencies first")
		}
	}
	b.deps = append(b.deps, dependsOn)
	b.waits = append(b.waits, w)
	return len(b.deps) - 1
}

func (b *RingBuilder[T]) build(k ProducerKind) (*shared[T], []*Consumer[T]) {
	seqs := make([]*Sequence, len(b.deps))
	depended := make([]bool, len(b.deps))
	for i := range seqs {
		seqs[i] = newSequence()
	}
	for _, d := range b.deps {
		for _, x := range d {
			depended[x] = true
		}
	}
	var gating []*Sequence
	for i, s := range seqs {
		if !depended[i] {
			gating = append(gating, s)
		}
	}
	sh := &shared[T]{
		slots: make([]T, b.size), size: int64(b.size), mask: int64(b.size - 1),
		kind: k, cursor: newSequence(), gatingCache: newSequence(), gating: gating,
	}
	for 1<<sh.shift < b.size {
		sh.shift++
	}
	if k == Multi {
		sh.available = make([]atomic.Int32, b.size)
		for i := range sh.available {
			sh.available[i].Store(-1)
		}
	}
	cons := make([]*Consumer[T], len(seqs))
	for i := range seqs {
		ds := make([]*Sequence, len(b.deps[i]))
		for j, d := range b.deps[i] {
			ds[j] = seqs[d]
		}
		cons[i] = &Consumer[T]{s: sh, deps: ds, seq: seqs[i], next: seqs[i].Get() + 1, maxBatch: 1024, waiter: newWaiter(b.waits[i])}
	}
	return sh, cons
}

func (b *RingBuilder[T]) BuildSingle() (*SingleProducer[T], []*Consumer[T]) {
	sh, cons := b.build(Single)
	return &SingleProducer[T]{s: sh, next: Initial, published: Initial, cachedGate: Initial}, cons
}

func (b *RingBuilder[T]) BuildMulti() (MultiProducer[T], []*Consumer[T]) {
	sh, cons := b.build(Multi)
	return MultiProducer[T]{sh}, cons
}
