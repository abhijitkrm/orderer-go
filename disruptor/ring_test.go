// Ring protocol tests (orderer-rust tests/ring.rs, ported): wrap,
// multi-producer integrity, batching, gating, try-publish CAS path, stalled
// producers, barrier dependencies, wait strategies, multicast. Run them with
// -race too (scripts/test.sh does).
package disruptor

import (
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func drainN[T any](t *testing.T, c *Consumer[T], n int, f func(*T, int64, bool)) {
	deadline := time.Now().Add(20 * time.Second)
	for got := 0; got < n; {
		got += c.Poll(f)
		if time.Now().After(deadline) {
			t.Errorf("timed out at %d", got)
			return
		}
		if got < n {
			runtime.Gosched()
		}
	}
}

func single[T any](size int) (*SingleProducer[T], *Consumer[T]) {
	b := NewRingBuilder[T](size)
	b.Consumer(BackoffWait())
	p, cs := b.BuildSingle()
	return p, cs[0]
}

func multi[T any](size int) (MultiProducer[T], *Consumer[T]) {
	b := NewRingBuilder[T](size)
	b.Consumer(BackoffWait())
	p, cs := b.BuildMulti()
	return p, cs[0]
}

func TestSpscWrap(t *testing.T) {
	p, c := single[uint64](8)
	const N = 100_000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		expect := uint64(0)
		bad := 0
		drainN(t, c, N, func(v *uint64, seq int64, _ bool) {
			if *v != expect || uint64(seq) != expect {
				bad++
			}
			expect++
		})
		if bad > 0 {
			t.Errorf("%d out-of-order reads", bad)
		}
	}()
	for i := uint64(0); i < N; i++ {
		p.Publish(func(s *uint64) { *s = i })
	}
	wg.Wait()
}

type msg struct{ producer, counter, check uint64 }

func multiProducerRun(t *testing.T, batch int) {
	const producers, per = 4, 100_000
	p, c := multi[msg](1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		next := make([]uint64, producers)
		last, bad := int64(-1), 0
		drainN(t, c, producers*per, func(m *msg, seq int64, _ bool) {
			if seq != last+1 || m.check != (m.producer*31)^m.counter || m.counter != next[m.producer] {
				bad++
			}
			last = seq
			next[m.producer]++
		})
		if bad > 0 {
			t.Errorf("%d ordering / torn-slot violations", bad)
		}
	}()
	var ws sync.WaitGroup
	for id := uint64(0); id < producers; id++ {
		ws.Add(1)
		go func(id uint64) {
			defer ws.Done()
			for i := 0; i < per; {
				n := min(batch, per-i)
				base := uint64(i)
				p.PublishBatch(n, func(k int, s *msg) { *s = msg{id, base + uint64(k), (id * 31) ^ (base + uint64(k))} })
				i += n
			}
		}(id)
	}
	ws.Wait()
	wg.Wait()
}

func TestMultiProducerIntegritySingleClaims(t *testing.T)  { multiProducerRun(t, 1) }
func TestMultiProducerIntegrityBatchedClaims(t *testing.T) { multiProducerRun(t, 37) }

func TestBatchClaimConsumeAndStaging(t *testing.T) {
	p, c := single[uint32](16)
	p.PublishBatch(5, func(i int, s *uint32) { *s = uint32(i * 10) })
	var eobs []bool
	if n := c.Poll(func(_ *uint32, _ int64, e bool) { eobs = append(eobs, e) }); n != 5 {
		t.Fatalf("polled %d", n)
	}
	if !eobs[4] || eobs[0] {
		t.Error("end_of_batch on the last only")
	}
	*p.Stage() = 1
	*p.Stage() = 2
	if p.Staged() != 2 {
		t.Error("staged count")
	}
	nop := func(*uint32, int64, bool) {}
	if c.Poll(nop) != 0 {
		t.Error("staged is invisible")
	}
	p.Commit()
	if c.Poll(nop) != 2 {
		t.Error("commit publishes")
	}
	p.PublishBatch(10, func(i int, s *uint32) { *s = uint32(i) })
	c.SetMaxBatch(4)
	if c.Poll(nop) != 4 {
		t.Error("max batch")
	}
}

func TestGatingBlocksAndTryPublishFull(t *testing.T) {
	p, c := single[uint32](4)
	for i := uint32(0); i < 4; i++ {
		if p.TryPublish(func(s *uint32) { *s = i }) != Ok {
			t.Fatal("try publish")
		}
	}
	if p.TryPublish(func(s *uint32) { *s = 99 }) != Full {
		t.Fatal("expected Full")
	}
	var done atomic.Bool
	finished := make(chan struct{})
	go func() { p.Publish(func(s *uint32) { *s = 4 }); done.Store(true); close(finished) }()
	time.Sleep(50 * time.Millisecond)
	if done.Load() {
		t.Error("producer must not lap the consumer")
	}
	var seen []uint32
	c.Poll(func(v *uint32, _ int64, _ bool) { seen = append(seen, *v) })
	<-finished
	c.Poll(func(v *uint32, _ int64, _ bool) { seen = append(seen, *v) })
	if !reflect.DeepEqual(seen, []uint32{0, 1, 2, 3, 4}) {
		t.Errorf("zero loss under Block: %v", seen)
	}
}

func TestStagedWorkIsCommittedBeforeWaiting(t *testing.T) {
	p, c := single[uint32](4)
	var seen []uint32
	done := make(chan struct{})
	go func() { drainN(t, c, 10, func(v *uint32, _ int64, _ bool) { seen = append(seen, *v) }); close(done) }()
	for i := uint32(0); i < 10; i++ {
		*p.Stage() = i
	}
	p.Commit()
	<-done
	if len(seen) != 10 || seen[9] != 9 {
		t.Errorf("seen %v", seen)
	}
}

func TestTryPublishCasNeverLeaksClaims(t *testing.T) {
	p, c := multi[uint32](8)
	for i := uint32(0); i < 8; i++ {
		p.TryPublish(func(s *uint32) { *s = i })
	}
	if p.TryPublish(func(s *uint32) { *s = 99 }) != Full || p.TryPublishBatch(3, func(int, *uint32) {}) != Full {
		t.Fatal("expected Full")
	}
	if p.Control().Published() != 7 {
		t.Error("a failed try leaves the cursor untouched")
	}
	if c.Poll(func(*uint32, int64, bool) {}) != 8 {
		t.Error("poll 8")
	}
	if p.TryPublish(func(s *uint32) { *s = 8 }) != Ok {
		t.Error("room after consuming")
	}
}

func TestTryAndBlockProducersNeverDoubleClaim(t *testing.T) {
	const per = 20_000
	p, c := multi[uint64](64)
	var accepted atomic.Int64
	var stop atomic.Bool
	delivered := make(chan int)
	go func() {
		seen := map[uint64]bool{}
		last, bad := int64(-1), 0
		for {
			stopping := stop.Load()
			n := c.Poll(func(v *uint64, seq int64, _ bool) {
				if seq != last+1 || seen[*v] {
					bad++
				}
				last = seq
				seen[*v] = true
			})
			if n == 0 {
				if stopping {
					break
				}
				runtime.Gosched()
			}
		}
		if bad > 0 {
			t.Errorf("%d delivered twice / out of order", bad)
		}
		delivered <- len(seen)
	}()
	var ws sync.WaitGroup
	for id := uint64(0); id < 4; id++ {
		ws.Add(1)
		go func(id uint64) {
			defer ws.Done()
			for i := uint64(0); i < per; i++ {
				v := id<<32 | i
				if id%2 == 0 {
					p.Publish(func(s *uint64) { *s = v })
					accepted.Add(1)
				} else if p.TryPublish(func(s *uint64) { *s = v }) == Ok {
					accepted.Add(1)
				}
			}
		}(id)
	}
	ws.Wait()
	stop.Store(true)
	if d := <-delivered; int64(d) != accepted.Load() {
		t.Errorf("%d delivered vs %d accepted", d, accepted.Load())
	}
}

func TestStalledProducerGatesOnlyItsOwnSlot(t *testing.T) {
	p, c := multi[uint32](16)
	p.Publish(func(s *uint32) { *s = 0 })
	p.Publish(func(s *uint32) { *s = 1 })
	claimed, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		p.Publish(func(s *uint32) { close(claimed); <-release; *s = 2 })
		close(done)
	}()
	<-claimed
	p.Publish(func(s *uint32) { *s = 3 })
	p.Publish(func(s *uint32) { *s = 4 })
	var seen []uint32
	c.Poll(func(v *uint32, _ int64, _ bool) { seen = append(seen, *v) })
	if !reflect.DeepEqual(seen, []uint32{0, 1}) {
		t.Errorf("consumer stops at the unpublished slot: %v", seen)
	}
	close(release)
	<-done
	c.Poll(func(v *uint32, _ int64, _ bool) { seen = append(seen, *v) })
	if !reflect.DeepEqual(seen, []uint32{0, 1, 2, 3, 4}) {
		t.Errorf("seen %v", seen)
	}
}

func TestBarrierDependencyOrdersStages(t *testing.T) {
	const N = 200_000
	b := NewRingBuilder[int64](256)
	ida := b.Consumer(BackoffWait())
	b.Consumer(BackoffWait(), ida)
	p, cs := b.BuildSingle()
	aSeq := cs[0].Sequence()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); drainN(t, cs[0], N, func(*int64, int64, bool) {}) }()
	bad := 0
	go func() {
		defer wg.Done()
		drainN(t, cs[1], N, func(v *int64, seq int64, _ bool) {
			if *v != seq || aSeq.Get() < seq {
				bad++
			}
		})
	}()
	for i := int64(0); i < N; i++ {
		p.Publish(func(s *int64) { *s = i })
	}
	wg.Wait()
	if bad > 0 {
		t.Error("stage B overtook stage A")
	}
}

func TestWaitStrategiesAllDeliver(t *testing.T) {
	for _, w := range []WaitStrategy{BusySpinWait(), YieldWait(), BackoffWait(), BlockingWait()} {
		b := NewRingBuilder[uint32](16)
		b.Consumer(w)
		p, cs := b.BuildSingle()
		ctl := p.Control()
		var seen []uint32
		done := make(chan struct{})
		go func() {
			for cs[0].WaitPoll(func(v *uint32, _ int64, _ bool) { seen = append(seen, *v) }) {
			}
			close(done)
		}()
		for i := uint32(0); i < 20; i++ {
			p.Publish(func(s *uint32) { *s = i })
			if i%5 == 0 {
				time.Sleep(3 * time.Millisecond)
			}
		}
		for ctl.Consumed() < 19 {
			runtime.Gosched()
		}
		ctl.Alert()
		<-done
		if len(seen) != 20 || seen[19] != 19 {
			t.Errorf("kind %d: seen %v", w.Kind, seen)
		}
	}
}

func TestMulticastAllSeeEverythingSlowestGates(t *testing.T) {
	b := NewRingBuilder[uint32](8)
	for i := 0; i < 3; i++ {
		b.Consumer(BackoffWait())
	}
	p, cs := b.BuildSingle()
	for i := uint32(0); i < 8; i++ {
		p.Publish(func(s *uint32) { *s = i })
	}
	nop := func(*uint32, int64, bool) {}
	if cs[0].Poll(nop) != 8 || cs[1].Poll(nop) != 8 {
		t.Fatal("fast consumers")
	}
	if p.TryPublish(func(s *uint32) { *s = 8 }) != Full {
		t.Error("third consumer gates")
	}
	if cs[2].Poll(nop) != 8 || p.TryPublish(func(s *uint32) { *s = 8 }) != Ok {
		t.Error("slowest consumer releases")
	}
}
