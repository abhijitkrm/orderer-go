// Integration suite (orderer-rust tests/{partitions,journal_recovery,
// control,plugs}.rs, ported).
package orderer

import (
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// ---- partitions -------------------------------------------------------------------------

func TestEveryPartitionCountMatchesReferencePerSymbol(t *testing.T) {
	cfg := fuzzCfg()
	for seed := uint64(1); seed <= 8; seed++ {
		cmds := fuzzCorpus(seed, 4000, 8)
		ref := referenceLines(cfg, cmds)
		for _, P := range []uint32{1, 2, 3, 4, 7} {
			parts := runPipeline(t, NewFifoCore, cfg, cmds, P, true)
			if P == 1 && !reflect.DeepEqual(parts[0], ref) {
				t.Errorf("seed %d: P=1 is the plain engine stream", seed)
			}
			all := concat(parts)
			if !dense(all) {
				t.Error("seq density")
			}
			if !reflect.DeepEqual(bySymbol(all), bySymbol(ref)) {
				t.Errorf("seed %d P=%d", seed, P)
			}
			for q, ls := range parts {
				for _, l := range ls {
					if s, _ := U64(l, "symbol"); HashPartition(uint32(s), P) != uint32(q) {
						t.Errorf("routing %s", l)
					}
				}
			}
		}
	}
}

func TestFuzzRunsAreDeterministicPerPartition(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		cmds := fuzzCorpus(seed, 3000, 8)
		if !reflect.DeepEqual(runPipeline(t, NewFifoCore, fuzzCfg(), cmds, 4, true), runPipeline(t, NewFifoCore, fuzzCfg(), cmds, 4, true)) {
			t.Errorf("seed %d", seed)
		}
	}
}

func TestPartitionTableRoutesSymbols(t *testing.T) {
	cmds := fuzzCorpus(11, 3000, 8)
	var table [][2]uint32
	for s := uint32(0); s < 8; s++ {
		q := uint32(2)
		if s == 5 {
			q = 0
		}
		table = append(table, [2]uint32{s, q})
	}
	m, err := NewPartitionMap(3, table)
	mustOk(t, err)
	f, h := Collect(true)
	p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).PartitionMap(m).Egress(f).Build()
	mustOk(t, err)
	p.PublishBatch(cmds)
	mustOk(t, p.Drain())
	mustOk(t, p.Shutdown())
	var parts [][]string
	for _, b := range h.Take() {
		parts = append(parts, lines(b))
	}
	if len(parts[1]) != 0 {
		t.Error("partition 1 must be empty")
	}
	for _, l := range parts[0] {
		if s, _ := U64(l, "symbol"); s != 5 {
			t.Errorf("partition 0 got %s", l)
		}
	}
	if !reflect.DeepEqual(bySymbol(concat(parts)), bySymbol(referenceLines(fuzzCfg(), cmds))) {
		t.Error("per-symbol streams")
	}
}

func TestManyProducersPreservePerSymbolOrder(t *testing.T) {
	cmds := fuzzCorpus(3, 20000, 16)
	f, h := Collect(true)
	p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Partitions(4).RingSizes(256, 64, 64).Egress(f).Build()
	mustOk(t, err)
	var wg sync.WaitGroup
	for k := uint32(0); k < 4; k++ {
		var mine []SymCmd
		for _, c := range cmds {
			if c.Sym%4 == k {
				mine = append(mine, c)
			}
		}
		hd := p.Handle()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer hd.Close()
			for i := 0; i < len(mine); i += 7 {
				n := min(7, len(mine)-i)
				if n%2 == 0 {
					hd.PublishBatch(mine[i : i+n])
				} else {
					for _, c := range mine[i : i+n] {
						hd.Publish(c.Sym, c.Cmd)
					}
				}
			}
		}()
	}
	wg.Wait()
	mustOk(t, p.Drain())
	mustOk(t, p.Shutdown())
	if !reflect.DeepEqual(bySymbol(lines(h.Listing())), bySymbol(referenceLines(fuzzCfg(), cmds))) {
		t.Error("per-symbol order")
	}
}

// ---- journals + recovery ------------------------------------------------------------------

func jcfg(dir string, f JournalFormat) JournalConfig {
	j := NewJournalConfig(dir, f)
	j.Fsync = FsyncEveryNPolicy(64)
	return j
}

func TestJournalsSnapshotAndRecoveryRoundTrip(t *testing.T) {
	cfg := fuzzCfg()
	for _, fm := range []JournalFormat{Jsonl, Binary} {
		for _, P := range []uint32{1, 3} {
			dir := scratch(t)
			cmds := fuzzCorpus(21+uint64(P), 5000, 8)
			const cut = 2000
			f, h := Collect(true)
			p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(P).RingSizes(512, 128, 128).
				Journal(jcfg(dir, fm)).Egress(f).Build()
			mustOk(t, err)
			p.PublishBatch(cmds[:cut])
			snap, err := p.Snapshot()
			mustOk(t, err)
			p.PublishBatch(cmds[cut:])
			mustOk(t, p.Shutdown())
			allRef := referenceLines(cfg, cmds)
			prefixLen := len(referenceLines(cfg, cmds[:cut]))
			if snap.Iseq != cut || snap.Body != referenceSnapshot(cfg, cmds, cut) {
				t.Errorf("snapshot cut %d / body", snap.Iseq)
			}
			hdr, recs, err := ReadCmdDir(dir, fm)
			mustOk(t, err)
			if hdr.Partitions != P || !SameBook(hdr.Book, cfg) {
				t.Error("journal header")
			}
			var merged []CmdRec
			for q, rs := range recs {
				for _, r := range rs {
					if HashPartition(r.Sym, P) != uint32(q) {
						t.Errorf("record routed to %d", q)
					}
					merged = append(merged, r)
				}
			}
			sort.Slice(merged, func(i, j int) bool { return merged[i].Iseq < merged[j].Iseq })
			if len(merged) != len(cmds) {
				t.Fatalf("%d records for %d commands", len(merged), len(cmds))
			}
			for i, r := range merged {
				if r.Iseq != uint64(i+1) || r.Sym != cmds[i].Sym || r.Cmd != cmds[i].Cmd {
					t.Fatalf("record %d", i)
				}
			}
			collected := h.Take()
			for q := uint32(0); q < P; q++ {
				got, err := ReadEvtJournal(JournalPath(dir, KindEvt, q, fm), fm)
				mustOk(t, err)
				if !reflect.DeepEqual(got, lines(collected[q])) {
					t.Errorf("evt-%d", q)
				}
			}
			for _, rp := range []uint32{P, 2} {
				m, _ := NewPartitionMap(rp, nil)
				var replayed []string
				rec, err := Recover(NewFifoCore, cfg, m, snap, &JournalSource{dir, fm},
					func(_ uint32, s uint32, seq uint64, ev *matcher.Event) {
						replayed = append(replayed, canonSym(seq, s, ev))
					})
				mustOk(t, err)
				if rec.SnapshotIseq != cut || rec.LastIseq != uint64(len(cmds)) || rec.Replayed != uint64(len(cmds)-cut) {
					t.Errorf("recovery %+v", rec)
				}
				if !reflect.DeepEqual(allRef[prefixLen:], replayed) {
					t.Errorf("recover P=%d → %d", P, rp)
				}
			}
		}
	}
}

func TestRecoveredPipelineResumesAndAppends(t *testing.T) {
	cfg := fuzzCfg()
	dir := scratch(t)
	j := jcfg(dir, Binary)
	cmds := fuzzCorpus(77, 4000, 6)
	p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(2).Journal(j).Build()
	mustOk(t, err)
	p.PublishBatch(cmds[:2500])
	mustOk(t, p.Shutdown())
	m, _ := NewPartitionMap(2, nil)
	rec, err := Recover(NewFifoCore, cfg, m, nil, &JournalSource{dir, Binary}, func(uint32, uint32, uint64, *matcher.Event) {})
	mustOk(t, err)
	if rec.LastIseq != 2500 {
		t.Fatalf("last iseq %d", rec.LastIseq)
	}
	j.Append = true
	f, h := Collect(true)
	p, err = NewBuilder(NewFifoCore).BookConfig(rec.Book).PartitionMap(m).Journal(j).Egress(f).Initial(rec.Initial()).Build()
	mustOk(t, err)
	p.PublishBatch(cmds[2500:])
	mustOk(t, p.Drain())
	snap, err := p.Snapshot()
	mustOk(t, err)
	mustOk(t, p.Shutdown())
	allRef := referenceLines(cfg, cmds)
	prefixLen := len(referenceLines(cfg, cmds[:2500]))
	if !reflect.DeepEqual(bySymbol(lines(h.Listing())), bySymbol(allRef[prefixLen:])) {
		t.Error("resumed stream")
	}
	if snap.Iseq != 4000 || snap.Body != referenceSnapshot(cfg, cmds, 4000) {
		t.Error("iseq resumed / snapshot")
	}
	_, recs, err := ReadCmdDir(dir, Binary)
	mustOk(t, err)
	if total := len(recs[0]) + len(recs[1]); total != 4000 {
		t.Errorf("appended journals hold %d records", total)
	}
}

func TestTornAndCorruptJournalsAreErrors(t *testing.T) {
	cmds := fuzzCorpus(8, 500, 1)
	for _, fm := range []JournalFormat{Jsonl, Binary} {
		dir := scratch(t)
		p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Journal(jcfg(dir, fm)).Build()
		mustOk(t, err)
		p.PublishBatch(cmds)
		mustOk(t, p.Shutdown())
		path := JournalPath(dir, KindCmd, 0, fm)
		good, _ := os.ReadFile(path)
		expectCorrupt := func(b []byte, what string) {
			mustOk(t, os.WriteFile(path, b, 0o644))
			_, _, err := ReadCmdDir(dir, fm)
			var cj *CorruptJournal
			if !errors.As(err, &cj) || !strings.Contains(err.Error(), what) {
				t.Errorf("format %d: want corrupt %q, got %v", fm, what, err)
			}
		}
		expectCorrupt(good[:len(good)-7], "torn")
		back := append([]byte(nil), good...)
		if fm == Binary {
			off := len(back) - CmdRecord
			for i := 0; i < 8; i++ {
				back[off+i] = 0
			}
			back[off] = 1
			seal(back[off:off+CmdRecord], CmdRecordV1) // well-formed, just out of order
		} else {
			back = append(back, `{"cmd":"cancel","symbol":0,"order_id":1,"iseq":3}`+"\n"...)
		}
		expectCorrupt(back, "iseq")
		hdr := append([]byte(nil), good...)
		hdr[2] = '#'
		expectCorrupt(hdr, "")
		mustOk(t, os.WriteFile(path, good, 0o644))
		if _, _, err := ReadCmdDir(dir, fm); err != nil {
			t.Errorf("good journal rejected: %v", err)
		}
	}
}

// ---- controls -------------------------------------------------------------------------------

func TestSnapshotsUnderLoadAreCleanCuts(t *testing.T) {
	cfg := fuzzCfg()
	cmds := fuzzCorpus(31, 30000, 8)
	p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(3).RingSizes(256, 64, 64).Build()
	mustOk(t, err)
	hd := p.Handle()
	done := make(chan struct{})
	go func() {
		for i := 0; i < len(cmds); i += 50 {
			hd.PublishBatch(cmds[i:min(i+50, len(cmds))])
		}
		close(done)
	}()
	var snaps []*Snapshot
	for len(snaps) < 6 {
		s, err := p.Snapshot()
		mustOk(t, err)
		snaps = append(snaps, s)
		time.Sleep(2 * time.Millisecond)
	}
	<-done
	s, err := p.Snapshot()
	mustOk(t, err)
	snaps = append(snaps, s)
	mustOk(t, p.Shutdown())
	for _, s := range snaps {
		if s.Body != referenceSnapshot(cfg, cmds, s.Iseq) {
			t.Errorf("cut at %d", s.Iseq)
		}
	}
	if snaps[len(snaps)-1].Iseq != uint64(len(cmds)) {
		t.Error("final cut")
	}
}

func TestShutdownIsIdempotentAndClosesPublishing(t *testing.T) {
	p, err := NewBuilder(NewFifoCore).Partitions(2).Build()
	mustOk(t, err)
	h := p.Handle()
	p.Publish(1, matcher.NewLimit(1, matcher.Bid, 10, 1, matcher.Gtc))
	mustOk(t, p.Shutdown())
	mustOk(t, p.Shutdown())
	if p.Publish(1, matcher.Cancel(1)) != Closed || h.Publish(1, matcher.Cancel(1)) != Closed ||
		h.TryPublish(1, matcher.Cancel(1)) != Closed {
		t.Error("publishing after shutdown")
	}
	var pe *Error
	if err := p.Drain(); !errors.As(err, &pe) || pe.Kind != ErrClosed {
		t.Errorf("drain after shutdown: %v", err)
	}
}

func TestEveryOkPublishRacingShutdownIsApplied(t *testing.T) {
	for round := 0; round < 5; round++ {
		f, h := Collect(true)
		p, err := NewBuilder(NewNoopCore).Partitions(2).RingSizes(64, 16, 16).Egress(f).Build()
		mustOk(t, err)
		var accepted atomic.Int64
		var wg sync.WaitGroup
		for k := uint32(0); k < 3; k++ {
			hd := p.Handle()
			wg.Add(1)
			go func(k uint32) {
				defer wg.Done()
				for i := uint64(0); hd.Publish(k, matcher.Cancel(i)) == Ok; i++ {
					accepted.Add(1)
				}
			}(k)
		}
		time.Sleep(5 * time.Millisecond)
		mustOk(t, p.Shutdown())
		wg.Wait()
		if got := int64(len(lines(h.Listing()))); got != accepted.Load() {
			t.Errorf("Ok ⇒ applied: %d events for %d accepted", got, accepted.Load())
		}
	}
}

func TestAcksWaitForFsync(t *testing.T) {
	cmds := fuzzCorpus(4, 2000, 6)
	total := int64(len(referenceLines(fuzzCfg(), cmds)))
	for _, noneBeforeShutdown := range []bool{true, false} {
		j := jcfg(scratch(t), Binary)
		if noneBeforeShutdown {
			j.Fsync = FsyncEveryPolicy(time.Hour)
		} else {
			j.Fsync = FsyncEveryNPolicy(1 << 40)
			j.Fsync.Idle = 20 * time.Millisecond
		}
		j.Events = false
		var acked atomic.Int64
		p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Partitions(2).Journal(j).
			Egress(Acks(func(uint32, *EvtMsg) { acked.Add(1) })).Build()
		mustOk(t, err)
		p.PublishBatch(cmds)
		mustOk(t, p.Drain())
		if noneBeforeShutdown {
			time.Sleep(100 * time.Millisecond)
			if acked.Load() != 0 || p.DurableIseq(0) != 0 || p.DurableIseq(1) != 0 {
				t.Error("acked before any fsync")
			}
		} else {
			deadline := time.Now().Add(20 * time.Second)
			for acked.Load() < total && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if max(p.DurableIseq(0), p.DurableIseq(1)) != uint64(len(cmds)) {
				t.Error("durable watermark")
			}
		}
		mustOk(t, p.Shutdown())
		if acked.Load() != total {
			t.Errorf("acked %d of %d", acked.Load(), total)
		}
	}
}

type slowEgress struct{ EgressBase }

func (slowEgress) OnEvent(*EvtMsg) {
	for until := time.Now().Add(20 * time.Microsecond); time.Now().Before(until); {
	}
}

func slow(*EgressCtx) Egress { return slowEgress{} }

func TestTinyRingsAndSlowEgressBlockWithoutLoss(t *testing.T) {
	cmds := fuzzCorpus(17, 3000, 4)
	f, h := Collect(true)
	p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Partitions(2).RingSizes(2, 2, 2).Egress(slow).Egress(f).Build()
	mustOk(t, err)
	for _, c := range cmds {
		if p.Publish(c.Sym, c.Cmd) != Ok {
			t.Fatal("publish")
		}
	}
	mustOk(t, p.Drain())
	mustOk(t, p.Shutdown())
	if !reflect.DeepEqual(bySymbol(lines(h.Listing())), bySymbol(referenceLines(fuzzCfg(), cmds))) {
		t.Error("per-symbol streams")
	}
}

func TestTryPublishShedsAtTheEdgeOnly(t *testing.T) {
	f, h := Collect(true)
	p, err := NewBuilder(NewNoopCore).Partitions(1).RingSizes(4, 2, 2).Egress(slow).Egress(f).Build()
	mustOk(t, err)
	ok, full := 0, 0
	for i := uint64(0); i < 2000; i++ {
		switch p.TryPublish(1, matcher.Cancel(i)) {
		case Ok:
			ok++
		case Full:
			full++
		}
	}
	mustOk(t, p.Drain())
	mustOk(t, p.Shutdown())
	got := lines(h.Listing())
	if full == 0 || len(got) != ok || !dense(got) {
		t.Errorf("ok %d full %d delivered %d", ok, full, len(got))
	}
}

// ---- plugs ----------------------------------------------------------------------------------

func TestNoopCoreSeesEveryCommand(t *testing.T) {
	cmds := fuzzCorpus(5, 5000, 8)
	got := concat(runPipeline(t, NewNoopCore, fuzzCfg(), cmds, 3, true))
	if len(got) != len(cmds) || !dense(got) {
		t.Error("noop core")
	}
}

// panicCore panics on a poison order id.
type panicCore struct{ MatchingCore }

func (c panicCore) Apply(s uint32, cmd *matcher.Command, emit Emitter) {
	if cmd.Kind == matcher.CmdCancel && cmd.OrderID == 666 {
		panic("poison command")
	}
	c.MatchingCore.Apply(s, cmd, emit)
}

func TestFailingCoreFailsThePipelineInsteadOfHanging(t *testing.T) {
	p, err := NewBuilder(func(c matcher.BookConfig) MatchingCore { return panicCore{NewFifoCore(c)} }).Partitions(2).Build()
	mustOk(t, err)
	p.Publish(1, matcher.NewLimit(1, matcher.Bid, 10, 1, matcher.Gtc))
	p.Publish(1, matcher.Cancel(666))
	var pe *Error
	if err := p.Drain(); !errors.As(err, &pe) || pe.Kind != ErrFailed || !strings.Contains(err.Error(), "engine") {
		t.Errorf("drain: %v", err)
	}
	if err := p.Shutdown(); !errors.As(err, &pe) || pe.Kind != ErrFailed {
		t.Errorf("shutdown: %v", err)
	}
}

// ---- allocation -----------------------------------------------------------------------------

// TestSteadyStateAllocations bounds the hot path's allocations: the rings and
// journals never allocate, and the engine adds nothing beyond matcher-go's
// own per-event cost.
func TestSteadyStateAllocations(t *testing.T) {
	cmds := fuzzCorpus(9, 50_000, 8)
	j := jcfg(scratch(t), Binary)
	j.Events = false
	p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Partitions(2).Journal(j).Build()
	mustOk(t, err)
	p.PublishBatch(cmds)
	mustOk(t, p.Drain())
	var m0, m1 runtimeMem
	m0.read()
	p.PublishBatch(cmds)
	mustOk(t, p.Drain())
	m1.read()
	mustOk(t, p.Shutdown())
	perCmd := float64(m1.mallocs-m0.mallocs) / float64(len(cmds))
	t.Logf("%.2f allocations per command", perCmd)
	if perCmd > 4 {
		t.Errorf("%.2f allocations per command", perCmd)
	}
}

// ---- 1.2: checksums, repair, checkpoints -------------------------------------------------------

func TestCRC32CMatchesTheSpecCheckValue(t *testing.T) {
	if CRC32C([]byte("123456789")) != 0xE3069283 {
		t.Fatal("check value")
	}
}

func TestChecksumsCatchFlippedBitsAnywhere(t *testing.T) {
	dir := scratch(t)
	p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Journal(jcfg(dir, Binary)).Build()
	mustOk(t, err)
	p.PublishBatch(fuzzCorpus(9, 300, 2))
	mustOk(t, p.Shutdown())
	path := JournalPath(dir, KindCmd, 0, Binary)
	bad, _ := os.ReadFile(path)
	bad[Header+100*CmdRecord+20] ^= 0x10
	mustOk(t, os.WriteFile(path, bad, 0o644))
	if _, _, err := ReadCmdDir(dir, Binary); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("strict: %v", err)
	}
	if _, err := RepairDir(dir, Binary); err == nil {
		t.Error("mid-file damage is not repairable")
	}
}

func TestRepairCutsOnlyATornTail(t *testing.T) {
	cmds := fuzzCorpus(10, 400, 3)
	for _, fm := range []JournalFormat{Jsonl, Binary} {
		dir := scratch(t)
		p, err := NewBuilder(NewFifoCore).BookConfig(fuzzCfg()).Journal(jcfg(dir, fm)).Build()
		mustOk(t, err)
		p.PublishBatch(cmds)
		mustOk(t, p.Shutdown())
		path := JournalPath(dir, KindCmd, 0, fm)
		good, _ := os.ReadFile(path)
		_, all, err := ReadCmdDir(dir, fm)
		mustOk(t, err)
		full := all[0]
		mustOk(t, os.WriteFile(path, good[:len(good)-5], 0o644))
		if _, _, err := ReadCmdDir(dir, fm); err == nil {
			t.Error("strict rejects a torn tail")
		}
		if fixed, err := RepairDir(dir, fm); err != nil || len(fixed) != 1 {
			t.Fatalf("repair: %v %v", fixed, err)
		}
		_, got, err := ReadCmdDir(dir, fm)
		mustOk(t, err)
		if !reflect.DeepEqual(got[0], full[:len(full)-1]) {
			t.Error("a prefix survives")
		}
		if fm == Binary {
			zeroed := append([]byte(nil), good...)
			for i := len(zeroed) - CmdRecord; i < len(zeroed); i++ {
				zeroed[i] = 0
			}
			mustOk(t, os.WriteFile(path, zeroed, 0o644))
			if fixed, err := RepairDir(dir, fm); err != nil || len(fixed) != 1 {
				t.Errorf("a complete record that never reached the disk: %v %v", fixed, err)
			}
		}
		if fixed, err := RepairDir(dir, fm); err != nil || len(fixed) != 0 {
			t.Errorf("a clean file is left alone: %v %v", fixed, err)
		}
	}
}

func TestCheckpointsRotateSegmentsAndBoundRecovery(t *testing.T) {
	cfg := fuzzCfg()
	cmds := fuzzCorpus(12, 3000, 6)
	for _, fm := range []JournalFormat{Jsonl, Binary} {
		dir := scratch(t)
		f, h := Collect(true)
		p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(3).Journal(jcfg(dir, fm)).Egress(f).Build()
		mustOk(t, err)
		p.PublishBatch(cmds[:1000])
		c1, err := p.Checkpoint()
		mustOk(t, err)
		p.PublishBatch(cmds[1000:2200])
		c2, err := p.Checkpoint()
		mustOk(t, err)
		p.PublishBatch(cmds[2200:])
		mustOk(t, p.Shutdown())
		if c1.Iseq != 1000 || c2.Iseq != 2200 {
			t.Errorf("cuts %d %d", c1.Iseq, c2.Iseq)
		}
		cps := ListCheckpoints(dir, true, ^uint64(0))
		if len(cps) != 1 || cps[0].Cut != 2200 {
			t.Fatalf("checkpoints %v", cps)
		}
		for _, k := range []JournalKind{KindCmd, KindEvt} {
			segs := ListSegments(dir, k, fm)
			if len(segs) != 3 {
				t.Errorf("segments %v", segs)
			}
			for _, s := range segs {
				if s.Start != 2200 {
					t.Errorf("stale segment %s", s.Path)
				}
			}
		}
		if c2.Body != referenceSnapshot(cfg, cmds, 2200) {
			t.Error("checkpoint body")
		}
		snap, err := ReadSnapshot(cps[0].Path)
		mustOk(t, err)
		m, _ := NewPartitionMap(3, nil)
		var replayed []string
		rec, err := Recover(NewFifoCore, cfg, m, snap, &JournalSource{dir, fm},
			func(_ uint32, s uint32, q uint64, ev *matcher.Event) { replayed = append(replayed, canonSym(q, s, ev)) })
		mustOk(t, err)
		all := referenceLines(cfg, cmds)
		prefix := len(referenceLines(cfg, cmds[:2200]))
		if rec.Replayed != uint64(len(cmds)-2200) || !reflect.DeepEqual(replayed, all[prefix:]) {
			t.Error("recover from the checkpoint")
		}
		var evts []string
		for q := uint32(0); q < 3; q++ {
			e, err := ReadEvtPartition(dir, fm, q)
			mustOk(t, err)
			evts = append(evts, e...)
		}
		want := append([]string(nil), all[prefix:]...)
		sort.Strings(evts)
		sort.Strings(want)
		if !reflect.DeepEqual(evts, want) {
			t.Error("event segments hold the tail")
		}
		if len(lines(h.Listing())) != len(all) {
			t.Error("listing")
		}
	}
}

func TestAppendContinuesTheLastSegmentAfterACheckpoint(t *testing.T) {
	cfg := fuzzCfg()
	cmds := fuzzCorpus(14, 2000, 4)
	dir := scratch(t)
	j := jcfg(dir, Binary)
	p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(2).Journal(j).Build()
	mustOk(t, err)
	p.PublishBatch(cmds[:800])
	_, err = p.Checkpoint()
	mustOk(t, err)
	p.PublishBatch(cmds[800:1200])
	mustOk(t, p.Shutdown())
	m, _ := NewPartitionMap(2, nil)
	snap, err := ReadSnapshot(ListCheckpoints(dir, true, ^uint64(0))[0].Path)
	mustOk(t, err)
	rec, err := Recover(NewFifoCore, cfg, m, snap, &JournalSource{dir, Binary}, func(uint32, uint32, uint64, *matcher.Event) {})
	mustOk(t, err)
	if rec.LastIseq != 1200 {
		t.Fatalf("last iseq %d", rec.LastIseq)
	}
	j.Append = true
	p, err = NewBuilder(NewFifoCore).BookConfig(rec.Book).PartitionMap(m).Journal(j).Initial(rec.Initial()).Build()
	mustOk(t, err)
	p.PublishBatch(cmds[1200:])
	s, err := p.Snapshot()
	mustOk(t, err)
	mustOk(t, p.Shutdown())
	if s.Iseq != 2000 || s.Body != referenceSnapshot(cfg, cmds, 2000) {
		t.Error("resumed state")
	}
	_, recs, err := ReadCmdDir(dir, Binary)
	mustOk(t, err)
	if n := len(recs[0]) + len(recs[1]); n != 1200 {
		t.Errorf("%d records; 800 should be checkpointed away", n)
	}
}

func TestAutomaticCheckpointsKeepTheDirectoryRecoverable(t *testing.T) {
	cfg := fuzzCfg()
	cmds := fuzzCorpus(15, 20000, 8)
	dir := scratch(t)
	p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(3).Journal(jcfg(dir, Binary)).
		CheckpointEvery(20 * time.Millisecond).Build()
	mustOk(t, err)
	for i := 0; i < len(cmds); i += 500 {
		p.PublishBatch(cmds[i:min(i+500, len(cmds))])
		time.Sleep(3 * time.Millisecond)
	}
	last, err := p.Snapshot()
	mustOk(t, err)
	mustOk(t, p.Shutdown())
	cps := ListCheckpoints(dir, true, ^uint64(0))
	if len(cps) != 1 || cps[0].Cut == 0 {
		t.Fatalf("checkpoints %v", cps)
	}
	snap, err := ReadSnapshot(cps[0].Path)
	mustOk(t, err)
	m, _ := NewPartitionMap(3, nil)
	rec, err := Recover(NewFifoCore, cfg, m, snap, &JournalSource{dir, Binary}, func(uint32, uint32, uint64, *matcher.Event) {})
	mustOk(t, err)
	if rec.LastIseq != uint64(len(cmds)) {
		t.Fatalf("last iseq %d", rec.LastIseq)
	}
	p2, err := NewBuilder(NewFifoCore).BookConfig(rec.Book).PartitionMap(m).Initial(rec.Initial()).Build()
	mustOk(t, err)
	s, err := p2.Snapshot()
	mustOk(t, err)
	mustOk(t, p2.Shutdown())
	if s.Body != last.Body {
		t.Error("the directory recovers the final state")
	}
	if _, err := NewBuilder(NewFifoCore).CheckpointEvery(5 * time.Millisecond).Build(); err == nil {
		t.Error("needs journals")
	}
}

func TestStatsCountCommandsEventsAndFsyncs(t *testing.T) {
	cfg := fuzzCfg()
	cmds := fuzzCorpus(16, 5000, 6)
	dir := scratch(t)
	p, err := NewBuilder(NewFifoCore).BookConfig(cfg).Partitions(3).Journal(jcfg(dir, Binary)).Build()
	mustOk(t, err)
	p.PublishBatch(cmds)
	mustOk(t, p.Drain())
	lagging := func() bool {
		for _, s := range p.Stats().Partitions {
			if s.Fsyncs == 0 || s.DurableIseq < s.FlushedIseq {
				return true
			}
		}
		return false
	}
	for deadline := time.Now().Add(10 * time.Second); lagging() && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	st := p.Stats()
	if st.IngressDepth != 0 || len(st.Partitions) != 3 {
		t.Fatalf("stats %+v", st)
	}
	var commands, events, durable uint64
	for _, s := range st.Partitions {
		if s.InboxDepth != 0 || s.OutboxDepth != 0 || s.Fsyncs == 0 || s.FsyncNsMax == 0 {
			t.Errorf("partition %+v", s)
		}
		commands += s.Commands
		events += s.Events
		durable = max(durable, s.DurableIseq)
	}
	if commands != uint64(len(cmds)) || events != uint64(len(referenceLines(cfg, cmds))) || durable != uint64(len(cmds)) {
		t.Errorf("commands %d events %d durable %d", commands, events, durable)
	}
	prom := st.ToPrometheus()
	if !strings.Contains(prom, "# TYPE orderer_commands_total counter") || !strings.Contains(prom, `orderer_inbox_depth{partition="2"} 0`) {
		t.Error(prom)
	}
	mustOk(t, p.Shutdown())
}
