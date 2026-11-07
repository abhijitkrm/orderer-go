package cluster

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// The event-chain digest (spec/REPLICATION.md 13.16): conformance vectors shared
// with the other languages, sensitivity, continuity across a snapshot restore,
// cost, and the documented limit.

func loadCorpus(t testing.TB, name string) *orderer.Corpus {
	t.Helper()
	path := filepath.Join("..", "vectors", "pipeline", name+".cmd.jsonl")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := orderer.ParseCorpus(string(b), path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// chainsOf runs cmds through a fresh pipeline and returns its chains.
func chainsOf(t testing.TB, book matcher.BookConfig, cmds []orderer.SymCmd, parts uint32) []uint64 {
	t.Helper()
	d := orderer.NewEventDigest(parts, nil)
	p, err := orderer.NewBuilder(orderer.NewFifoCore).BookConfig(book).Partitions(parts).Egress(d.Factory()).Build()
	if err != nil {
		t.Fatal(err)
	}
	if p.PublishBatch(cmds) != orderer.Ok {
		t.Fatal("publish")
	}
	if err := p.Drain(); err != nil {
		t.Fatal(err)
	}
	v := d.Values()
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	return v
}

// combineDigest is the replica-level digest over tag, commands applied, partitions and chains.
func combineDigest(iseq uint64, chains []uint64) Digest {
	b := append([]byte("orderer-evdigest/1"), putU64(nil, iseq)...)
	b = putU32(b, uint32(len(chains)))
	for _, c := range chains {
		b = putU64(b, c)
	}
	return sha256.Sum256(b)
}

func vectorText(iseq uint64, chains []uint64) string {
	hs := make([]string, len(chains))
	for i, c := range chains {
		hs[i] = fmt.Sprintf("%016x", c)
	}
	return fmt.Sprintf("orderer-evdigest/1\npartitions %d\ncommands %d\nchains %s\ndigest %x\n",
		len(chains), iseq, strings.Join(hs, " "), combineDigest(iseq, chains))
}

func TestChainsMatchTheSharedVectors(t *testing.T) {
	checked := 0
	for _, name := range []string{"multisymbol", "fuzz_s11"} {
		c := loadCorpus(t, name)
		for _, p := range []uint32{1, 2, 4} {
			got := vectorText(uint64(len(c.Cmds)), chainsOf(t, c.Book, c.Cmds, p))
			file := fmt.Sprintf("%s.P%d.txt", name, p)
			want, err := os.ReadFile(filepath.Join("..", "vectors", "replication-digest", file))
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("%s:\n got %q\nwant %q", file, got, want)
			}
			checked++
		}
	}
	if checked != 6 {
		t.Fatalf("checked %d vectors", checked)
	}
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestChainsAreDeterministicAndSensitiveToEveryField(t *testing.T) {
	var base []orderer.SymCmd
	for i := uint64(1); i <= 40; i++ {
		side := matcher.Ask
		if i%2 == 0 {
			side = matcher.Bid
		}
		base = append(base, orderer.SymCmd{Sym: uint32(i % 5), Cmd: matcher.NewLimit(i, side, 100+int64(i%3), 1+i%4, matcher.Gtc)})
	}
	// Sweeping orders at the end trade against whatever rests, which is what
	// exposes resting prices and quantities in the event stream.
	for _, s := range append([]uint32{0, 1, 2, 3, 4}, 7, 8, 9, 10, 11) {
		base = append(base,
			orderer.SymCmd{Sym: s, Cmd: matcher.NewLimit(9000+uint64(s), matcher.Bid, 1000, 10000, matcher.Ioc)},
			orderer.SymCmd{Sym: s, Cmd: matcher.NewLimit(9100+uint64(s), matcher.Ask, 0, 10000, matcher.Ioc)})
	}
	book := matcher.DefaultConfig()
	ref := chainsOf(t, book, base, 2)
	if !equalU64(ref, chainsOf(t, book, base, 2)) {
		t.Fatal("not deterministic")
	}
	tweak := func(f func(*orderer.SymCmd)) []uint64 {
		v := append([]orderer.SymCmd(nil), base...)
		f(&v[0])
		return chainsOf(t, book, v, 2)
	}
	for what, f := range map[string]func(*orderer.SymCmd){
		"qty":      func(c *orderer.SymCmd) { c.Cmd.Qty++ },
		"price":    func(c *orderer.SymCmd) { c.Cmd.Price++ },
		"order id": func(c *orderer.SymCmd) { c.Cmd.OrderID += 1000 },
		"symbol":   func(c *orderer.SymCmd) { c.Sym += 7 },
	} {
		if equalU64(ref, tweak(f)) {
			t.Errorf("a different %s did not change the chain", what)
		}
	}
	if equalU64(ref, chainsOf(t, book, base[:39], 2)) {
		t.Error("dropping a command did not change the chain")
	}
}

func TestARestoredPipelineContinuesTheSameChain(t *testing.T) {
	c := loadCorpus(t, "fuzz_s11")
	const p = 2
	whole := chainsOf(t, c.Book, c.Cmds, p)

	d1 := orderer.NewEventDigest(p, nil)
	first, err := orderer.NewBuilder(orderer.NewFifoCore).BookConfig(c.Book).Partitions(p).Egress(d1.Factory()).Build()
	if err != nil {
		t.Fatal(err)
	}
	first.PublishBatch(c.Cmds[:1500])
	if err := first.Drain(); err != nil {
		t.Fatal(err)
	}
	snap, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	mid := d1.Values()
	first.Shutdown()

	pm, _ := orderer.NewPartitionMap(p, nil)
	_, cores, err := orderer.Restore(orderer.NewFifoCore, c.Book, pm, snap)
	if err != nil {
		t.Fatal(err)
	}
	d2 := orderer.NewEventDigest(p, mid)
	second, err := orderer.NewBuilder(orderer.NewFifoCore).BookConfig(c.Book).PartitionMap(pm).Egress(d2.Factory()).
		Initial(orderer.Initial{Cores: cores, NextIseq: snap.Iseq + 1}).Build()
	if err != nil {
		t.Fatal(err)
	}
	second.PublishBatch(c.Cmds[1500:])
	if err := second.Drain(); err != nil {
		t.Fatal(err)
	}
	if got := d2.Values(); !equalU64(got, whole) {
		t.Fatalf("the chain broke across a snapshot: %v vs %v", got, whole)
	}
	second.Shutdown()
}

func applyCmds(sm *PipelineSm, idx consensus.Index, cmds []orderer.SymCmd) {
	sm.Apply(idx, consensus.Entry{Term: 1, Data: EncodeBatch(cmds, nil, nil)})
}

func TestTheDigestCostsABarrierNotASnapshot(t *testing.T) {
	sm, err := NewCountingPipelineSm(2, matcher.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()
	// 300k resting orders over 8 symbols: bids and asks that never cross.
	cmds := make([]orderer.SymCmd, 0, 300_000)
	for i := uint64(1); i <= 300_000; i++ {
		side, price := matcher.Bid, 1000-int64(i%500)
		if i%2 == 1 {
			side, price = matcher.Ask, 2000+int64(i%500)
		}
		cmds = append(cmds, orderer.SymCmd{Sym: uint32(i % 8), Cmd: matcher.NewLimit(i, side, price, 1, matcher.Gtc)})
	}
	applyCmds(sm, 1, cmds)
	t0 := time.Now()
	cheap, ok1 := sm.Digest(1)
	cheapT := time.Since(t0)
	t0 = time.Now()
	deep, ok2 := sm.DeepDigest(1)
	deepT := time.Since(t0)
	t.Logf("digest %v, deep digest %v (300k resting orders)", cheapT, deepT)
	if !ok1 || !ok2 || cheap == deep {
		t.Fatal("digests missing or equal")
	}
	if cheapT >= deepT/10 {
		t.Errorf("digest %v is not much cheaper than deep %v", cheapT, deepT)
	}
	if cheapT > 100*time.Millisecond {
		t.Errorf("digest took %v", cheapT)
	}
}

func TestARestingDifferenceIsInvisibleToTheChainUntilItTrades(t *testing.T) {
	// The limit of an event-stream digest, stated as a test: books that differ
	// only in a resting order's price emit the same events until something trades
	// against it. The deep digest sees it at once.
	run := func(price int64) (Digest, Digest) {
		sm, err := NewCountingPipelineSm(1, matcher.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Close()
		applyCmds(sm, 1, []orderer.SymCmd{{Sym: 1, Cmd: matcher.NewLimit(1, matcher.Ask, price, 5, matcher.Gtc)}})
		c, _ := sm.Digest(1)
		d, _ := sm.DeepDigest(1)
		return c, d
	}
	ca, da := run(100)
	cb, db := run(101)
	if ca != cb {
		t.Error("the chain saw a price that has not traded")
	}
	if da == db {
		t.Error("the deep digest missed a different resting price")
	}
}

func TestSnapshotsCarryTheChainsAndAreValidated(t *testing.T) {
	c := loadCorpus(t, "multisymbol")
	a, err := NewCountingPipelineSm(2, c.Book)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	applyCmds(a, 1, c.Cmds)
	snap := a.Snapshot(1)
	if string(snap[:4]) != "PSM2" {
		t.Fatalf("magic %q", snap[:4])
	}
	b, _ := NewCountingPipelineSm(2, c.Book)
	defer b.Close()
	if err := b.Restore(1, snap); err != nil {
		t.Fatal(err)
	}
	da, _ := a.Digest(1)
	db, _ := b.Digest(1)
	deepA, _ := a.DeepDigest(1)
	deepB, _ := b.DeepDigest(1)
	if da != db || deepA != deepB {
		t.Fatal("a restored replica's digests differ")
	}
	// Other layouts and partition counts are refused.
	if err := b.Restore(1, append([]byte("PSM1"), snap[4:]...)); err == nil {
		t.Error("accepted a snapshot with the wrong magic")
	}
	one, _ := NewCountingPipelineSm(1, c.Book)
	defer one.Close()
	if err := one.Restore(1, snap); err == nil {
		t.Error("accepted a snapshot with a different partition count")
	}
}
