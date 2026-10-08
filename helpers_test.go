// Test helpers: vectors, a seeded adversarial generator, single-Engine
// references, per-symbol views, pipeline runs.
package orderer

import (
	"os"
	"strings"
	"testing"

	"github.com/abhijitkrm/orderer-go/matcher"
)

func slurp(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func lines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// rng is xorshift64* (the tools' generator family).
type rng struct{ x uint64 }

func (r *rng) next() uint64 {
	r.x ^= r.x >> 12
	r.x ^= r.x << 25
	r.x ^= r.x >> 27
	return r.x * 0x2545F4914F6CDD1D
}
func (r *rng) below(n uint64) uint64 { return r.next() % n }

func fuzzCfg() matcher.BookConfig {
	return matcher.BookConfig{PriceMin: 1, PriceMax: 200, MaxOrders: 4096, Index: matcher.IndexLadder}
}

// fuzzCorpus is an adversarial engine stream: crossing prices, small id
// space, every TIF, markets, ~5% malformed.
func fuzzCorpus(seed uint64, n int, symbols uint64) []SymCmd {
	r := rng{(seed * 0x9E3779B97F4A7C15) | 1}
	tifs := []matcher.Tif{matcher.Gtc, matcher.Gtc, matcher.Ioc, matcher.Fok, matcher.PostOnly}
	bad := []int64{0, 201, -5}
	out := make([]SymCmd, 0, n)
	for i := 0; i < n; i++ {
		sym, id := uint32(r.below(symbols)), r.below(256)
		price := 90 + int64(r.below(21))
		if r.below(20) == 0 {
			price = bad[r.below(3)]
		}
		qty := r.below(50) + 1
		if r.below(25) == 0 {
			qty = 0
		}
		var c matcher.Command
		switch k := r.below(10); {
		case k < 5:
			side := matcher.Bid
			if r.below(2) == 1 {
				side = matcher.Ask
			}
			if r.below(8) == 0 {
				c = matcher.NewMarket(id, side, qty)
			} else {
				c = matcher.NewLimit(id, side, price, qty, tifs[r.below(5)])
			}
		case k < 8:
			c = matcher.Cancel(id)
		default:
			c = matcher.Replace(id, price, qty)
		}
		out = append(out, SymCmd{sym, c})
	}
	return out
}

func canonSym(seq uint64, sym uint32, ev *matcher.Event) string {
	var b []byte
	ev.WriteCanonicalSym(seq, sym, &b)
	return string(b)
}

func referenceLines(cfg matcher.BookConfig, cmds []SymCmd) []string {
	e := matcher.NewEngine(cfg)
	var out []string
	for _, c := range cmds {
		e.SubmitTagged(c.Sym, c.Cmd, func(s uint32, seq uint64, ev *matcher.Event) { out = append(out, canonSym(seq, s, ev)) })
	}
	return out
}

func referenceSnapshot(cfg matcher.BookConfig, cmds []SymCmd, n uint64) string {
	e := matcher.NewEngine(cfg)
	sink := &matcher.NullSink{}
	for _, c := range cmds[:n] {
		e.Submit(c.Sym, c.Cmd, sink)
	}
	var sb strings.Builder
	matcher.WriteEngine(e, &sb)
	return sb.String()
}

func bySymbol(ls []string) map[uint64][]string {
	m := map[uint64][]string{}
	for _, l := range ls {
		s, _ := U64(l, "symbol")
		m[s] = append(m[s], l)
	}
	return m
}

func dense(ls []string) bool {
	next := map[uint64]uint64{}
	for _, l := range ls {
		s, _ := U64(l, "symbol")
		q, _ := U64(l, "seq")
		next[s]++
		if q != next[s] {
			return false
		}
	}
	return true
}

func mustOk(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// runPipeline returns per-partition canonical lines from a pipeline run.
func runPipeline(t *testing.T, core CoreFactory, cfg matcher.BookConfig, cmds []SymCmd, P uint32, tagged bool) [][]string {
	f, h := Collect(tagged)
	p, err := NewBuilder(core).BookConfig(cfg).Partitions(P).RingSizes(1<<10, 1<<8, 1<<8).Egress(f).Build()
	mustOk(t, err)
	p.PublishBatch(cmds)
	mustOk(t, p.Drain())
	mustOk(t, p.Shutdown())
	var out [][]string
	for _, b := range h.Take() {
		out = append(out, lines(b))
	}
	return out
}

func concat(v [][]string) []string {
	var out []string
	for _, p := range v {
		out = append(out, p...)
	}
	return out
}

func scratch(t *testing.T) string { return t.TempDir() }

