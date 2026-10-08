// A1 — every vendored matcher vector (and orderer's regress vectors), every
// index mode, through the live pipeline: P=1 byte-identical to .evt; P=4
// per-symbol identical (engine) / unchanged (single-book), routing honored.
package orderer

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/abhijitkrm/orderer-go/matcher"
)

func runVector(t *testing.T, cmd, evt string) int {
	text := slurp(t, cmd)
	c, err := ParseCorpus(text, cmd)
	mustOk(t, err)
	expected := lines(slurp(t, evt))[1:]
	hdr, _, _ := strings.Cut(text, "\n")
	modes := []matcher.IndexKind{matcher.IndexLadder}
	switch ix, _ := Get(hdr, "index"); ix {
	case "both":
		modes = []matcher.IndexKind{matcher.IndexLadder, matcher.IndexTree}
	case "tree":
		modes = []matcher.IndexKind{matcher.IndexTree}
	}
	for _, k := range modes {
		cfg := c.Book
		cfg.Index = k
		if got := concat(runPipeline(t, NewFifoCore, cfg, c.Cmds, 1, c.Engine)); !reflect.DeepEqual(got, expected) {
			t.Errorf("%s P=1 index=%d", cmd, k)
		}
		parts := runPipeline(t, NewFifoCore, cfg, c.Cmds, 4, c.Engine)
		if c.Engine {
			if !reflect.DeepEqual(bySymbol(concat(parts)), bySymbol(expected)) {
				t.Errorf("%s P=4", cmd)
			}
			for p, ls := range parts {
				for _, l := range ls {
					s, _ := U64(l, "symbol")
					if HashPartition(uint32(s), 4) != uint32(p) {
						t.Errorf("routing %s", l)
					}
				}
			}
		} else if !reflect.DeepEqual(concat(parts), expected) {
			t.Errorf("%s P=4 single-book", cmd)
		}
	}
	return len(modes)
}

func TestEveryVectorThroughPipeline(t *testing.T) {
	runs := 0
	mf := slurp(t, "vectors/matcher/manifest.json")
	for pos := 0; ; {
		i := strings.Index(mf[pos:], `"file"`)
		if i < 0 {
			break
		}
		pos += i
		a := pos + strings.Index(mf[pos:], ":") + 1
		a += strings.Index(mf[a:], `"`) + 1
		b := a + strings.Index(mf[a:], `"`)
		name := mf[a:b]
		pos = b
		runs += runVector(t, "vectors/matcher/"+name+".cmd.jsonl", "vectors/matcher/"+name+".evt.jsonl")
	}
	regress, _ := filepath.Glob("vectors/regress/*.cmd.jsonl")
	for _, p := range regress {
		runs += runVector(t, p, strings.TrimSuffix(p, ".cmd.jsonl")+".evt.jsonl")
	}
	if runs < 80 {
		t.Errorf("only %d vector runs", runs)
	}
	t.Logf("%d vector runs", runs)
}

func TestRoutingVectors(t *testing.T) {
	n := 0
	for _, l := range lines(slurp(t, "vectors/routing/hash.jsonl")) {
		if strings.Contains(l, `"format"`) {
			continue
		}
		s, _ := U64(l, "symbol")
		P, _ := U64(l, "partitions")
		want, _ := U64(l, "partition")
		m, err := NewPartitionMap(uint32(P), nil)
		mustOk(t, err)
		if HashPartition(uint32(s), uint32(P)) != uint32(want) || m.Partition(uint32(s)) != uint32(want) {
			t.Errorf("%s", l)
		}
		n++
	}
	if n <= 500 {
		t.Errorf("only %d routing vectors", n)
	}
	m, err := ParsePartitionTable(slurp(t, "vectors/routing/table.map.jsonl"), 4)
	mustOk(t, err)
	for _, l := range lines(slurp(t, "vectors/routing/table.expect.jsonl")) {
		if strings.Contains(l, `"format"`) {
			continue
		}
		s, _ := U64(l, "symbol")
		want, _ := U64(l, "partition")
		if m.Partition(uint32(s)) != uint32(want) {
			t.Errorf("%s", l)
		}
	}
	if _, err := ParsePartitionTable(slurp(t, "vectors/routing/table.map.jsonl"), 3); err == nil {
		t.Error("P mismatch must fail")
	}
}
