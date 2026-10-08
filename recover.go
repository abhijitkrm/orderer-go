package orderer

// Recovery (spec/JOURNAL.md §4–5): restore a snapshot into per-partition
// cores under the restoring pipeline's routing, then replay command journals
// merged by iseq.

import (
	"fmt"
	"os"
	"strings"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// RecoverError is a bad snapshot or an inconsistent recovery input.
type RecoverError struct{ Msg string }

func (e *RecoverError) Error() string { return e.Msg }

// ReadSnapshot reads a snapshot body + its .meta sidecar (missing sidecar ⇒
// cut 0, e.g. a matcher snapshot).
func ReadSnapshot(path string) (*Snapshot, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, &RecoverError{"snapshot: " + path + ": cannot read"}
	}
	s := &Snapshot{Body: string(body), Partitions: 1}
	mp := MetaPath(path)
	if _, err := os.Stat(mp); err == nil {
		t, err := os.ReadFile(mp)
		if err != nil {
			return nil, &RecoverError{"snapshot: " + mp + ": bad sidecar"}
		}
		line, _, _ := strings.Cut(string(t), "\n")
		iseq, ok := U64(line, "iseq")
		if f, _ := Get(line, "format"); f != "orderer-meta/1" || !ok {
			return nil, &RecoverError{"snapshot: " + mp + ": bad sidecar"}
		}
		s.Iseq = iseq
		if p, ok := U64(line, "partitions"); ok {
			s.Partitions = uint32(p)
		}
	}
	return s, nil
}

// Restore makes empty cores, or cores restored from snap (its header overrides book).
func Restore(core CoreFactory, book matcher.BookConfig, pmap *PartitionMap, snap *Snapshot) (matcher.BookConfig, []MatchingCore, error) {
	var cores []MatchingCore
	if snap == nil {
		for i := uint32(0); i < pmap.Partitions(); i++ {
			cores = append(cores, core(book))
		}
		return book, cores, nil
	}
	ps, err := ParseSnapshot(snap.Body)
	if err != nil {
		return book, nil, &RecoverError{"snapshot: " + err.Error()}
	}
	for i := uint32(0); i < pmap.Partitions(); i++ {
		cores = append(cores, core(ps.Cfg))
	}
	seen := map[uint32]bool{}
	for _, b := range ps.Books {
		if seen[b.Symbol] {
			return book, nil, &RecoverError{fmt.Sprintf("snapshot: book %d appears twice", b.Symbol)}
		}
		seen[b.Symbol] = true
		if err := cores[pmap.Partition(b.Symbol)].RestoreBook(b.Symbol, b.Seq, b.Orders); err != nil {
			return book, nil, &RecoverError{"snapshot: " + err.Error()}
		}
	}
	return ps.Cfg, cores, nil
}

// Recovery is recovered state, ready to start a pipeline from.
type Recovery struct {
	Book                             matcher.BookConfig
	Cores                            []MatchingCore
	SnapshotIseq, LastIseq, Replayed uint64
}

// Initial converts the recovery into a pipeline's starting state.
func (r *Recovery) Initial() Initial { return Initial{Cores: r.Cores, NextIseq: r.LastIseq + 1} }

// JournalSource locates command journals for recovery.
type JournalSource struct {
	Dir    string
	Format JournalFormat
}

// Recover restores the snapshot (optional), then replays every command
// journal in the source (optional) after the snapshot's cut, in iseq order:
// emit(partition, symbol, seq, event). The book config comes from the
// snapshot, else the journals, else book.
func Recover(core CoreFactory, book matcher.BookConfig, pmap *PartitionMap, snap *Snapshot, journal *JournalSource,
	emit func(p uint32, sym uint32, seq uint64, ev *matcher.Event)) (*Recovery, error) {
	var jh JournalHeader
	var parts [][]CmdRec
	if journal != nil {
		var err error
		if jh, parts, err = ReadCmdDir(journal.Dir, journal.Format); err != nil {
			return nil, err
		}
		if snap == nil {
			book = jh.Book
		}
	}
	cfg, cores, err := Restore(core, book, pmap, snap)
	if err != nil {
		return nil, err
	}
	if journal != nil && !SameBook(jh.Book, cfg) {
		return nil, &RecoverError{"snapshot: snapshot and journal book configs differ"}
	}
	var cut uint64
	if snap != nil {
		cut = snap.Iseq
	}
	recs, err := MergeJournals(parts, cut)
	if err != nil {
		return nil, err
	}
	r := &Recovery{Book: cfg, Cores: cores, SnapshotIseq: cut, LastIseq: cut, Replayed: uint64(len(recs))}
	if len(recs) > 0 {
		r.LastIseq = max(cut, recs[len(recs)-1].Iseq)
	}
	for i := range recs {
		rec := &recs[i]
		p := pmap.Partition(rec.Sym)
		cores[p].Apply(rec.Sym, &rec.Cmd, EmitFunc(func(s uint32, seq uint64, ev *matcher.Event) { emit(p, s, seq, ev) }))
	}
	return r, nil
}
