// orderrecover — spec/HARNESS.md §4.3 (mirrors matcherrecover).
//
//	orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]
//	orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]
//
// Tail form: restore, submit every tail line without "format" through a
// pipeline, print the replayed events. Journal form: recover from journals
// (after the optional snapshot's cut; by default the directory's newest
// checkpoint). Malformed/corrupt input exits 2; --repair first truncates torn
// tails (spec/JOURNAL.md §5.1).
package main

import (
	"fmt"
	"os"
	"strings"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/matcher"
)

func tailForm(snapPath, tailPath string, pmap *orderer.PartitionMap) {
	snap, err := orderer.ReadSnapshot(snapPath)
	if err != nil {
		orderer.Die(err.Error())
	}
	book, cores, err := orderer.Restore(orderer.NewFifoCore, matcher.DefaultConfig(), pmap, snap)
	if err != nil {
		orderer.Die(err.Error())
	}
	var cmds []orderer.SymCmd
	for i, line := range strings.Split(orderer.ReadText(tailPath), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" || strings.Contains(line, `"format"`) {
			continue
		}
		cmd, ok := orderer.ParseCommand(line)
		sym := uint64(0)
		symOk := true
		if s, has := orderer.Get(line, "symbol"); has {
			sym, symOk = orderer.ParseU64(s)
		}
		if !ok || !symOk || sym > 0xFFFFFFFF {
			orderer.Die(fmt.Sprintf("%s:%d: malformed journal line: %s", tailPath, i+1, line))
		}
		cmds = append(cmds, orderer.SymCmd{Sym: uint32(sym), Cmd: cmd})
	}
	f, events := orderer.Collect(true)
	p, err := orderer.NewBuilder(orderer.NewFifoCore).BookConfig(book).PartitionMap(pmap).Egress(f).
		Initial(orderer.Initial{Cores: cores, NextIseq: snap.Iseq + 1}).Build()
	if err != nil {
		orderer.Fail(err.Error())
	}
	if p.PublishBatch(cmds) != orderer.Ok {
		orderer.Fail("pipeline closed")
	}
	if err := p.Drain(); err != nil {
		orderer.Fail(err.Error())
	}
	if err := p.Shutdown(); err != nil {
		orderer.Fail(err.Error())
	}
	orderer.Print(events.Listing())
}

func journalForm(dir string, snapPath string, hasSnap bool, f orderer.JournalFormat, repair bool, pmap *orderer.PartitionMap) {
	parts := make([][]byte, pmap.Partitions())
	if repair {
		fixed, err := orderer.RepairDir(dir, f)
		if err != nil {
			orderer.Die(err.Error())
		}
		for _, r := range fixed {
			fmt.Fprintf(os.Stderr, "repaired %s %d\n", r.Path, r.Bytes)
		}
	}
	if !hasSnap {
		if cps := orderer.ListCheckpoints(dir, true, ^uint64(0)); len(cps) > 0 {
			snapPath, hasSnap = cps[len(cps)-1].Path, true
		}
	}
	var snap *orderer.Snapshot
	if hasSnap {
		var err error
		if snap, err = orderer.ReadSnapshot(snapPath); err != nil {
			orderer.Die(err.Error())
		}
	}
	_, err := orderer.Recover(orderer.NewFifoCore, matcher.DefaultConfig(), pmap, snap,
		&orderer.JournalSource{Dir: dir, Format: f}, func(p, s uint32, seq uint64, ev *matcher.Event) {
			ev.WriteCanonicalSym(seq, s, &parts[p])
			parts[p] = append(parts[p], '\n')
		})
	if err != nil {
		orderer.Die(err.Error())
	}
	var out strings.Builder
	for _, p := range parts {
		out.Write(p)
	}
	orderer.Print(out.String())
}

func main() {
	usage := "orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]\n" +
		"       orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]"
	a := orderer.ParseArgs(os.Args[1:], usage, []string{"--partitions", "--partition-map", "--journal-dir", "--snap"}, []string{"--binary", "--repair"})
	pmap := orderer.PartitionMapArg(a)
	dir, hasDir := a.Get("--journal-dir")
	snapPath, hasSnap := a.Get("--snap")
	switch {
	case hasDir && len(a.Positional) == 0:
		f := orderer.Jsonl
		if a.Flag("--binary") {
			f = orderer.Binary
		}
		journalForm(dir, snapPath, hasSnap, f, a.Flag("--repair"), pmap)
	case !hasDir && len(a.Positional) == 2 && !hasSnap && !a.Flag("--binary") && !a.Flag("--repair"):
		tailForm(a.Positional[0], a.Positional[1], pmap)
	default:
		orderer.Die(usage)
	}
}
