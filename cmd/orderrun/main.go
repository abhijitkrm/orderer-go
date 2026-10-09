// orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//
//	orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
//	         [--checkpoint-every K] [--durable]
//
// Runs the file through a pipeline (one producer, file order), drains, prints
// every event symbol-tagged, grouped by partition. --snap then writes the
// merged snapshot to PATH and its cut to PATH.meta.
package main

import (
	"os"

	orderer "github.com/abhijitkrm/orderer-go"
)

func main() {
	usage := "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH] " +
		"[--checkpoint-every K] [--durable]"
	valued := append(append([]string{}, orderer.CommonValued...), "--snap", "--checkpoint-every")
	flags := append(append([]string{}, orderer.CommonFlags...), "--durable")
	a := orderer.ParseArgs(os.Args[1:], usage, valued, flags)
	if len(a.Positional) != 1 {
		orderer.Die(usage)
	}
	c := orderer.LoadCorpus(a.Positional[0])
	com := orderer.CommonArgs(a)
	snapPath, snap := a.Get("--snap")
	opts := orderer.RunOpts{Snapshot: snap, Durable: a.Flag("--durable")}
	if _, ok := a.Get("--checkpoint-every"); ok {
		opts.CheckpointEvery = int(a.Num("--checkpoint-every", 0, 1<<31-1))
		if opts.CheckpointEvery == 0 {
			orderer.Die("--checkpoint-every: K must be at least 1")
		}
	}
	listing, s := orderer.RunCorpusOpts(c, com, true, opts)
	orderer.Print(listing)
	if snap {
		if err := s.Write(snapPath); err != nil {
			orderer.Fail(err.Error())
		}
	}
}
