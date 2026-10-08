// orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//
//	orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
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
	usage := "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]"
	a := orderer.ParseArgs(os.Args[1:], usage, append(orderer.CommonValued, "--snap"), orderer.CommonFlags)
	if len(a.Positional) != 1 {
		orderer.Die(usage)
	}
	c := orderer.LoadCorpus(a.Positional[0])
	com := orderer.CommonArgs(a)
	snapPath, snap := a.Get("--snap")
	listing, s := orderer.RunCorpus(c, com, true, snap)
	orderer.Print(listing)
	if snap {
		if err := s.Write(snapPath); err != nil {
			orderer.Fail(err.Error())
		}
	}
}
