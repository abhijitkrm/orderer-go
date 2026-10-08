// ordererfuzz — spec/HARNESS.md §4.2 (mirrors matcherfuzz).
//
//	ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//
// Like orderrun, but symbol-tagged only for engine files. The CHECKED build
// (scripts/build-harness.sh) is compiled with the race detector.
package main

import (
	"os"

	orderer "github.com/abhijitkrm/orderer-go"
)

func main() {
	usage := "ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]"
	a := orderer.ParseArgs(os.Args[1:], usage, orderer.CommonValued, orderer.CommonFlags)
	if len(a.Positional) != 1 {
		orderer.Die(usage)
	}
	c := orderer.LoadCorpus(a.Positional[0])
	listing, _ := orderer.RunCorpus(c, orderer.CommonArgs(a), c.Engine, false)
	orderer.Print(listing)
}
