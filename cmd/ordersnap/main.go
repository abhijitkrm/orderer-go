// ordersnap — spec/HARNESS.md §4.4 (mirrors matchersnap).
//
//	ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//
// Runs the file, drains, prints the merged matcher-snap/1 snapshot.
package main

import (
	"os"

	orderer "github.com/abhijitkrm/orderer-go"
)

func main() {
	usage := "ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]"
	a := orderer.ParseArgs(os.Args[1:], usage, orderer.CommonValued, orderer.CommonFlags)
	if len(a.Positional) != 1 {
		orderer.Die(usage)
	}
	c := orderer.LoadCorpus(a.Positional[0])
	_, s := orderer.RunCorpus(c, orderer.CommonArgs(a), true, true)
	orderer.Print(s.Body)
}
