// orderernode: one replica of a replicated cluster, as a process. Same command
// line and client protocol as orderer-rust's orderer-node and orderer-cpp's
// orderer_node, so replicas of all three languages can share a cluster.
//
//	orderernode --id N --members 0=HOST:PORT,1=HOST:PORT,... --client HOST:PORT
//	            --dir DIR [--compact-every N] [--digest-every N]
//	            [--fsync on|off] [--audit] [--sm pipeline|recorder] [--partitions P]
//
// The state machine is the matching pipeline (default) or the language-neutral
// RecorderSm, wrapped in DedupSm. --audit appends (log index, first order id)
// for every applied batch to DIR/applied.log (truncated at start), as the
// other nodes do. ORDERER_KEY in the environment sets the pre-shared cluster key.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/abhijitkrm/orderer-go/cluster"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "orderernode: "+format+"\n", a...)
	os.Exit(2)
}

// audited records (index, first order id) of every applied batch, then delegates.
type audited struct {
	inner cluster.StateMachine
	log   *os.File
}

func (a *audited) Apply(index consensus.Index, e consensus.Entry) {
	if a.log != nil {
		if cmds, err := cluster.DecodeBatch(e.Data); err == nil && len(cmds) > 0 {
			var rec [16]byte
			putLE(rec[:8], uint64(index))
			putLE(rec[8:], cmds[0].Cmd.OrderID)
			a.log.Write(rec[:]) // straight to the OS: survives kill -9 of this process
		}
	}
	a.inner.Apply(index, e)
}

func putLE(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}

func (a *audited) Snapshot(i consensus.Index) []byte               { return a.inner.Snapshot(i) }
func (a *audited) Restore(i consensus.Index, d []byte) error       { return a.inner.Restore(i, d) }
func (a *audited) Digest(i consensus.Index) (cluster.Digest, bool) { return a.inner.Digest(i) }
func (a *audited) DeepDigest(i consensus.Index) (cluster.Digest, bool) {
	return a.inner.DeepDigest(i)
}

func main() {
	id := flag.Int("id", -1, "node id")
	members := flag.String("members", "", "ID=HOST:PORT,...")
	client := flag.String("client", "", "client HOST:PORT")
	dir := flag.String("dir", "", "state directory")
	compact := flag.Uint64("compact-every", 0, "snapshot and compact every N entries")
	digest := flag.Uint64("digest-every", 0, "exchange state digests every N entries")
	fsync := flag.String("fsync", "on", "on|off")
	audit := flag.Bool("audit", false, "write applied.log")
	smKind := flag.String("sm", "pipeline", "pipeline|recorder")
	partitions := flag.Uint("partitions", 2, "pipeline partitions")
	flag.Parse()
	if *id < 0 || *members == "" || *client == "" || *dir == "" {
		die("--id, --members, --client and --dir are required")
	}
	if *smKind != "pipeline" && *smKind != "recorder" {
		die("--sm %s: want pipeline or recorder", *smKind)
	}
	ms, err := cluster.ParseMembers(*members)
	if err != nil {
		die("--members: %v", err)
	}
	cfg := cluster.DefaultConfig(consensus.NodeID(*id), ms, filepath.Join(*dir, "state"))
	cfg.CompactEvery, cfg.DigestEvery, cfg.NoFsync = *compact, *digest, *fsync != "on"
	cfg.MaxPending = 1 << 16
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		die("%v", err)
	}
	var key []byte
	if k := os.Getenv("ORDERER_KEY"); k != "" {
		key = []byte(k)
	}
	cfg.AuthKey = key

	var inner cluster.StateMachine = &cluster.RecorderSm{}
	if *smKind == "pipeline" {
		p, err := cluster.NewCountingPipelineSm(uint32(*partitions), matcher.DefaultConfig())
		if err != nil {
			die("pipeline: %v", err)
		}
		inner = p
	}
	a := &audited{inner: inner}
	if *audit {
		if a.log, err = os.Create(filepath.Join(*dir, "applied.log")); err != nil {
			die("%v", err)
		}
	}
	r, err := cluster.StartReplica(cfg, cluster.NewDedupSm(a), nil)
	if err != nil {
		die("start: %v", err)
	}
	if _, err := cluster.NewClientServer(*client, r, key); err != nil {
		die("client port: %v", err)
	}
	fmt.Printf("orderernode %s ready (client port %s)\n", strconv.Itoa(*id), *client)
	for {
		time.Sleep(time.Hour)
	}
}
