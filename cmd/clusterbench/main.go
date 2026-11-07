// clusterbench: throughput of the replicated path, in one process over
// loopback TCP (the Go counterpart of orderer-rust's clusterbench, same flags
// and same output).
//
//	clusterbench [--nodes N] [--groups G] [--sm noop|pipeline] [--partitions P]
//	             [--batch CMDS] [--producers K] [--window W]
//	             [--cmds TOTAL] [--fsync on|off] [--dir PATH]
//
// G independent replicated groups of N nodes each (symbols sharded between
// them). For every group, K producers keep W batches of CMDS commands in
// flight against its leader. Everything shares one machine: use the numbers as
// a floor for what dedicated hardware per replica would do.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/cluster"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

type noop struct{}

func (noop) Apply(consensus.Index, consensus.Entry)        {}
func (noop) Snapshot(consensus.Index) []byte               { return nil }
func (noop) Restore(consensus.Index, []byte) error         { return nil }
func (noop) Digest(consensus.Index) (cluster.Digest, bool) { return cluster.Digest{}, false }
func (noop) DeepDigest(consensus.Index) (cluster.Digest, bool) {
	return cluster.Digest{}, false
}

var workdir string

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	if workdir != "" {
		os.RemoveAll(workdir)
	}
	os.Exit(2)
}

// groupOf is the splitmix64-based group assignment, same as orderer-rust's ShardMap.
func groupOf(sym, groups uint32) uint32 {
	z := uint64(sym) + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	return uint32(z % uint64(groups))
}

func main() {
	nodes := flag.Int("nodes", 3, "nodes per group")
	groups := flag.Int("groups", 1, "independent replica groups")
	smKind := flag.String("sm", "noop", "noop|pipeline")
	partitions := flag.Uint("partitions", 2, "pipeline partitions")
	batch := flag.Int("batch", 1000, "commands per proposal")
	producers := flag.Int("producers", 2, "producers per group")
	window := flag.Int("window", 8, "in-flight proposals per producer")
	total := flag.Uint64("cmds", 20_000_000, "total commands")
	fsync := flag.String("fsync", "on", "on|off")
	dir := flag.String("dir", "", "directory for WALs")
	profile := flag.String("cpuprofile", "", "write a CPU profile of the measured run to this file")
	flag.Parse()
	if *dir == "" {
		*dir = filepath.Join(os.TempDir(), fmt.Sprintf("clusterbench-go-%d", os.Getpid()))
	}
	os.RemoveAll(*dir)
	workdir = *dir
	defer os.RemoveAll(workdir)

	var all [][]*cluster.Replica
	var sms []*cluster.PipelineSm
	for g := 0; g < *groups; g++ {
		var ls []net.Listener
		var members []cluster.Member
		for i := 0; i < *nodes; i++ {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				die("listen: %v", err)
			}
			ls = append(ls, l)
			members = append(members, cluster.Member{ID: consensus.NodeID(i), Addr: l.Addr().String()})
		}
		for _, l := range ls {
			l.Close()
		}
		var reps []*cluster.Replica
		for i := 0; i < *nodes; i++ {
			cfg := cluster.DefaultConfig(consensus.NodeID(i), members, filepath.Join(*dir, fmt.Sprintf("g%dn%d", g, i)))
			cfg.ClusterID = 1000 + uint64(g)
			cfg.Consensus = consensus.Config{ElectionTicks: 30, HeartbeatTicks: 3, MaxBatch: 512, PreVote: true}
			cfg.NoFsync = *fsync != "on"
			cfg.MaxPending = 1 << 20
			cfg.ProposeTimeout = 60 * time.Second
			var sm cluster.StateMachine
			switch *smKind {
			case "noop":
				sm = noop{}
			case "pipeline":
				p, err := cluster.NewCountingPipelineSm(uint32(*partitions), matcher.DefaultConfig())
				if err != nil {
					die("pipeline: %v", err)
				}
				sms = append(sms, p)
				sm = p
			default:
				die("--sm %s", *smKind)
			}
			r, err := cluster.StartReplica(cfg, sm, nil)
			if err != nil {
				die("start: %v", err)
			}
			reps = append(reps, r)
		}
		all = append(all, reps)
	}

	leaders := make([]int, *groups)
	end := time.Now().Add(20 * time.Second)
	for g, rs := range all {
		for leaders[g] = -1; leaders[g] < 0; {
			for i, r := range rs {
				if r.Status().Role == consensus.Leader {
					leaders[g] = i
				}
			}
			if leaders[g] < 0 {
				if time.Now().After(end) {
					die("no leader")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			die("%v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	perProducer := *total / uint64(*groups**producers)
	var acked, latNs, batches atomic.Uint64
	t0 := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < *groups; g++ {
		leader := all[g][leaders[g]]
		var syms []uint32 // symbols this group owns
		for s := uint32(0); s < 4096 && len(syms) < 64; s++ {
			if groupOf(s, uint32(*groups)) == uint32(g) {
				syms = append(syms, s)
			}
		}
		for p := 0; p < *producers; p++ {
			wg.Add(1)
			go func(g, p int) {
				defer wg.Done()
				nextID := uint64(g*64+p+1) << 36
				x := 0x9E3779B97F4A7C15 ^ uint64(g*64+p)
				type inflight struct {
					t  time.Time
					ch <-chan cluster.ProposeResult
				}
				var q []inflight
				var sent uint64
				cmds := make([]orderer.SymCmd, *batch)
				for sent < perProducer || len(q) > 0 {
					for sent < perProducer && len(q) < *window {
						for i := range cmds {
							x ^= x << 13
							x ^= x >> 7
							x ^= x << 17
							nextID++
							side := matcher.Bid
							if x&1 != 0 {
								side = matcher.Ask
							}
							cmds[i] = orderer.SymCmd{Sym: syms[x%uint64(len(syms))],
								Cmd: matcher.NewLimit(nextID, side, 10_000+int64((x>>8)%8)-4, 1+(x>>20)%10, matcher.Gtc)}
						}
						payload := cluster.EncodeBatch(cmds, make([]byte, 0, 5+*batch*32), nil)
						q = append(q, inflight{time.Now(), leader.ProposeAsync(payload)})
						sent += uint64(*batch)
					}
					if len(q) == 0 {
						break
					}
					head := q[0]
					q = q[1:]
					r := <-head.ch
					switch r.Kind {
					case cluster.ProposeOK:
						acked.Add(uint64(*batch))
						latNs.Add(uint64(time.Since(head.t)))
						batches.Add(1)
					case cluster.ProposeBusy:
						time.Sleep(time.Millisecond)
					default:
						die("proposal failed: %+v", r)
					}
				}
			}(g, p)
		}
	}
	wg.Wait()
	secs := time.Since(t0).Seconds()
	done := acked.Load()
	nb := max(batches.Load(), 1)
	// Every node of every group must apply what its group committed.
	deadline := time.Now().Add(180 * time.Second)
	for g, rs := range all {
		want := rs[leaders[g]].Status().Commit
		for {
			caught := true
			for _, r := range rs {
				if r.Status().Applied < want {
					caught = false
				}
			}
			if caught {
				break
			}
			if time.Now().After(deadline) {
				die("followers did not catch up")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	appliedSecs := time.Since(t0).Seconds()
	on := "off"
	if *fsync == "on" {
		on = "on"
	}
	fmt.Printf("groups=%d nodes=%d sm=%s batch=%d producers=%d/group window=%d fsync=%s: "+
		"%.2fM cmds/s acked (%.2fM per group), %.2fM applied everywhere, %.0f MB/s log payload, "+
		"mean batch latency %.2f ms, %d cmds in %.2fs\n",
		*groups, *nodes, *smKind, *batch, *producers, *window, on,
		float64(done)/secs/1e6, float64(done)/secs/1e6/float64(*groups), float64(done)/appliedSecs/1e6,
		float64(done)*32/1e6/secs, float64(latNs.Load())/float64(nb)/1e6, done, secs)
	for _, rs := range all {
		for _, r := range rs {
			r.Crash()
		}
	}
	for _, p := range sms {
		p.Close()
	}
}
