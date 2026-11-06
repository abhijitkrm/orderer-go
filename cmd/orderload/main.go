// orderload: load generator, cluster inspector and failover verifier (the Go
// port of orderer-rust's orderer-load; same modes, flags and output).
//
//	orderload run    --clients A,B,C --secs S --acked FILE [--maybe FILE] [--batch N] [--threads T]
//	orderload status --clients A,B,C
//	orderload settle --clients A,B,C [--timeout S]
//	orderload verify --acked FILE [--maybe FILE] --applied F1,F2,...
//
// run writes the id of every acknowledged batch to the acked file as soon as
// it is acknowledged, and ids with an unknown outcome to the maybe file.
// verify then proves that nothing acknowledged was lost, nothing was applied
// twice or invented, and every replica applied the same sequence. ORDERER_KEY
// in the environment sets the pre-shared cluster key.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/cluster"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "orderload: "+format+"\n", a...)
	os.Exit(2)
}

func keyFromEnv() []byte {
	if k := os.Getenv("ORDERER_KEY"); k != "" {
		return []byte(k)
	}
	return nil
}

func addrs(s string) []cluster.Member {
	var out []cluster.Member
	for i, a := range strings.Split(s, ",") {
		if !strings.Contains(a, ":") {
			die("bad address %s", a)
		}
		out = append(out, cluster.Member{ID: consensus.NodeID(i), Addr: a})
	}
	return out
}

func readIDs(path string) []uint64 {
	b, _ := os.ReadFile(path)
	var out []uint64
	for _, l := range strings.Split(string(b), "\n") {
		if v, err := strconv.ParseUint(strings.TrimSpace(l), 10, 64); err == nil {
			out = append(out, v)
		}
	}
	return out
}

type applied struct{ index, id uint64 }

func readApplied(path string) []applied {
	b, err := os.ReadFile(path)
	if err != nil {
		die("%s: %v", path, err)
	}
	var out []applied
	for ; len(b) >= 16; b = b[16:] {
		out = append(out, applied{binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:])})
	}
	return out
}

func main() {
	if len(os.Args) < 2 {
		die("mode: run|status|settle|verify")
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	clients := fs.String("clients", "", "client ports, comma separated")
	secs := fs.Uint64("secs", 10, "run length")
	acked := fs.String("acked", "", "file of acknowledged batch ids")
	maybe := fs.String("maybe", "", "file of unknown-outcome batch ids")
	appliedF := fs.String("applied", "", "applied.log files, comma separated")
	batch := fs.Int("batch", 50, "commands per batch")
	threads := fs.Int("threads", 2, "client threads")
	timeout := fs.Uint64("timeout", 60, "settle timeout, seconds")
	fs.Parse(os.Args[2:])
	switch mode {
	case "run":
		run(addrs(*clients), *secs, *acked, *maybe, *batch, *threads)
	case "status":
		status(addrs(*clients))
	case "settle":
		settle(addrs(*clients), *timeout)
	case "verify":
		verify(*acked, *maybe, *appliedF)
	default:
		die("unknown mode %s", mode)
	}
}

type idFile struct {
	mu sync.Mutex
	f  *os.File
}

func openIDs(p string) *idFile {
	if p == "" {
		return nil
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		die("%s: %v", p, err)
	}
	return &idFile{f: f}
}

func (f *idFile) write(id uint64) {
	f.mu.Lock()
	fmt.Fprintf(f.f, "%d\n", id)
	f.mu.Unlock()
}

func run(clients []cluster.Member, secs uint64, acked, maybe string, batch, threads int) {
	if acked == "" {
		die("--acked is required")
	}
	ackedF, maybeF := openIDs(acked), openIDs(maybe)
	end := time.Now().Add(time.Duration(secs) * time.Second)
	var mu sync.Mutex
	var ok, unknown, failed uint64
	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			// One session per thread: retries are exactly-once.
			c := cluster.NewClient(clients).WithSession(uint64(t) + 1)
			defer c.Close()
			if k := keyFromEnv(); k != nil {
				c.WithKey(k)
			}
			x := 0x9E3779B97F4A7C15 ^ (uint64(t) + 1)
			var seq uint64
			for time.Now().Before(end) {
				seq++
				// The batch id doubles as its first order's id; the rest are
				// derived from it so every order id is unique.
				id := ((uint64(t)+1)<<40 | seq) << 12
				cmds := make([]orderer.SymCmd, batch)
				for k := range cmds {
					x ^= x << 13
					x ^= x >> 7
					x ^= x << 17
					side := matcher.Bid
					if x&1 != 0 {
						side = matcher.Ask
					}
					cmds[k] = orderer.SymCmd{Sym: uint32(x % 16),
						Cmd: matcher.NewLimit(id|uint64(k), side, 10_000+int64((x>>8)%8)-4, 1+(x>>20)%10, matcher.Gtc)}
				}
				r := c.ProposeSession(cmds)
				mu.Lock()
				switch r.Kind {
				case cluster.ClientOK:
					ackedF.write(id)
					ok++
				case cluster.ClientUnknown:
					if maybeF != nil {
						maybeF.write(id)
					}
					unknown++
				default:
					failed++
				}
				mu.Unlock()
			}
		}(t)
	}
	wg.Wait()
	fmt.Printf("load finished: %d acknowledged, %d unknown, %d not submitted (%d commands each)\n", ok, unknown, failed, batch)
}

func status(clients []cluster.Member) {
	for _, a := range clients {
		c := cluster.Connect(a.Addr, keyFromEnv())
		if c == nil {
			fmt.Printf("%s: unreachable (connect failed)\n", a.Addr)
			continue
		}
		if s, ok := c.Status(); ok {
			fmt.Printf("%s: %s", a.Addr, s)
		} else {
			fmt.Printf("%s: unreachable (no status)\n", a.Addr)
		}
		c.Close()
	}
}

func field(s, k string) string {
	for _, kv := range strings.Fields(s) {
		if v, ok := strings.CutPrefix(kv, k+"="); ok {
			return v
		}
	}
	return ""
}

type hashRow struct {
	idx consensus.Index
	d   cluster.Digest
}

// settle waits until every node has applied everything committed and all agree.
func settle(clients []cluster.Member, timeout uint64) {
	end := time.Now().Add(time.Duration(timeout) * time.Second)
	for {
		var stats []string
		var hashes []hashRow
		ok := true
		for _, a := range clients {
			c := cluster.Connect(a.Addr, keyFromEnv())
			if c == nil {
				ok = false
				continue
			}
			s, sok := c.Status()
			i, d, hok := c.Hash()
			c.Close()
			if !sok || !hok {
				ok = false
				continue
			}
			stats, hashes = append(stats, s), append(hashes, hashRow{i, d})
		}
		if ok && len(stats) == len(clients) {
			for _, s := range stats {
				if h := field(s, "halted"); h != "" && h != "-" {
					fmt.Println("FAIL: a node halted")
					os.Exit(1)
				}
			}
			agree := true
			for i := range stats {
				if field(stats[i], "applied") != field(stats[0], "applied") || field(stats[i], "commit") != field(stats[0], "commit") ||
					field(stats[i], "applied") != field(stats[i], "commit") || hashes[i] != hashes[0] {
					agree = false
				}
			}
			if agree {
				fmt.Printf("settled: all %d nodes at index %d, state digest %x\n", len(clients), hashes[0].idx, hashes[0].d)
				return
			}
		}
		if time.Now().After(end) {
			fmt.Printf("FAIL: nodes did not settle within %ds\n", timeout)
			status(clients)
			os.Exit(1)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func verify(acked, maybe, appliedPaths string) {
	ackedIDs := readIDs(acked)
	maybeSet := map[uint64]bool{}
	for _, id := range readIDs(maybe) {
		maybeSet[id] = true
	}
	ackedSet := map[uint64]bool{}
	for _, id := range ackedIDs {
		ackedSet[id] = true
	}
	if len(ackedSet) != len(ackedIDs) {
		fmt.Println("FAIL: the load generator acknowledged an id twice")
		os.Exit(1)
	}
	var first []applied
	failures := 0
	for i, path := range strings.Split(appliedPaths, ",") {
		log := readApplied(path)
		set := map[uint64]bool{}
		for _, l := range log {
			set[l.id] = true
		}
		var bad []string
		if len(set) != len(log) {
			bad = append(bad, "a batch was applied twice")
		}
		if !sort.SliceIsSorted(log, func(a, b int) bool { return log[a].index < log[b].index }) || hasEqualIndex(log) {
			bad = append(bad, "log indexes are not increasing")
		}
		missing := 0
		for a := range ackedSet {
			if !set[a] {
				missing++
			}
		}
		if missing > 0 {
			bad = append(bad, fmt.Sprintf("%d acknowledged batches are missing", missing))
		}
		invented := 0
		for id := range set {
			if !ackedSet[id] && !maybeSet[id] {
				invented++
			}
		}
		if invented > 0 {
			bad = append(bad, fmt.Sprintf("%d applied batches were never submitted", invented))
		}
		if i == 0 {
			first = log
		} else if !equalApplied(first, log) {
			bad = append(bad, "applied sequence differs from the first node's")
		}
		if len(bad) == 0 {
			fmt.Printf("%s: ok (%d batches applied, %d acknowledged all present)\n", path, len(log), len(ackedSet))
		} else {
			failures++
			fmt.Printf("%s: FAIL: %s\n", path, strings.Join(bad, "; "))
		}
	}
	if failures > 0 {
		os.Exit(1)
	}
	fmt.Println("PASS: every acknowledged batch survived on every replica, in the same order, exactly once")
}

func hasEqualIndex(l []applied) bool {
	for i := 1; i < len(l); i++ {
		if l[i].index <= l[i-1].index {
			return true
		}
	}
	return false
}

func equalApplied(a, b []applied) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
