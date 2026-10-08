// orderbench — spec/BENCH.md protocol.
//
//	orderbench <prefix> --mode core [--tag NAME]
//	orderbench <prefix> --mode pipe --partitions P [--producers N]
//	           [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME]
//
// orderer-go tuning flags (listed in the config column when set):
//
//	--core fifo|noop  --waits relaxed|low  --batch N  --ingress N --inbox N --outbox N
//	--events on|off   --baseline OPS (core untimed ops/s, for eff)
//
// Core rows also report alloc_b_op= (bytes allocated per command, untimed pass).
package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/matcher"
)

type row struct {
	ops        int
	wall       time.Duration
	lat        []uint64
	untimed    float64
	allocPerOp float64
}

type target interface {
	apply(sym uint32, c matcher.Command)
}

type engineT struct {
	e    *matcher.Engine
	sink *matcher.NullSink
}

func (t engineT) apply(s uint32, c matcher.Command) { t.e.Submit(s, c, t.sink) }

type bookT struct {
	b    *matcher.OrderBook
	sink *matcher.NullSink
}

func (t bookT) apply(_ uint32, c matcher.Command) { t.b.Apply(c, t.sink) }

func applyAll(t target, cmds []orderer.SymCmd) {
	for i := range cmds {
		t.apply(cmds[i].Sym, cmds[i].Cmd)
	}
}

func coreMode(setup, run *orderer.Corpus) row {
	r := row{ops: len(run.Cmds)}
	sink := &matcher.NullSink{}
	mk := func() target {
		if setup.Engine {
			return engineT{matcher.NewEngine(setup.Book), sink}
		}
		return bookT{matcher.NewOrderBook(setup.Book), sink}
	}
	{ // warmup: setup + 10% of run
		t := mk()
		applyAll(t, setup.Cmds)
		applyAll(t, run.Cmds[:len(run.Cmds)/10])
	}
	{ // timed per op (matcher protocol)
		t := mk()
		applyAll(t, setup.Cmds)
		runtime.GC()
		r.lat = make([]uint64, len(run.Cmds))
		wall := time.Now()
		for i := range run.Cmds {
			t0 := time.Now()
			t.apply(run.Cmds[i].Sym, run.Cmds[i].Cmd)
			r.lat[i] = uint64(time.Since(t0))
		}
		r.wall = time.Since(wall)
	}
	{ // untimed: the scaling gate's denominator (spec/BENCH.md 1.1)
		t := mk()
		applyAll(t, setup.Cmds)
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		wall := time.Now()
		applyAll(t, run.Cmds)
		d := time.Since(wall)
		runtime.ReadMemStats(&m1)
		r.untimed = float64(len(run.Cmds)) / d.Seconds()
		r.allocPerOp = float64(m1.TotalAlloc-m0.TotalAlloc) / float64(len(run.Cmds))
	}
	if sink.Acc == 42 {
		fmt.Fprint(os.Stderr, "")
	}
	return r
}

type pipeOpts struct {
	partitions             uint32
	producers, batch       int
	journal                *orderer.JournalConfig
	waits                  orderer.Waits
	ingress, inbox, outbox int
}

func build(core orderer.CoreFactory, book matcher.BookConfig, o *pipeOpts, m orderer.EgressFactory) *orderer.Pipeline {
	b := orderer.NewBuilder(core).BookConfig(book).Partitions(o.partitions).Waits(o.waits).
		RingSizes(o.ingress, o.inbox, o.outbox)
	if o.journal != nil {
		b.Journal(*o.journal)
	}
	if m != nil {
		b.Egress(m)
	}
	p, err := b.Build()
	if err != nil {
		orderer.Fail(err.Error())
	}
	return p
}

func must(err error) {
	if err != nil {
		orderer.Fail(err.Error())
	}
}

func pipeMode(core orderer.CoreFactory, setup, run *orderer.Corpus, o *pipeOpts) row {
	{ // warmup on a throwaway pipeline
		p := build(core, setup.Book, o, nil)
		p.PublishBatch(setup.Cmds)
		p.PublishBatch(run.Cmds[:len(run.Cmds)/10])
		must(p.Drain())
		must(p.Shutdown())
	}
	mf, results := orderer.Metrics(len(run.Cmds) + 1024)
	p := build(core, setup.Book, o, mf)
	p.PublishBatch(setup.Cmds)
	must(p.Drain())
	streams := make([][]orderer.SymCmd, o.producers)
	for _, sc := range run.Cmds {
		k := int(sc.Sym) % o.producers
		streams[k] = append(streams[k], sc)
	}
	runtime.GC()
	p.SetTimestamps(true)
	var ready, start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for _, s := range streams {
		s := s
		h := p.Handle()
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			runtime.LockOSThread()
			ready.Done()
			start.Wait()
			for i := 0; i < len(s); i += o.batch {
				h.PublishBatch(s[i:min(i+o.batch, len(s))])
			}
			h.Close()
		}()
	}
	ready.Wait()
	wall := time.Now()
	start.Done()
	done.Wait()
	must(p.Drain())
	r := row{ops: len(run.Cmds), wall: time.Since(wall)}
	p.SetTimestamps(false)
	must(p.Shutdown())
	for _, m := range results.Results() {
		r.lat = append(r.lat, m.Latencies...)
	}
	return r
}

func pct(v []uint64, p float64) uint64 {
	if len(v) == 0 {
		return 0
	}
	i := int(math.Ceil(float64(len(v)-1) * p))
	return v[min(i, len(v)-1)]
}

func cpu() string {
	out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
	if s := strings.TrimSpace(string(out)); err == nil && s != "" {
		return s
	}
	return "unknown cpu"
}

func main() {
	usage := "orderbench <prefix> --mode core|pipe [--partitions P] [--producers N] [--journal binary|jsonl|off] " +
		"[--journal-dir DIR] [--fsync N] [--tag NAME] [--core fifo|noop] [--waits relaxed|low] [--batch N] " +
		"[--ingress N] [--inbox N] [--outbox N] [--events on|off] [--baseline OPS]"
	a := orderer.ParseArgs(os.Args[1:], usage, []string{"--mode", "--partitions", "--producers", "--journal",
		"--journal-dir", "--fsync", "--tag", "--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox",
		"--events", "--baseline"}, nil)
	if len(a.Positional) != 1 {
		orderer.Die(usage)
	}
	prefix := a.Positional[0]
	tag, ok := a.Get("--tag")
	if !ok {
		tag = filepath.Base(prefix)
	}
	setup := orderer.LoadCorpus(prefix + ".setup.cmd.jsonl")
	run := orderer.LoadCorpus(prefix + ".run.cmd.jsonl")
	get := func(k, def string) string {
		if v, ok := a.Get(k); ok {
			return v
		}
		return def
	}
	mode := get("--mode", "core")
	var config []string
	var r row
	P, prod := "-", "-"
	switch mode {
	case "core":
		r = coreMode(setup, run)
	case "pipe":
		o := &pipeOpts{partitions: uint32(a.Num("--partitions", 1, orderer.MaxPartitions)),
			producers: int(max(a.Num("--producers", 1, 1024), 1)), batch: int(max(a.Num("--batch", 64, 1<<20), 1)),
			waits: orderer.LowLatencyWaits(), ingress: 1 << 14, inbox: 1 << 12, outbox: 1 << 13}
		o.ingress = int(a.Num("--ingress", uint64(o.ingress), 1<<30))
		o.inbox = int(a.Num("--inbox", uint64(o.inbox), 1<<30))
		o.outbox = int(a.Num("--outbox", uint64(o.outbox), 1<<30))
		fsync := a.Num("--fsync", 1024, math.MaxUint64)
		jm := get("--journal", "binary")
		tmp := filepath.Join(os.TempDir(), "orderbench-go-"+strconv.Itoa(os.Getpid()))
		switch jm {
		case "binary", "jsonl":
			f := orderer.Binary
			if jm == "jsonl" {
				f = orderer.Jsonl
			}
			j := orderer.NewJournalConfig(get("--journal-dir", tmp), f)
			j.Fsync = orderer.FsyncNeverPolicy()
			if fsync > 0 {
				j.Fsync = orderer.FsyncEveryNPolicy(fsync)
			}
			j.Events = get("--events", "off") == "on"
			o.journal = &j
		case "off":
		default:
			orderer.Die("--journal: unknown mode " + jm)
		}
		switch get("--waits", "low") {
		case "relaxed":
			o.waits = orderer.RelaxedWaits()
		case "low":
		default:
			orderer.Die("--waits: unknown " + get("--waits", ""))
		}
		config = append(config, fmt.Sprintf("journal=%s fsync=%d", jm, fsync))
		for _, k := range []string{"--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events"} {
			if v, ok := a.Get(k); ok {
				config = append(config, k[2:]+"="+v)
			}
		}
		switch get("--core", "fifo") {
		case "fifo":
			r = pipeMode(orderer.NewFifoCore, setup, run, o)
		case "noop":
			r = pipeMode(orderer.NewNoopCore, setup, run, o)
		default:
			orderer.Die("--core: unknown " + get("--core", ""))
		}
		os.RemoveAll(tmp)
		P, prod = strconv.Itoa(int(o.partitions)), strconv.Itoa(o.producers)
	default:
		orderer.Die("--mode: unknown " + mode)
	}
	if r.untimed > 0 {
		config = append(config, fmt.Sprintf("untimed=%d alloc_b_op=%.0f", uint64(r.untimed), r.allocPerOp))
	}
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })
	opsS := float64(r.ops) / r.wall.Seconds()
	var sum float64
	for _, v := range r.lat {
		sum += float64(v)
	}
	mean := uint64(0)
	if len(r.lat) > 0 {
		mean = uint64(sum / float64(len(r.lat)))
	}
	eff := ""
	if b, ok := a.Get("--baseline"); ok && mode == "pipe" {
		base, _ := strconv.ParseFloat(b, 64)
		pp, _ := strconv.ParseFloat(P, 64)
		eff = fmt.Sprintf("%.2f", opsS/(pp*base))
	}
	maxLat := uint64(0)
	if len(r.lat) > 0 {
		maxLat = r.lat[len(r.lat)-1]
	}
	fmt.Printf("| %s | %s | %s | %s | %d | %.0f | %s | %d | %d | %d | %d | %d | %d | %s |\n", tag, mode, P, prod, r.ops,
		opsS, eff, mean, pct(r.lat, 0.5), pct(r.lat, 0.9), pct(r.lat, 0.99), pct(r.lat, 0.999), maxLat,
		strings.Join(config, " "))
	fmt.Fprintf(os.Stderr, "env: %s / orderer-go 0.1.0 / %s\n", cpu(), runtime.Version())
}
