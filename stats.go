package orderer

// Operational statistics (not part of the spec contract): ring depths,
// per-partition counters, journal watermarks and fsync timings, and a
// Prometheus text-format rendering.

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// IoStats are the fsync timings one journal I/O goroutine records.
type IoStats struct {
	Fsyncs, FsyncNsTotal, FsyncNsMax atomic.Uint64
}

func (s *IoStats) record(ns uint64) {
	s.Fsyncs.Add(1)
	s.FsyncNsTotal.Add(ns)
	for m := s.FsyncNsMax.Load(); ns > m && !s.FsyncNsMax.CompareAndSwap(m, ns); m = s.FsyncNsMax.Load() {
	}
}

// engineCounters are published by an engine goroutine once per batch.
type engineCounters struct {
	commands, events atomic.Uint64
}

// PartitionStats is one partition's view. FlushedIseq/DurableIseq are max
// uint64 without journals.
type PartitionStats struct {
	Partition                        uint32
	InboxDepth, OutboxDepth          uint64
	Commands, Events                 uint64
	FlushedIseq, DurableIseq         uint64
	Fsyncs, FsyncNsTotal, FsyncNsMax uint64
}

// PipelineStats is a point-in-time view (fields individually exact, not mutually consistent).
type PipelineStats struct {
	IngressDepth uint64
	Partitions   []PartitionStats
}

// ToPrometheus renders the text exposition format (version 0.0.4).
func (st PipelineStats) ToPrometheus() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP orderer_ingress_depth Commands published, not yet routed.\n# TYPE orderer_ingress_depth gauge\norderer_ingress_depth %d\n", st.IngressDepth)
	series := func(name, help, kind string, get func(PartitionStats) uint64) {
		fmt.Fprintf(&b, "# HELP orderer_%s %s\n# TYPE orderer_%s %s\n", name, help, name, kind)
		for _, p := range st.Partitions {
			fmt.Fprintf(&b, "orderer_%s{partition=\"%d\"} %d\n", name, p.Partition, get(p))
		}
	}
	series("inbox_depth", "Commands routed, not yet applied.", "gauge", func(p PartitionStats) uint64 { return p.InboxDepth })
	series("outbox_depth", "Events staged, not yet consumed by egress.", "gauge", func(p PartitionStats) uint64 { return p.OutboxDepth })
	series("commands_total", "Commands applied.", "counter", func(p PartitionStats) uint64 { return p.Commands })
	series("events_total", "Events emitted.", "counter", func(p PartitionStats) uint64 { return p.Events })
	series("durable_iseq", "Highest iseq covered by a completed fsync.", "gauge", func(p PartitionStats) uint64 { return p.DurableIseq })
	series("fsyncs_total", "Journal fsyncs.", "counter", func(p PartitionStats) uint64 { return p.Fsyncs })
	series("fsync_ns_total", "Time spent in journal fsync, in nanoseconds.", "counter", func(p PartitionStats) uint64 { return p.FsyncNsTotal })
	series("fsync_max_ns", "Longest journal fsync, in nanoseconds.", "gauge", func(p PartitionStats) uint64 { return p.FsyncNsMax })
	return b.String()
}
