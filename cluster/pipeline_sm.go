package cluster

import (
	"crypto/sha256"
	"errors"
	"fmt"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// PipelineSm is the orderer pipeline as the replicated state machine (port of
// orderer-rust's pipeline_sm.rs). A committed log entry carries a batch of
// commands; applying it publishes the batch into the local Pipeline in order,
// so every replica's partitions see the same ingress order and emit
// byte-identical events (spec/PIPELINE.md §3). The pipeline runs without
// journaling: the replicated log is the journal. The state serialised for
// snapshots and digests is `iseq u64 | partitions u32 | matcher-snap/1 body`,
// the same bytes orderer-rust and orderer-cpp produce, so replicas of the
// three languages agree on digests.
type PipelineSm struct {
	book   matcher.BookConfig
	pmap   *orderer.PartitionMap
	core   orderer.CoreFactory
	build  func(*orderer.Initial) (*orderer.Pipeline, error)
	pipe   *orderer.Pipeline
	handle *orderer.Handle

	badEntries uint64
}

// NewPipelineSm wraps a pipeline built by build, which must configure book
// and pmap's partition count, and no journaling. It is called again, with the
// restored state, whenever a snapshot is installed.
func NewPipelineSm(core orderer.CoreFactory, book matcher.BookConfig, pmap *orderer.PartitionMap,
	build func(*orderer.Initial) (*orderer.Pipeline, error)) (*PipelineSm, error) {
	s := &PipelineSm{book: book, pmap: pmap, core: core, build: build}
	if err := s.replace(nil); err != nil {
		return nil, err
	}
	return s, nil
}

// NewCountingPipelineSm is a ready-made one with a counting (non-accumulating)
// egress. The egress runs inline on the engine goroutines: it is trivially
// cheap, and three replicas' pipelines share a machine in tests and benchmarks.
func NewCountingPipelineSm(partitions uint32, book matcher.BookConfig) (*PipelineSm, error) {
	pmap, err := orderer.NewPartitionMap(partitions, nil)
	if err != nil {
		return nil, err
	}
	return NewPipelineSm(orderer.NewFifoCore, book, pmap, func(init *orderer.Initial) (*orderer.Pipeline, error) {
		egress, _ := orderer.Metrics(0)
		b := orderer.NewBuilder(orderer.NewFifoCore).BookConfig(book).PartitionMap(pmap).Egress(egress).Timestamps(false).
			EgressPlace(orderer.EgressInline)
		if init != nil {
			b = b.Initial(*init)
		}
		return b.Build()
	})
}

func (s *PipelineSm) replace(init *orderer.Initial) error {
	p, err := s.build(init)
	if err != nil {
		return err
	}
	s.closePipeline()
	s.pipe, s.handle = p, p.Handle()
	return nil
}

func (s *PipelineSm) closePipeline() {
	if s.pipe != nil {
		s.handle.Close()
		s.pipe.Shutdown()
		s.pipe, s.handle = nil, nil
	}
}

// Close shuts the pipeline down.
func (s *PipelineSm) Close() { s.closePipeline() }

func (s *PipelineSm) Apply(_ consensus.Index, e consensus.Entry) {
	cmds, err := DecodeBatch(e.Data)
	if err != nil {
		s.badEntries++ // every replica skips it identically
		return
	}
	if len(cmds) == 0 {
		return
	}
	if s.handle.PublishBatch(cmds) != orderer.Ok {
		panic("pipeline rejected committed commands")
	}
}

func (s *PipelineSm) encodeState() ([]byte, error) {
	if err := s.pipe.Drain(); err != nil { // egress has seen everything before the snapshot is taken
		return nil, err
	}
	snap, err := s.pipe.Snapshot()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 12+len(snap.Body))
	out = putU64(out, snap.Iseq)
	out = putU32(out, snap.Partitions)
	return append(out, snap.Body...), nil
}

func (s *PipelineSm) Snapshot(consensus.Index) []byte {
	b, err := s.encodeState()
	if err != nil {
		panic("pipeline snapshot: " + err.Error())
	}
	return b
}

func (s *PipelineSm) Restore(_ consensus.Index, d []byte) error {
	if len(d) < 12 {
		return errors.New("short pipeline snapshot")
	}
	snap := &orderer.Snapshot{Iseq: le.Uint64(d), Partitions: le.Uint32(d[8:]), Body: string(d[12:])}
	_, cores, err := orderer.Restore(s.core, s.book, s.pmap, snap)
	if err != nil {
		return err
	}
	if err := s.replace(&orderer.Initial{Cores: cores, NextIseq: snap.Iseq + 1}); err != nil {
		return fmt.Errorf("rebuild pipeline: %w", err)
	}
	return nil
}

func (s *PipelineSm) Digest(consensus.Index) (Digest, bool) {
	b, err := s.encodeState()
	if err != nil {
		return Digest{}, false
	}
	return sha256.Sum256(b), true
}

// BadEntries counts committed payloads skipped as undecodable.
func (s *PipelineSm) BadEntries() uint64 { return s.badEntries }
