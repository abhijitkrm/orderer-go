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
//
// Divergence detection uses an EventDigest: a rolling hash over the events each
// partition emits, so a digest costs one pipeline barrier, not a snapshot of
// every book (spec/REPLICATION.md 13.16). DeepDigest is the full-state hash,
// computed on demand.
type PipelineSm struct {
	book   matcher.BookConfig
	pmap   *orderer.PartitionMap
	core   orderer.CoreFactory
	build  func(*orderer.Initial, *orderer.EventDigest) (*orderer.Pipeline, error)
	pipe   *orderer.Pipeline
	handle *orderer.Handle
	chains *orderer.EventDigest
	parts  uint32
	iseq   uint64 // commands applied so far (the pipeline's iseq)

	badEntries uint64
}

// snapMagic marks the snapshot layout: "PSM2" | iseq u64 | partitions u32 | one
// chain u64 per partition | matcher-snap/1 body.
const snapMagic = "PSM2"

// digestTag prefixes the replica digest: SHA-256 over this tag, the commands
// applied, the partition count and every partition's event chain.
const digestTag = "orderer-evdigest/1"

// NewPipelineSm wraps a pipeline built by build, which must configure book
// and pmap's partition count, no journaling, and register the EventDigest it
// is given as an egress (Builder.Egress(digest.Factory())). It is called
// again, with the restored state, whenever a snapshot is installed.
func NewPipelineSm(core orderer.CoreFactory, book matcher.BookConfig, pmap *orderer.PartitionMap,
	build func(*orderer.Initial, *orderer.EventDigest) (*orderer.Pipeline, error)) (*PipelineSm, error) {
	s := &PipelineSm{book: book, pmap: pmap, core: core, build: build, parts: pmap.Partitions()}
	if err := s.replace(nil, nil); err != nil {
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
	return NewPipelineSm(orderer.NewFifoCore, book, pmap, func(init *orderer.Initial, digest *orderer.EventDigest) (*orderer.Pipeline, error) {
		egress, _ := orderer.Metrics(0)
		b := orderer.NewBuilder(orderer.NewFifoCore).BookConfig(book).PartitionMap(pmap).Egress(egress).
			Egress(digest.Factory()).Timestamps(false).EgressPlace(orderer.EgressInline)
		if init != nil {
			b = b.Initial(*init)
		}
		return b.Build()
	})
}

func (s *PipelineSm) replace(init *orderer.Initial, chains []uint64) error {
	digest := orderer.NewEventDigest(s.parts, chains)
	p, err := s.build(init, digest)
	if err != nil {
		return err
	}
	s.closePipeline()
	s.pipe, s.handle, s.chains = p, p.Handle(), digest
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
	s.iseq += uint64(len(cmds))
}

// encodeSnapshot is the state serialised for a snapshot: the full-state
// encoding plus the event chains, so a replica that restores continues them.
func (s *PipelineSm) encodeSnapshot() ([]byte, error) {
	if err := s.pipe.Drain(); err != nil { // egress has seen everything before the snapshot is taken
		return nil, err
	}
	snap, err := s.pipe.Snapshot()
	if err != nil {
		return nil, err
	}
	chains := s.chains.Values()
	out := make([]byte, 0, 16+8*len(chains)+len(snap.Body))
	out = append(out, snapMagic...)
	out = putU64(out, snap.Iseq)
	out = putU32(out, snap.Partitions)
	for _, c := range chains {
		out = putU64(out, c)
	}
	return append(out, snap.Body...), nil
}

func (s *PipelineSm) Snapshot(consensus.Index) []byte {
	b, err := s.encodeSnapshot()
	if err != nil {
		panic("pipeline snapshot: " + err.Error())
	}
	return b
}

func (s *PipelineSm) Restore(_ consensus.Index, d []byte) error {
	head := 16 + 8*int(s.parts)
	if len(d) < head || string(d[:4]) != snapMagic {
		return errors.New("not a pipeline snapshot (PSM2)")
	}
	iseq, parts := le.Uint64(d[4:]), le.Uint32(d[12:])
	if parts != s.parts {
		return fmt.Errorf("snapshot has %d partitions, this replica runs %d", parts, s.parts)
	}
	chains := make([]uint64, parts)
	for i := range chains {
		chains[i] = le.Uint64(d[16+8*i:])
	}
	snap := &orderer.Snapshot{Iseq: iseq, Partitions: parts, Body: string(d[head:])}
	_, cores, err := orderer.Restore(s.core, s.book, s.pmap, snap)
	if err != nil {
		return err
	}
	if err := s.replace(&orderer.Initial{Cores: cores, NextIseq: iseq + 1}, chains); err != nil {
		return fmt.Errorf("rebuild pipeline: %w", err)
	}
	s.iseq = iseq
	return nil
}

// Digest is the event-chain digest: O(partitions) after one pipeline barrier.
func (s *PipelineSm) Digest(consensus.Index) (Digest, bool) {
	if err := s.pipe.Drain(); err != nil {
		return Digest{}, false
	}
	b := append([]byte(digestTag), putU64(nil, s.iseq)...)
	b = putU32(b, s.parts)
	for _, c := range s.chains.Values() {
		b = putU64(b, c)
	}
	return sha256.Sum256(b), true
}

// DeepDigest is the full-state hash: SHA-256 of iseq | partitions | matcher-snap/1
// body. It costs a snapshot of every book.
func (s *PipelineSm) DeepDigest(consensus.Index) (Digest, bool) {
	if err := s.pipe.Drain(); err != nil {
		return Digest{}, false
	}
	snap, err := s.pipe.Snapshot()
	if err != nil {
		return Digest{}, false
	}
	b := putU64(nil, snap.Iseq)
	b = putU32(b, snap.Partitions)
	return sha256.Sum256(append(b, snap.Body...)), true
}

// BadEntries counts committed payloads skipped as undecodable.
func (s *PipelineSm) BadEntries() uint64 { return s.badEntries }
