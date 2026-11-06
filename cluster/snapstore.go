package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/abhijitkrm/orderer-go/consensus"
)

// Durable snapshots, byte-compatible with orderer-rust (snapstore.rs) and
// orderer-cpp: snap-<index>.snap = "ORDS" | index u64 | term u32 | len u64 |
// crc u32 | data, written to a temp name, fsynced, renamed, dir fsynced.

const snapHeader = 4 + 8 + 4 + 8 + 4

// SnapMeta is a snapshot file's header.
type SnapMeta struct {
	Index consensus.Index
	Term  consensus.Term
	Len   uint64
	CRC   uint32
}

// SnapStore is a directory of snapshots.
type SnapStore struct{ dir string }

// OpenSnapStore opens (creating) dir and discards leftover incomplete writes.
func OpenSnapStore(dir string) (*SnapStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, ioErr("create %s: %v", dir, err)
	}
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return &SnapStore{dir}, nil
}

func (s *SnapStore) path(i consensus.Index) string {
	return filepath.Join(s.dir, fmt.Sprintf("snap-%020d.snap", uint64(i)))
}

func (s *SnapStore) tmp(i consensus.Index) string {
	return filepath.Join(s.dir, fmt.Sprintf("snap-%020d.tmp", uint64(i)))
}

func snapHead(index consensus.Index, term consensus.Term, n uint64, crc uint32) []byte {
	h := []byte{'O', 'R', 'D', 'S'}
	h = putU64(h, uint64(index))
	h = putU32(h, uint32(term))
	h = putU64(h, n)
	return putU32(h, crc)
}

func parseSnapHead(h []byte) (SnapMeta, bool) {
	if len(h) < snapHeader || string(h[:4]) != "ORDS" {
		return SnapMeta{}, false
	}
	return SnapMeta{consensus.Index(le.Uint64(h[4:])), consensus.Term(le.Uint32(h[12:])), le.Uint64(h[16:]), le.Uint32(h[24:])}, true
}

// Put stores a snapshot durably.
func (s *SnapStore) Put(index consensus.Index, term consensus.Term, data []byte) error {
	tmp := s.tmp(index)
	if err := writeFileDurably(tmp, snapHead(index, term, uint64(len(data)), crc32c(data)), data); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(index)); err != nil {
		return ioErr("rename: %v", err)
	}
	return syncDir(s.dir)
}

// Meta reads a snapshot's header if the file is the right size.
func (s *SnapStore) Meta(index consensus.Index) (SnapMeta, bool) {
	f, err := os.Open(s.path(index))
	if err != nil {
		return SnapMeta{}, false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() < snapHeader {
		return SnapMeta{}, false
	}
	h := make([]byte, snapHeader)
	if _, err := f.ReadAt(h, 0); err != nil {
		return SnapMeta{}, false
	}
	m, ok := parseSnapHead(h)
	if !ok || m.Index != index || uint64(st.Size()) != snapHeader+m.Len {
		return SnapMeta{}, false
	}
	return m, true
}

// List is the indexes of intact snapshot files, ascending. A corrupt file is skipped.
func (s *SnapStore) List() []consensus.Index {
	var v []consensus.Index
	ents, _ := os.ReadDir(s.dir)
	for _, e := range ents {
		n := e.Name()
		if strings.HasPrefix(n, "snap-") && strings.HasSuffix(n, ".snap") {
			if i, err := strconv.ParseUint(n[5:len(n)-5], 10, 64); err == nil {
				if _, ok := s.Meta(consensus.Index(i)); ok {
					v = append(v, consensus.Index(i))
				}
			}
		}
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v
}

// Get loads and verifies a snapshot; ok is false if absent or corrupt.
func (s *SnapStore) Get(index consensus.Index) (SnapMeta, []byte, bool) {
	all, err := os.ReadFile(s.path(index))
	if err != nil || len(all) < snapHeader {
		return SnapMeta{}, nil, false
	}
	m, ok := parseSnapHead(all)
	if !ok || m.Index != index || uint64(len(all)-snapHeader) != m.Len {
		return SnapMeta{}, nil, false
	}
	d := all[snapHeader:]
	if crc32c(d) != m.CRC {
		return SnapMeta{}, nil, false
	}
	return m, d, true
}

// GC deletes snapshots older than keepFrom.
func (s *SnapStore) GC(keepFrom consensus.Index) {
	for _, i := range s.List() {
		if i < keepFrom {
			os.Remove(s.path(i))
		}
	}
}

// Incoming is a snapshot being received from a peer, verified before it is published.
type Incoming struct {
	f        *os.File
	tmp, dst string
	dir      string
	len, got uint64
	crc      uint32
	running  uint32
}

// Receive starts accepting a snapshot announced by a peer.
func (s *SnapStore) Receive(index consensus.Index, term consensus.Term, n uint64, crc uint32) (*Incoming, error) {
	in := &Incoming{tmp: s.tmp(index), dst: s.path(index), dir: s.dir, len: n, crc: crc}
	f, err := os.OpenFile(in.tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, ioErr("create %s: %v", in.tmp, err)
	}
	in.f = f
	if _, err := f.Write(snapHead(index, term, n, crc)); err != nil {
		in.Abort()
		return nil, ioErr("write: %v", err)
	}
	return in, nil
}

// Write appends a chunk.
func (in *Incoming) Write(d []byte) error {
	if in.got+uint64(len(d)) > in.len {
		return ioErr("snapshot longer than announced")
	}
	in.got += uint64(len(d))
	in.running = crc32cUpdate(in.running, d)
	if _, err := in.f.Write(d); err != nil {
		return ioErr("write: %v", err)
	}
	return nil
}

// Finish verifies length and checksum and publishes the file.
func (in *Incoming) Finish() error {
	if in.got != in.len || in.running != in.crc {
		in.Abort()
		if in.got != in.len {
			return ioErr("snapshot shorter than announced")
		}
		return ioErr("snapshot checksum mismatch")
	}
	if err := syncFile(in.f); err != nil {
		in.Abort()
		return err
	}
	in.f.Close()
	in.f = nil
	if err := os.Rename(in.tmp, in.dst); err != nil {
		return ioErr("rename: %v", err)
	}
	return syncDir(in.dir)
}

// Abort discards a partial snapshot.
func (in *Incoming) Abort() {
	if in.f != nil {
		in.f.Close()
		in.f = nil
	}
	if err := os.Remove(in.tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = err
	}
}
