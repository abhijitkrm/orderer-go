package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/abhijitkrm/orderer-go/consensus"
)

// The replicated log's write-ahead file, byte-compatible with orderer-rust
// (wal.rs) and orderer-cpp (wal.hpp); spec/REPLICATION.md 13.3.
//
// One consensus write (Persist) is one frame: len u32 | crc32c u32 | body.
// Recovery cuts a torn tail (including a zero-filled one that a SIGKILL during
// an append can leave) and refuses to start when valid frames follow a bad one.

var walMagic = []byte{'O', 'R', 'D', 'W', 'A', 'L', 0, 1}

const (
	frameHead = 8
	maxFrame  = 256 << 20
	// term u32, vote u8, compact flag u8, from u64, entry count u32.
	minBody = 4 + 1 + 1 + 8 + 4
)

// WalErrKind classifies a WalError.
type WalErrKind int

const (
	WalIO WalErrKind = iota
	WalCorrupt
	WalBadHeader
)

// WalError is a storage failure; Offset locates corruption.
type WalError struct {
	Kind   WalErrKind
	Msg    string
	Offset uint64
}

func (e *WalError) Error() string { return e.Msg }

func ioErr(format string, a ...any) error {
	return &WalError{Kind: WalIO, Msg: fmt.Sprintf(format, a...)}
}

// Repair reports what opening a WAL had to cut.
type Repair struct{ Frames, TornBytes uint64 }

// syncFile flushes a file as durably as the platform allows (the standard
// library uses F_FULLFSYNC on macOS).
func syncFile(f *os.File) error {
	if err := f.Sync(); err != nil {
		return ioErr("fsync: %v", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return ioErr("open dir %s: %v", dir, err)
	}
	defer d.Close()
	// Some platforms refuse fsync on a directory handle; the rename is as
	// durable as that platform can make it then.
	if err := d.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOTSUP) {
		return ioErr("fsync dir %s: %v", dir, err)
	}
	return nil
}

// writeFileDurably creates path with data, fsynced.
func writeFileDurably(path string, data ...[]byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return ioErr("create %s: %v", path, err)
	}
	for _, d := range data {
		if _, err := f.Write(d); err != nil {
			f.Close()
			return ioErr("write %s: %v", path, err)
		}
	}
	if err := syncFile(f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Wal is an open write-ahead log.
type Wal struct {
	f    *os.File
	path string
	len  uint64
	buf  []byte
}

// OpenWal opens (creating if absent) the WAL in dir, repairing a torn tail.
// The recovered disk image comes back with the repair report.
func OpenWal(dir string) (*Wal, consensus.Disk, Repair, error) {
	var none consensus.Disk
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, none, Repair{}, ioErr("create %s: %v", dir, err)
	}
	path, tmp := filepath.Join(dir, "raft.wal"), filepath.Join(dir, "raft.wal.tmp")
	os.Remove(tmp)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := writeFileDurably(tmp, walMagic); err != nil {
			return nil, none, Repair{}, err
		}
		if err := os.Rename(tmp, path); err != nil {
			return nil, none, Repair{}, ioErr("rename: %v", err)
		}
		if err := syncDir(dir); err != nil {
			return nil, none, Repair{}, err
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, none, Repair{}, ioErr("cannot read %s: %v", path, err)
	}
	disk, valid, frames, err := replayWal(data)
	if err != nil {
		return nil, none, Repair{}, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, none, Repair{}, ioErr("open %s: %v", path, err)
	}
	torn := uint64(len(data) - valid)
	if torn > 0 {
		if err := f.Truncate(int64(valid)); err != nil {
			f.Close()
			return nil, none, Repair{}, ioErr("truncate: %v", err)
		}
		if err := syncFile(f); err != nil {
			f.Close()
			return nil, none, Repair{}, err
		}
	}
	if _, err := f.Seek(int64(valid), io.SeekStart); err != nil {
		f.Close()
		return nil, none, Repair{}, ioErr("seek: %v", err)
	}
	return &Wal{f: f, path: path, len: uint64(valid)}, disk, Repair{frames, torn}, nil
}

// Append buffers one write. Nothing is durable until Sync.
func (w *Wal) Append(p consensus.Persist) { w.buf = frameWal(p, w.buf) }

// Sync writes buffered frames and fsyncs (durable=false only writes: benchmarks).
func (w *Wal) Sync(durable bool) error {
	if len(w.buf) > 0 {
		if _, err := w.f.Write(w.buf); err != nil {
			return ioErr("write: %v", err)
		}
		w.len += uint64(len(w.buf))
		w.buf = w.buf[:0]
	}
	if durable {
		return syncFile(w.f)
	}
	return nil
}

// Len is the file's length in bytes.
func (w *Wal) Len() uint64 { return w.len }

// Close releases the file.
func (w *Wal) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Rewrite replaces the file with one frame holding the current state, so a
// compacted log's prefix no longer occupies disk. Crash-safe.
func (w *Wal) Rewrite() error {
	if err := w.Sync(false); err != nil {
		return err
	}
	data, err := os.ReadFile(w.path)
	if err != nil {
		return ioErr("cannot read %s: %v", w.path, err)
	}
	disk, _, _, err := replayWal(data)
	if err != nil {
		return err
	}
	p := consensus.Persist{Term: disk.Term, VotedFor: disk.VotedFor, From: disk.Base + 1, Entries: disk.Log}
	if disk.Base > 0 {
		p.HasCompact, p.CompactIdx, p.CompactTrm = true, disk.Base, disk.BaseTerm
	}
	out := frameWal(p, append([]byte(nil), walMagic...))
	dir := filepath.Dir(w.path)
	tmp := filepath.Join(dir, "raft.wal.tmp")
	if err := writeFileDurably(tmp, out); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return ioErr("rename: %v", err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	w.f.Close()
	f, err := os.OpenFile(w.path, os.O_RDWR, 0)
	if err != nil {
		return ioErr("reopen %s: %v", w.path, err)
	}
	w.f, w.len = f, uint64(len(out))
	if _, err := f.Seek(int64(w.len), io.SeekStart); err != nil {
		return ioErr("seek: %v", err)
	}
	return nil
}

func frameWal(p consensus.Persist, out []byte) []byte {
	start := len(out)
	out = append(out, make([]byte, frameHead)...)
	out = encodePersist(p, out)
	body := out[start+frameHead:]
	le.PutUint32(out[start:], uint32(len(body)))
	le.PutUint32(out[start+4:], crc32c(body))
	return out
}

// frameAt returns the body of the valid frame at off, if there is one.
func frameAt(d []byte, off int) ([]byte, bool) {
	if off+frameHead > len(d) {
		return nil, false
	}
	l, c := le.Uint32(d[off:]), le.Uint32(d[off+4:])
	// A real body is never shorter than the fixed fields of a write. That also
	// rejects zeros: an all-zero header (length 0, checksum 0) would otherwise
	// pass as a valid empty frame.
	if l < minBody || l > maxFrame || off+frameHead+int(l) > len(d) {
		return nil, false
	}
	body := d[off+frameHead : off+frameHead+int(l)]
	return body, crc32c(body) == c
}

func fits(disk *consensus.Disk, p *consensus.Persist) bool {
	if p.From == 0 {
		return false
	}
	base, n := disk.Base, uint64(len(disk.Log))
	if p.HasCompact && p.CompactIdx > disk.Base {
		base = p.CompactIdx
		drop := uint64(p.CompactIdx - disk.Base)
		if n > drop {
			n -= drop
		} else {
			n = 0
		}
	}
	return p.From > base && uint64(p.From-1-base) <= n
}

// replayWal replays a whole WAL image. valid is the length of the intact prefix.
func replayWal(data []byte) (disk consensus.Disk, valid int, frames uint64, err error) {
	if len(data) < len(walMagic) || !bytes.Equal(data[:len(walMagic)], walMagic) {
		return disk, 0, 0, &WalError{Kind: WalBadHeader, Msg: "wal header missing or unrecognised"}
	}
	disk.VotedFor = consensus.None
	off := len(walMagic)
	for off < len(data) {
		body, ok := frameAt(data, off)
		if !ok {
			// Torn only if nothing valid follows.
			if validFrameFollows(data, off) {
				return disk, 0, 0, &WalError{Kind: WalCorrupt, Msg: "wal corrupt", Offset: uint64(off)}
			}
			break
		}
		p, derr := decodePersist(body)
		if derr != nil || !fits(&disk, &p) {
			return disk, 0, 0, &WalError{Kind: WalCorrupt, Msg: "wal corrupt", Offset: uint64(off)}
		}
		applyPersist(&disk, p)
		off += frameHead + len(body)
		frames++
	}
	return disk, off, frames, nil
}

func validFrameFollows(d []byte, bad int) bool {
	if bad+frameHead <= len(d) {
		if l := le.Uint32(d[bad:]); l >= minBody && l <= maxFrame {
			next := bad + frameHead + int(l)
			if next < len(d) {
				if _, ok := frameAt(d, next); ok {
					return true
				}
			}
		}
	}
	// Bounded scan for a frame starting anywhere in the next 1 MiB.
	end := bad + 1 + 1<<20
	if end > len(d) {
		end = len(d)
	}
	for o := bad + 1; o < end; o++ {
		if o+4 > len(d) {
			break
		}
		if uint64(le.Uint32(d[o:])) > uint64(len(d)-o) {
			continue // cheap reject before checksumming
		}
		if body, ok := frameAt(d, o); ok {
			if _, err := decodePersist(body); err == nil {
				return true
			}
		}
	}
	return false
}
