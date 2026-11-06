package cluster

import (
	"encoding/binary"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// Binary encodings shared with orderer-rust (codec.rs) and orderer-cpp
// (codec.hpp): protocol messages, WAL frame bodies and command-batch log entry
// payloads. Little-endian. Decoders never read out of bounds; they return a
// *CodecError on malformed input.

const (
	maxEntry        = 16 << 20
	cmdBytes        = 32
	tagBatch        = 1
	tagSessionBatch = 2
)

// CodecError reports malformed input.
type CodecError struct{ Msg string }

func (e *CodecError) Error() string { return "codec: " + e.Msg }

var le = binary.LittleEndian

// reader is a bounds-checked cursor; the first failure sticks and later reads
// return zeros, so decoders check err once at the end.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail(m string) {
	if r.err == nil {
		r.err = &CodecError{m}
	}
}

func (r *reader) remaining() int { return len(r.b) }

func (r *reader) bytes(n uint64) []byte {
	if r.err != nil || uint64(len(r.b)) < n {
		r.fail("truncated input")
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() uint8 {
	if v := r.bytes(1); v != nil {
		return v[0]
	}
	return 0
}

func (r *reader) u32() uint32 {
	if v := r.bytes(4); v != nil {
		return le.Uint32(v)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if v := r.bytes(8); v != nil {
		return le.Uint64(v)
	}
	return 0
}

func (r *reader) flag() bool {
	v := r.u8()
	if v > 1 {
		r.fail("flag")
	}
	return v == 1
}

func (r *reader) finish() error {
	if r.err == nil && len(r.b) != 0 {
		r.fail("trailing bytes")
	}
	return r.err
}

func putU8(o []byte, v uint8) []byte   { return append(o, v) }
func putU32(o []byte, v uint32) []byte { return le.AppendUint32(o, v) }
func putU64(o []byte, v uint64) []byte { return le.AppendUint64(o, v) }

func putBool(o []byte, b bool) []byte {
	if b {
		return append(o, 1)
	}
	return append(o, 0)
}

func putEntries(o []byte, es []consensus.Entry) []byte {
	o = putU32(o, uint32(len(es)))
	for _, e := range es {
		o = putU32(o, uint32(e.Term))
		o = putU32(o, uint32(len(e.Data)))
		o = append(o, e.Data...)
	}
	return o
}

// getEntries decodes an entry list. With own, entry data is copied out of the
// input buffer; otherwise it aliases it (fine when the buffer is not reused).
func getEntries(r *reader, own bool) []consensus.Entry {
	n := uint64(r.u32())
	// Each entry is at least 8 bytes; reject counts the input cannot hold.
	if r.err != nil || n > uint64(r.remaining())/8 {
		r.fail("entry count")
		return nil
	}
	es := make([]consensus.Entry, 0, n)
	for i := uint64(0); i < n; i++ {
		term := consensus.Term(r.u32())
		l := uint64(r.u32())
		if l > maxEntry {
			r.fail("entry too large")
			return nil
		}
		d := r.bytes(l)
		if r.err != nil {
			return nil
		}
		if own {
			d = append([]byte(nil), d...)
		}
		es = append(es, consensus.Entry{Term: term, Data: d})
	}
	return es
}

func encodeMsg(m consensus.Msg, o []byte) []byte {
	switch m.Kind {
	case consensus.MsgVote:
		o = putU8(o, 1)
		o = putU32(o, uint32(m.Term))
		o = putU64(o, uint64(m.LastIndex))
		o = putU32(o, uint32(m.LastTerm))
		o = putBool(o, m.Pre)
	case consensus.MsgVoteReply:
		o = putU8(o, 2)
		o = putU32(o, uint32(m.Term))
		o = putBool(o, m.Granted)
		o = putBool(o, m.Pre)
	case consensus.MsgAppend:
		o = putU8(o, 3)
		o = putU32(o, uint32(m.Term))
		o = putU64(o, uint64(m.PrevIndex))
		o = putU32(o, uint32(m.PrevTerm))
		o = putU64(o, uint64(m.Commit))
		o = putEntries(o, m.Entries)
	case consensus.MsgAppendReply:
		o = putU8(o, 4)
		o = putU32(o, uint32(m.Term))
		o = putBool(o, m.Success)
		o = putU64(o, uint64(m.Index))
	}
	return o
}

func decodeMsg(r *reader) consensus.Msg {
	var m consensus.Msg
	switch r.u8() {
	case 1:
		m.Kind = consensus.MsgVote
		m.Term = consensus.Term(r.u32())
		m.LastIndex = consensus.Index(r.u64())
		m.LastTerm = consensus.Term(r.u32())
		m.Pre = r.flag()
	case 2:
		m.Kind = consensus.MsgVoteReply
		m.Term = consensus.Term(r.u32())
		m.Granted = r.flag()
		m.Pre = r.flag()
	case 3:
		m.Kind = consensus.MsgAppend
		m.Term = consensus.Term(r.u32())
		m.PrevIndex = consensus.Index(r.u64())
		m.PrevTerm = consensus.Term(r.u32())
		m.Commit = consensus.Index(r.u64())
		m.Entries = getEntries(r, false)
	case 4:
		m.Kind = consensus.MsgAppendReply
		m.Term = consensus.Term(r.u32())
		m.Success = r.flag()
		m.Index = consensus.Index(r.u64())
	default:
		r.fail("message tag")
	}
	return m
}

// DecodeMsg decodes exactly one message from b.
func DecodeMsg(b []byte) (consensus.Msg, error) {
	r := reader{b: b}
	m := decodeMsg(&r)
	return m, r.finish()
}

// EncodeMsg appends the encoding of m to o.
func EncodeMsg(m consensus.Msg, o []byte) []byte { return encodeMsg(m, o) }

// encodePersist writes a WAL frame body: one atomic consensus write (the
// token is not stored).
func encodePersist(p consensus.Persist, o []byte) []byte {
	o = putU32(o, uint32(p.Term))
	if p.VotedFor == consensus.None {
		o = putU8(o, 255)
	} else {
		o = putU8(o, uint8(p.VotedFor))
	}
	if p.HasCompact {
		o = putU8(o, 1)
		o = putU64(o, uint64(p.CompactIdx))
		o = putU32(o, uint32(p.CompactTrm))
	} else {
		o = putU8(o, 0)
	}
	o = putU64(o, uint64(p.From))
	return putEntries(o, p.Entries)
}

func decodePersist(d []byte) (consensus.Persist, error) {
	r := reader{b: d}
	p := consensus.Persist{VotedFor: consensus.None}
	p.Term = consensus.Term(r.u32())
	if v := r.u8(); v != 255 {
		p.VotedFor = int(v)
	}
	switch r.u8() {
	case 0:
	case 1:
		p.HasCompact = true
		p.CompactIdx = consensus.Index(r.u64())
		p.CompactTrm = consensus.Term(r.u32())
	default:
		r.fail("compact flag")
	}
	p.From = consensus.Index(r.u64())
	if r.err == nil && p.From == 0 {
		r.fail("write starts at index 0")
	}
	p.Entries = getEntries(&r, true)
	return p, r.finish()
}

// applyPersist applies one write to a recovered disk image (the protocol
// core's Disk rules). Writes must be applied in issue order.
func applyPersist(d *consensus.Disk, p consensus.Persist) {
	d.Term = p.Term
	d.VotedFor = p.VotedFor
	if p.HasCompact {
		i := p.CompactIdx
		if i > d.Base {
			drop := uint64(i - d.Base)
			if drop > uint64(len(d.Log)) {
				drop = uint64(len(d.Log))
			}
			d.Log = append([]consensus.Entry(nil), d.Log[drop:]...)
			d.Base, d.BaseTerm = i, p.CompactTrm
		}
	}
	keep := uint64(p.From) - 1 - uint64(d.Base)
	if keep < uint64(len(d.Log)) {
		d.Log = d.Log[:keep]
	}
	d.Log = append(d.Log, p.Entries...)
}

// ---- command batches: the payload of a log entry ----

// SessionKey identifies one client request stream position.
type SessionKey struct{ Client, Seq uint64 }

// putCmd32 writes one command as the journal record's bytes 8..40 (symbol,
// kind, side, otype, tif, order id, price, qty) without iseq, checksum or
// padding.
func putCmd32(r []byte, sym uint32, c *matcher.Command) {
	for i := range r[:cmdBytes] {
		r[i] = 0
	}
	le.PutUint32(r, sym)
	le.PutUint64(r[8:], c.OrderID)
	switch c.Kind {
	case matcher.CmdNew:
		r[4], r[5], r[6], r[7] = 1, byte(c.Side), byte(c.OType), byte(c.Tif)
		le.PutUint64(r[16:], uint64(c.Price))
		le.PutUint64(r[24:], c.Qty)
	case matcher.CmdCancel:
		r[4] = 2
	default:
		r[4] = 3
		le.PutUint64(r[16:], uint64(c.Price))
		le.PutUint64(r[24:], c.Qty)
	}
}

// EncodeBatch appends `n` commands: 0x01 | n u32 | n*32 bytes; the session
// form is 0x02 | client u64 | seq u64 | n | ...
func EncodeBatch(cmds []orderer.SymCmd, o []byte, session *SessionKey) []byte {
	if session != nil {
		o = putU8(o, tagSessionBatch)
		o = putU64(o, session.Client)
		o = putU64(o, session.Seq)
	} else {
		o = putU8(o, tagBatch)
	}
	o = putU32(o, uint32(len(cmds)))
	at := len(o)
	if need := at + len(cmds)*cmdBytes; need > cap(o) {
		n := make([]byte, at, need)
		copy(n, o)
		o = n
	}
	o = o[:at+len(cmds)*cmdBytes]
	for i := range cmds {
		putCmd32(o[at+i*cmdBytes:], cmds[i].Sym, &cmds[i].Cmd)
	}
	return o
}

// SessionOf is the session a payload belongs to, if any, without decoding commands.
func SessionOf(d []byte) (SessionKey, bool) {
	if len(d) < 17 || d[0] != tagSessionBatch {
		return SessionKey{}, false
	}
	return SessionKey{le.Uint64(d[1:]), le.Uint64(d[9:])}, true
}

// DecodeBatch decodes a plain or session-tagged payload. Empty is a leader's no-op.
func DecodeBatch(d []byte) ([]orderer.SymCmd, error) {
	if len(d) == 0 {
		return nil, nil
	}
	r := reader{b: d}
	switch r.u8() {
	case tagBatch:
	case tagSessionBatch:
		r.u64()
		r.u64()
	default:
		r.fail("entry tag")
	}
	count := uint64(r.u32())
	if r.err != nil {
		return nil, r.err
	}
	if rem := uint64(r.remaining()); count != rem/cmdBytes || rem%cmdBytes != 0 {
		return nil, &CodecError{"batch length"}
	}
	out := make([]orderer.SymCmd, 0, count)
	for i := uint64(0); i < count; i++ {
		b := r.bytes(cmdBytes)
		sym := le.Uint32(b)
		id, price, qty := le.Uint64(b[8:]), int64(le.Uint64(b[16:])), le.Uint64(b[24:])
		var c matcher.Command
		switch b[4] {
		case 1:
			if b[5] > 1 || b[6] > 1 || b[7] > 3 {
				return nil, &CodecError{"command"}
			}
			c = matcher.Command{Kind: matcher.CmdNew, OrderID: id, Side: matcher.Side(b[5]),
				OType: matcher.OType(b[6]), Price: price, Qty: qty, Tif: matcher.Tif(b[7])}
		case 2:
			c = matcher.Cancel(id)
		case 3:
			c = matcher.Replace(id, price, qty)
		default:
			return nil, &CodecError{"command"}
		}
		out = append(out, orderer.SymCmd{Sym: sym, Cmd: c})
	}
	return out, nil
}
