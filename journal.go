package orderer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// Per-partition journals (spec/JOURNAL.md): naming, JSONL and binary
// encodings, strict readers, and the asynchronous chunk writer.

// JournalFormat is JSONL (canonical lines) or Binary (fixed records).
type JournalFormat uint8

const (
	Jsonl JournalFormat = iota
	Binary
)

// JournalKind is a command or event journal.
type JournalKind uint8

const (
	KindCmd JournalKind = 1
	KindEvt JournalKind = 2
)

func (k JournalKind) label() string {
	if k == KindCmd {
		return "cmd"
	}
	return "evt"
}

// recordSize is the record size in a binary journal of version.
func (k JournalKind) recordSize(version uint16) int {
	if version == 1 {
		return k.payload()
	}
	return k.payload() + 8
}

// payload is the bytes the checksum covers (the version-1 record).
func (k JournalKind) payload() int {
	if k == KindCmd {
		return CmdRecordV1
	}
	return EvtRecordV1
}

const (
	Header = 64
	// Version-2 record sizes (1.2): the version-1 record + CRC-32C + 4 reserved bytes.
	CmdRecord   = 48
	EvtRecord   = 56
	CmdRecordV1 = 40
	EvtRecordV1 = 48
	// Version is the binary journal version 1.2 writers produce.
	Version   = 2
	maxRecord = 256
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC32C is CRC-32C (Castagnoli), spec/JOURNAL.md §2.2.
func CRC32C(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// seal writes a version-2 record's checksum after its payload.
func seal(r []byte, payload int) {
	binary.LittleEndian.PutUint32(r[payload:], CRC32C(r[:payload]))
	binary.LittleEndian.PutUint32(r[payload+4:], 0)
}

func sealed(r []byte, payload int) bool {
	return binary.LittleEndian.Uint32(r[payload:]) == CRC32C(r[:payload])
}

// FsyncMode selects when the I/O goroutine fsyncs. Never observable (spec/PIPELINE.md §8).
type FsyncMode uint8

const (
	FsyncNever FsyncMode = iota
	FsyncEveryN
	FsyncEvery
)

// FsyncPolicy is a FsyncMode plus its parameters.
type FsyncPolicy struct {
	Mode     FsyncMode
	N        uint64
	Idle     time.Duration // EveryN: sync after this long with nothing more queued
	Interval time.Duration // Every
}

func FsyncNeverPolicy() FsyncPolicy { return FsyncPolicy{Mode: FsyncNever} }

// FsyncEveryNPolicy group-commits once n records are pending, or after 200 µs idle.
func FsyncEveryNPolicy(n uint64) FsyncPolicy {
	return FsyncPolicy{Mode: FsyncEveryN, N: n, Idle: 200 * time.Microsecond}
}

func FsyncEveryPolicy(d time.Duration) FsyncPolicy {
	return FsyncPolicy{Mode: FsyncEvery, Idle: d, Interval: d}
}

// JournalConfig places and paces a pipeline's journals.
type JournalConfig struct {
	Dir    string
	Format JournalFormat
	Fsync  FsyncPolicy
	Events bool // event journals too (derived data)
	Append bool // reopen existing journals (after recovery)
}

// NewJournalConfig is binary-or-JSONL, durable (fsync every 1024 records), events on.
func NewJournalConfig(dir string, f JournalFormat) JournalConfig {
	return JournalConfig{Dir: dir, Format: f, Fsync: FsyncEveryNPolicy(1024), Events: true}
}

// CorruptJournal is a malformed, torn or inconsistent journal.
type CorruptJournal struct{ Msg string }

func (e *CorruptJournal) Error() string { return e.Msg }

func corrupt(p, d string) error { return &CorruptJournal{p + ": " + d} }

func extOf(f JournalFormat) string {
	if f == Binary {
		return ".bin"
	}
	return ".journal"
}

// JournalPath names partition p's segment-0 journal of kind k.
func JournalPath(dir string, k JournalKind, p uint32, f JournalFormat) string {
	return SegmentPath(dir, k, p, 0, f)
}

// SegmentPath names the segment starting after cut start (spec/JOURNAL.md §1).
func SegmentPath(dir string, k JournalKind, p uint32, start uint64, f JournalFormat) string {
	if start == 0 {
		return filepath.Join(dir, fmt.Sprintf("%s-%d%s", k.label(), p, extOf(f)))
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%d.%d%s", k.label(), p, start, extOf(f)))
}

// Segment is one journal segment file.
type Segment struct {
	Partition uint32
	Start     uint64
	Path      string
}

func parseUint(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}

// ListSegments lists every kind-k segment in dir, sorted by (partition, start).
func ListSegments(dir string, k JournalKind, f JournalFormat) []Segment {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	prefix, suffix := k.label()+"-", extOf(f)
	var out []Segment
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) || len(name) <= len(prefix)+len(suffix) {
			continue
		}
		mid := name[len(prefix) : len(name)-len(suffix)]
		ps, ss, dotted := strings.Cut(mid, ".")
		p, ok := parseUint(ps)
		if !ok || p > 0xFFFFFFFF {
			continue
		}
		var start uint64
		if dotted {
			if start, ok = parseUint(ss); !ok || start == 0 {
				continue
			}
		}
		out = append(out, Segment{uint32(p), start, filepath.Join(dir, name)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Start < out[j].Start
	})
	return out
}

// CheckpointPath is the checkpoint snapshot path for cut n (spec/JOURNAL.md §6).
func CheckpointPath(dir string, n uint64) string {
	return filepath.Join(dir, fmt.Sprintf("checkpoint-%d.snap", n))
}

// Checkpoint is one checkpoint snapshot in a journal directory.
type Checkpoint struct {
	Cut  uint64
	Path string
}

// ListCheckpoints lists checkpoints with cut below `below`, ascending;
// complete ones have their sidecar.
func ListCheckpoints(dir string, complete bool, below uint64) []Checkpoint {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []Checkpoint
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "checkpoint-") || !strings.HasSuffix(name, ".snap") {
			continue
		}
		n, ok := parseUint(name[len("checkpoint-") : len(name)-len(".snap")])
		if !ok || n >= below {
			continue
		}
		path := filepath.Join(dir, name)
		if complete {
			if _, err := os.Stat(MetaPath(path)); err != nil {
				continue
			}
		}
		out = append(out, Checkpoint{n, path})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cut < out[j].Cut })
	return out
}

// ---- encodings -----------------------------------------------------------------------------

func jsonlHeader(k JournalKind, p, P uint32, b matcher.BookConfig) string {
	return fmt.Sprintf("{\"format\":\"orderer-journal/1\",\"kind\":\"%s\",\"partition\":%d,\"partitions\":%d,\"pmin\":%d,\"pmax\":%d,\"max_orders\":%d,\"index\":\"%s\"}\n",
		k.label(), p, P, b.PriceMin, b.PriceMax, b.MaxOrders, IndexName(b.Index))
}

func binaryHeader(k JournalKind, p, P uint32, b matcher.BookConfig) []byte {
	h := make([]byte, Header)
	copy(h, "ORDJ")
	le := binary.LittleEndian
	le.PutUint16(h[4:], Version)
	h[6] = byte(k)
	if b.Index == matcher.IndexTree {
		h[7] = 1
	}
	le.PutUint32(h[8:], p)
	le.PutUint32(h[12:], P)
	le.PutUint32(h[16:], uint32(k.recordSize(Version)))
	le.PutUint64(h[24:], uint64(b.PriceMin))
	le.PutUint64(h[32:], uint64(b.PriceMax))
	le.PutUint64(h[40:], uint64(b.MaxOrders))
	return h
}

// AppendCmdLine appends a JSONL command record (no newline): canonical engine line + "iseq".
func AppendCmdLine(iseq uint64, sym uint32, c *matcher.Command, b []byte) []byte {
	b = WriteCommand(c, sym, true, b)
	b = b[:len(b)-1]
	b = append(b, `,"iseq":`...)
	b = strconv.AppendUint(b, iseq, 10)
	return append(b, '}')
}

// EncodeCmd writes a sealed version-2 binary command record (CmdRecord bytes).
func EncodeCmd(iseq uint64, sym uint32, c *matcher.Command, r []byte) {
	le := binary.LittleEndian
	le.PutUint64(r[0:], iseq)
	le.PutUint32(r[8:], sym)
	r[13], r[14], r[15] = 0, 0, 0
	var price int64
	var qty uint64
	switch c.Kind {
	case matcher.CmdNew:
		r[12], r[13], r[14], r[15] = 1, byte(c.Side), byte(c.OType), byte(c.Tif)
		price, qty = c.Price, c.Qty
	case matcher.CmdCancel:
		r[12] = 2
	default:
		r[12] = 3
		price, qty = c.Price, c.Qty
	}
	le.PutUint64(r[16:], c.OrderID)
	le.PutUint64(r[24:], uint64(price))
	le.PutUint64(r[32:], qty)
	seal(r, CmdRecordV1)
}

// CmdRec is one command record.
type CmdRec struct {
	Iseq uint64
	Sym  uint32
	Cmd  matcher.Command
}

func decodeCmd(r []byte) (CmdRec, bool) {
	le := binary.LittleEndian
	rec := CmdRec{Iseq: le.Uint64(r), Sym: le.Uint32(r[8:])}
	id, price, qty := le.Uint64(r[16:]), int64(le.Uint64(r[24:])), le.Uint64(r[32:])
	switch r[12] {
	case 1:
		if r[13] > 1 || r[14] > 1 || r[15] > 3 {
			return rec, false
		}
		rec.Cmd = matcher.Command{Kind: matcher.CmdNew, OrderID: id, Side: matcher.Side(r[13]),
			OType: matcher.OType(r[14]), Price: price, Qty: qty, Tif: matcher.Tif(r[15])}
	case 2:
		rec.Cmd = matcher.Cancel(id)
	case 3:
		rec.Cmd = matcher.Replace(id, price, qty)
	default:
		return rec, false
	}
	return rec, true
}

// EncodeEvt writes a sealed version-2 binary event record (spec/JOURNAL.md §3.2).
func EncodeEvt(seq uint64, sym uint32, e *matcher.Event, r []byte) {
	le := binary.LittleEndian
	le.PutUint64(r[0:], seq)
	le.PutUint32(r[8:], sym)
	var a, b, d uint64
	var c int64
	reason := byte(0)
	switch e.Kind {
	case matcher.EvAccepted:
		a, d = e.OrderID, e.LeavesQty
	case matcher.EvRejected:
		a, reason = e.OrderID, e.Reason+1
	case matcher.EvTrade:
		a, b, c, d = e.Maker, e.Taker, e.Price, e.Qty
	case matcher.EvClosed:
		a, reason = e.OrderID, e.Reason+1
	case matcher.EvReplaced:
		a, c, d = e.OrderID, e.Price, e.Qty
	}
	r[12], r[13], r[14], r[15] = byte(e.Kind)+1, reason, 0, 0
	le.PutUint64(r[16:], a)
	le.PutUint64(r[24:], b)
	le.PutUint64(r[32:], uint64(c))
	le.PutUint64(r[40:], d)
	seal(r, EvtRecordV1)
}

func decodeEvt(r []byte) (uint64, uint32, matcher.Event, bool) {
	le := binary.LittleEndian
	seq, sym := le.Uint64(r), le.Uint32(r[8:])
	a, b, c, d := le.Uint64(r[16:]), le.Uint64(r[24:]), int64(le.Uint64(r[32:])), le.Uint64(r[40:])
	reason := r[13]
	var e matcher.Event
	switch r[12] {
	case 1:
		e = matcher.Event{Kind: matcher.EvAccepted, OrderID: a, LeavesQty: d}
		return seq, sym, e, reason == 0
	case 2:
		e = matcher.Event{Kind: matcher.EvRejected, OrderID: a, Reason: reason - 1}
		return seq, sym, e, reason >= 1 && reason <= 7
	case 3:
		e = matcher.Event{Kind: matcher.EvTrade, Maker: a, Taker: b, Price: c, Qty: d}
		return seq, sym, e, reason == 0
	case 4:
		e = matcher.Event{Kind: matcher.EvClosed, OrderID: a, Reason: reason - 1}
		return seq, sym, e, reason >= 1 && reason <= 3
	case 5:
		e = matcher.Event{Kind: matcher.EvReplaced, OrderID: a, Price: c, Qty: d}
		return seq, sym, e, reason == 0
	}
	return seq, sym, e, false
}

// ---- reading (recovery) --------------------------------------------------------------------

// JournalHeader is a journal file's header. Version is the binary journal
// version (JSONL reports 2); it is not part of SameHeader.
type JournalHeader struct {
	Kind       JournalKind
	Partition  uint32
	Partitions uint32
	Book       matcher.BookConfig
	Version    uint16
}

// SameHeader compares everything but the version.
func (h JournalHeader) SameHeader(o JournalHeader) bool {
	return h.Kind == o.Kind && h.Partition == o.Partition && h.Partitions == o.Partitions && SameBook(h.Book, o.Book)
}

func parseJsonlHeader(p, line string) (JournalHeader, error) {
	h := JournalHeader{Version: Version}
	if f, _ := Get(line, "format"); f != "orderer-journal/1" {
		return h, corrupt(p, "not an orderer-journal/1 header")
	}
	switch k, _ := Get(line, "kind"); k {
	case "cmd":
		h.Kind = KindCmd
	case "evt":
		h.Kind = KindEvt
	default:
		return h, corrupt(p, "bad kind")
	}
	part, ok := U64(line, "partition")
	if !ok || part > 0xFFFFFFFF {
		return h, corrupt(p, "bad partition")
	}
	parts, ok := U64(line, "partitions")
	if !ok || parts > 0xFFFFFFFF {
		return h, corrupt(p, "bad partitions")
	}
	h.Partition, h.Partitions = uint32(part), uint32(parts)
	if h.Book.PriceMin, ok = I64(line, "pmin"); !ok {
		return h, corrupt(p, "bad pmin")
	}
	if h.Book.PriceMax, ok = I64(line, "pmax"); !ok {
		return h, corrupt(p, "bad pmax")
	}
	mo, ok := U64(line, "max_orders")
	if !ok {
		return h, corrupt(p, "bad max_orders")
	}
	h.Book.MaxOrders = int(mo)
	switch ix, _ := Get(line, "index"); ix {
	case "ladder":
		h.Book.Index = matcher.IndexLadder
	case "tree":
		h.Book.Index = matcher.IndexTree
	default:
		return h, corrupt(p, "bad index")
	}
	return h, nil
}

func parseBinaryHeader(p string, b []byte) (JournalHeader, error) {
	var h JournalHeader
	if len(b) < Header || string(b[:4]) != "ORDJ" {
		return h, corrupt(p, "bad magic")
	}
	le := binary.LittleEndian
	h.Version = le.Uint16(b[4:])
	if h.Version != 1 && h.Version != 2 {
		return h, corrupt(p, "unsupported version")
	}
	switch b[6] {
	case 1:
		h.Kind = KindCmd
	case 2:
		h.Kind = KindEvt
	default:
		return h, corrupt(p, "bad kind")
	}
	if le.Uint32(b[16:]) != uint32(h.Kind.recordSize(h.Version)) {
		return h, corrupt(p, "bad record_size")
	}
	if b[7] > 1 {
		return h, corrupt(p, "bad index")
	}
	h.Partition, h.Partitions = le.Uint32(b[8:]), le.Uint32(b[12:])
	h.Book = matcher.BookConfig{PriceMin: int64(le.Uint64(b[24:])), PriceMax: int64(le.Uint64(b[32:])),
		MaxOrders: int(le.Uint64(b[40:])), Index: matcher.IndexKind(b[7])}
	return h, nil
}

func readFile(p string) ([]byte, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, corrupt(p, err.Error())
	}
	return b, nil
}

// ReadJournalHeader reads just a journal's header.
func ReadJournalHeader(p string, f JournalFormat) (JournalHeader, error) {
	b, err := readFile(p)
	if err != nil {
		return JournalHeader{}, err
	}
	if f == Binary {
		return parseBinaryHeader(p, b)
	}
	t := string(b)
	if i := strings.IndexByte(t, '\n'); i >= 0 {
		t = t[:i]
	}
	return parseJsonlHeader(p, t)
}

// ReadMode is strict (default) or repair reading (spec/JOURNAL.md §5, §5.1).
type ReadMode uint8

const (
	Strict ReadMode = iota
	Repair
)

// body is one file's records as byte ranges (binary records already checksum-checked).
type body struct {
	header   JournalHeader
	records  [][2]int // offset, length
	validLen int      // bytes a repair keeps
}

func splitBody(p string, b []byte, f JournalFormat, mode ReadMode) (body, error) {
	var out body
	if f == Binary {
		h, err := parseBinaryHeader(p, b)
		if err != nil {
			return out, err
		}
		out.header = h
		size := h.Kind.recordSize(h.Version)
		n := (len(b) - Header) / size
		if (len(b)-Header)%size != 0 && mode == Strict {
			return out, corrupt(p, "torn tail (partial record)")
		}
		if mode == Repair { // 1.3: zero records an interrupted write left (§5.1)
			for n > 0 && allZero(b[Header+(n-1)*size:Header+n*size]) {
				n--
			}
		}
		if h.Version >= 2 {
			for i := 0; i < n; i++ {
				at := Header + i*size
				if !sealed(b[at:at+size], h.Kind.payload()) {
					if mode == Repair && i+1 == n { // a torn final record (§5.1)
						n--
						break
					}
					return out, corrupt(p, fmt.Sprintf("record %d: checksum mismatch", i))
				}
			}
		}
		for i := 0; i < n; i++ {
			out.records = append(out.records, [2]int{Header + i*size, size})
		}
		out.validLen = Header + n*size
		return out, nil
	}
	end := len(b)
	if end > 0 && b[end-1] != '\n' {
		if mode == Strict {
			return out, corrupt(p, "torn tail (final line has no newline)")
		}
		for end > 0 && b[end-1] != '\n' {
			end--
		}
	}
	first := 0
	for first < end && b[first] != '\n' {
		first++
	}
	h, err := parseJsonlHeader(p, string(b[:first]))
	if err != nil {
		return out, err
	}
	out.header = h
	for pos := first + 1; pos < end; {
		e := pos
		for b[e] != '\n' {
			e++
		}
		out.records = append(out.records, [2]int{pos, e - pos})
		pos = e + 1
	}
	out.validLen = end
	return out, nil
}

func decodeCmds(p string, b []byte, f JournalFormat, bd body) ([]CmdRec, error) {
	recs := make([]CmdRec, 0, len(bd.records))
	for i, r := range bd.records {
		if f == Binary {
			rec, ok := decodeCmd(b[r[0]:])
			if !ok {
				return nil, corrupt(p, fmt.Sprintf("record %d: bad codes", i))
			}
			recs = append(recs, rec)
			continue
		}
		l := string(b[r[0] : r[0]+r[1]])
		iseq, ok1 := U64(l, "iseq")
		sym, ok2 := U64(l, "symbol")
		c, ok3 := ParseCommand(l)
		if !ok1 || !ok2 || sym > 0xFFFFFFFF || !ok3 {
			return nil, corrupt(p, fmt.Sprintf("line %d: malformed record: %s", i+2, l))
		}
		recs = append(recs, CmdRec{iseq, uint32(sym), c})
	}
	return recs, nil
}

func checkIncreasing(p string, recs []CmdRec, after *uint64) error {
	for _, r := range recs {
		if after != nil && r.Iseq <= *after {
			return corrupt(p, fmt.Sprintf("iseq not increasing (%d then %d)", *after, r.Iseq))
		}
		v := r.Iseq
		after = &v
	}
	return nil
}

// ReadCmdJournal reads one command journal file strictly.
func ReadCmdJournal(p string, f JournalFormat) (JournalHeader, []CmdRec, error) {
	b, err := readFile(p)
	if err != nil {
		return JournalHeader{}, nil, err
	}
	bd, err := splitBody(p, b, f, Strict)
	if err != nil {
		return bd.header, nil, err
	}
	if bd.header.Kind != KindCmd {
		return bd.header, nil, corrupt(p, "not a command journal")
	}
	recs, err := decodeCmds(p, b, f, bd)
	if err != nil {
		return bd.header, nil, err
	}
	return bd.header, recs, checkIncreasing(p, recs, nil)
}

// ReadCmdDir reads every partition's command journal in dir, all segments in
// order (spec/JOURNAL.md §1, §5 step 3).
func ReadCmdDir(dir string, f JournalFormat) (JournalHeader, [][]CmdRec, error) {
	segs := ListSegments(dir, KindCmd, f)
	if len(segs) == 0 {
		return JournalHeader{}, nil, corrupt(JournalPath(dir, KindCmd, 0, f), "no command journal")
	}
	h0, err := ReadJournalHeader(segs[0].Path, f)
	if err != nil {
		return h0, nil, err
	}
	all := make([][]CmdRec, h0.Partitions)
	seen := make([]bool, h0.Partitions)
	for _, s := range segs {
		h, recs, err := ReadCmdJournal(s.Path, f)
		if err != nil {
			return h0, nil, err
		}
		if h.Partition != s.Partition || s.Partition >= h0.Partitions || h.Partitions != h0.Partitions ||
			!SameBook(h.Book, h0.Book) {
			return h0, nil, corrupt(s.Path, "header does not match its file name, partition count or book config")
		}
		part := all[s.Partition]
		var after *uint64
		if len(part) > 0 {
			after = &part[len(part)-1].Iseq
		}
		if err := checkIncreasing(s.Path, recs, after); err != nil {
			return h0, nil, err
		}
		all[s.Partition] = append(part, recs...)
		seen[s.Partition] = true
	}
	for p, ok := range seen {
		if !ok {
			return h0, nil, corrupt(JournalPath(dir, KindCmd, uint32(p), f), "partition has no journal")
		}
	}
	return h0, all, nil
}

// ReadEvtJournal reads an event journal file as canonical symbol-tagged lines.
func ReadEvtJournal(p string, f JournalFormat) ([]string, error) {
	b, err := readFile(p)
	if err != nil {
		return nil, err
	}
	bd, err := splitBody(p, b, f, Strict)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(bd.records))
	var buf []byte
	for i, r := range bd.records {
		if f == Jsonl {
			out = append(out, string(b[r[0]:r[0]+r[1]]))
			continue
		}
		seq, sym, e, ok := decodeEvt(b[r[0]:])
		if !ok {
			return nil, corrupt(p, fmt.Sprintf("record %d: bad codes", i))
		}
		buf = buf[:0]
		e.WriteCanonicalSym(seq, sym, &buf)
		out = append(out, string(buf))
	}
	return out, nil
}

// ReadEvtPartition reads a partition's whole event journal (all segments, in order).
func ReadEvtPartition(dir string, f JournalFormat, p uint32) ([]string, error) {
	var out []string
	for _, s := range ListSegments(dir, KindEvt, f) {
		if s.Partition == p {
			l, err := ReadEvtJournal(s.Path, f)
			if err != nil {
				return nil, err
			}
			out = append(out, l...)
		}
	}
	return out, nil
}

// Repaired is one truncation RepairDir made.
type Repaired struct {
	Path  string
	Bytes int64
}

// RepairDir truncates a torn tail off each journal family's last segment, in
// place, after deleting trailing segments a crash left without a usable
// header (spec/JOURNAL.md §5.1, 1.3).
func RepairDir(dir string, f JournalFormat) ([]Repaired, error) {
	var out []Repaired
	for _, k := range []JournalKind{KindCmd, KindEvt} {
		segs := map[uint32][]Segment{}
		var parts []uint32
		for _, s := range ListSegments(dir, k, f) {
			if _, ok := segs[s.Partition]; !ok {
				parts = append(parts, s.Partition)
			}
			segs[s.Partition] = append(segs[s.Partition], s)
		}
		for _, p := range parts {
			ss := segs[p]
			sort.Slice(ss, func(i, j int) bool { return ss[i].Start < ss[j].Start })
			// drop trailing segments without a usable header; the segment
			// before becomes the last
			for len(ss) > 0 && ss[len(ss)-1].Start > 0 {
				path := ss[len(ss)-1].Path
				b, err := readFile(path)
				if err != nil {
					return out, err
				}
				if !headerless(path, b, f) {
					break
				}
				if err := os.Remove(path); err != nil {
					return out, corrupt(path, err.Error())
				}
				if d, err := os.Open(dir); err == nil {
					d.Sync()
					d.Close()
				}
				out = append(out, Repaired{path, int64(len(b))})
				ss = ss[:len(ss)-1]
			}
			// repair the last segment; while it holds no records, the one
			// before it too (its writer may still have been finishing it)
			for i := len(ss) - 1; i >= 0; i-- {
				path := ss[i].Path
				b, err := readFile(path)
				if err != nil {
					return out, err
				}
				bd, err := splitBody(path, b, f, Repair)
				if err != nil {
					return out, err
				}
				if bd.validLen < len(b) {
					if err := os.Truncate(path, int64(bd.validLen)); err != nil {
						return out, corrupt(path, err.Error())
					}
					if fh, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
						fh.Sync()
						fh.Close()
					}
					out = append(out, Repaired{path, int64(len(b) - bd.validLen)})
				}
				if len(bd.records) > 0 {
					break
				}
			}
		}
	}
	return out, nil
}

// headerless: a segment that cannot hold a record (spec/JOURNAL.md 1.3
// §5.1) — JSONL with no newline at all, or binary with an invalid header and
// nothing but zeros after it.
func headerless(path string, b []byte, f JournalFormat) bool {
	if f == Jsonl {
		return bytes.IndexByte(b, '\n') < 0
	}
	if len(b) > Header && !allZero(b[Header:]) {
		return false
	}
	_, err := parseBinaryHeader(path, b)
	return err != nil
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// MergeJournals is one iseq-ordered stream of records after `after`; iseqs must be disjoint.
func MergeJournals(parts [][]CmdRec, after uint64) ([]CmdRec, error) {
	var all []CmdRec
	for _, p := range parts {
		for _, r := range p {
			if r.Iseq > after {
				all = append(all, r)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Iseq < all[j].Iseq })
	for i := 1; i < len(all); i++ {
		if all[i].Iseq == all[i-1].Iseq {
			return nil, &CorruptJournal{fmt.Sprintf("iseq %d appears in two partitions", all[i].Iseq)}
		}
	}
	return all, nil
}

// ---- writing ---------------------------------------------------------------------------------

// openSegment creates segment `start` (truncating any old file) with its header.
func openSegment(dir string, f JournalFormat, k JournalKind, p, P uint32, book matcher.BookConfig, start uint64) (*os.File, error) {
	fh, err := os.OpenFile(SegmentPath(dir, k, p, start, f), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	var h []byte
	if f == Jsonl {
		h = []byte(jsonlHeader(k, p, P, book))
	} else {
		h = binaryHeader(k, p, P, book)
	}
	if _, err := fh.Write(h); err != nil {
		fh.Close()
		return nil, err
	}
	return fh, nil
}

// openJournal opens, in append mode, the partition's last segment (header
// checked); otherwise a fresh segment 0.
func openJournal(cfg *JournalConfig, k JournalKind, p, P uint32, book matcher.BookConfig) (*os.File, error) {
	if cfg.Append {
		var last string
		for _, s := range ListSegments(cfg.Dir, k, cfg.Format) {
			if s.Partition == p {
				last = s.Path
			}
		}
		if last != "" {
			h, err := ReadJournalHeader(last, cfg.Format)
			if err != nil {
				return nil, err
			}
			if !h.SameHeader(JournalHeader{Kind: k, Partition: p, Partitions: P, Book: book}) {
				return nil, corrupt(last, "header does not match the pipeline")
			}
			if cfg.Format == Binary && h.Version != Version {
				return nil, corrupt(last, "cannot append to a version-1 journal")
			}
			return os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0o644)
		}
	}
	return openSegment(cfg.Dir, cfg.Format, k, p, P, book, 0)
}

// removeCheckpointsBelow removes checkpoints with cut below n (body first).
func removeCheckpointsBelow(dir string, n uint64) error {
	for _, c := range ListCheckpoints(dir, false, n) {
		if err := os.Remove(c.Path); err != nil {
			return err
		}
		os.Remove(MetaPath(c.Path))
	}
	return nil
}

// removeSegmentsBelow removes segments that start below n (spec/JOURNAL.md §6 step 4).
func removeSegmentsBelow(dir string, f JournalFormat, n uint64) error {
	for _, k := range []JournalKind{KindCmd, KindEvt} {
		for _, s := range ListSegments(dir, k, f) {
			if s.Start < n {
				if err := os.Remove(s.Path); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// clearJournalDir: a fresh (non-append) pipeline owns its directory's journals.
func clearJournalDir(dir string, f JournalFormat) error {
	if err := removeSegmentsBelow(dir, f, ^uint64(0)); err != nil {
		return err
	}
	return removeCheckpointsBelow(dir, ^uint64(0))
}

// writeDurably writes contents to path: temporary name, sync, rename, sync the directory.
func writeDurably(path string, contents []byte) error {
	tmp := path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fh.Write(contents); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

const (
	chunkSize = 1 << 18
	chunks    = 64
)

type chunk struct {
	buf     []byte
	last    uint64 // id of the last record in the chunk
	records uint64
}

// ChunkWriter is the asynchronous journal writer: the owner encodes records
// into a chunk; full or idle chunks go to a dedicated I/O goroutine that
// writes and group-commits fsyncs (File.Sync: F_FULLFSYNC on macOS, fsync on
// Linux). Chunks are preallocated and recycled.
type ChunkWriter struct {
	f                *os.File
	fsync            *FsyncPolicy
	flushed, durable *atomic.Uint64
	io               *IoStats
	cs               []chunk
	cur              int
	last, records    uint64
	toIO, free       chan int
	rotations        chan *os.File
	done             chan struct{}
	finished         bool
	err              atomic.Pointer[string]
}

const (
	stopChunk   = -1
	rotateChunk = -2
)

func newChunkWriter(f *os.File, fsync *FsyncPolicy, flushed, durable *atomic.Uint64, io *IoStats) *ChunkWriter {
	w := &ChunkWriter{f: f, fsync: fsync, flushed: flushed, durable: durable, io: io, cs: make([]chunk, chunks),
		toIO: make(chan int, chunks+64), free: make(chan int, chunks+2), done: make(chan struct{}),
		rotations: make(chan *os.File, 64)}
	for i := range w.cs {
		w.cs[i].buf = make([]byte, 0, chunkSize)
		if i > 0 {
			w.free <- i
		}
	}
	go w.ioLoop()
	return w
}

// reserve makes room for one record of at most n bytes.
func (w *ChunkWriter) reserve(n int) {
	if len(w.cs[w.cur].buf)+n > chunkSize {
		w.handOff()
	}
}

func (w *ChunkWriter) pending() int { return len(w.cs[w.cur].buf) }

func (w *ChunkWriter) pushCmd(f JournalFormat, iseq uint64, sym uint32, c *matcher.Command) {
	w.reserve(maxRecord)
	ch := &w.cs[w.cur]
	if f == Binary {
		n := len(ch.buf)
		ch.buf = ch.buf[:n+CmdRecord]
		EncodeCmd(iseq, sym, c, ch.buf[n:])
	} else {
		ch.buf = append(AppendCmdLine(iseq, sym, c, ch.buf), '\n')
	}
	w.last = iseq
	w.records++
}

func (w *ChunkWriter) pushEvt(f JournalFormat, seq uint64, sym uint32, e *matcher.Event) {
	w.reserve(maxRecord)
	ch := &w.cs[w.cur]
	if f == Binary {
		n := len(ch.buf)
		ch.buf = ch.buf[:n+EvtRecord]
		EncodeEvt(seq, sym, e, ch.buf[n:])
	} else {
		e.WriteCanonicalSym(seq, sym, &ch.buf)
		ch.buf = append(ch.buf, '\n')
	}
	w.last = seq
	w.records++
}

// handOff hands the chunk to the I/O goroutine (blocks only when every chunk is in flight).
func (w *ChunkWriter) handOff() {
	ch := &w.cs[w.cur]
	if len(ch.buf) == 0 {
		return
	}
	ch.last, ch.records = w.last, w.records
	w.records = 0
	w.toIO <- w.cur
	w.cur = <-w.free
}

// rotate continues in next (a new segment, header written): everything so
// far goes to the current file, which the I/O goroutine syncs per policy and
// closes (spec/JOURNAL.md §6 step 2).
func (w *ChunkWriter) rotate(next *os.File) {
	w.handOff()
	w.rotations <- next
	w.toIO <- rotateChunk
}

// finish writes and (per policy) syncs everything; the first I/O error, if any.
func (w *ChunkWriter) finish() error {
	if !w.finished {
		w.finished = true
		w.handOff()
		w.toIO <- stopChunk
		<-w.done
		if err := w.f.Close(); err != nil {
			w.setErr("journal close: " + err.Error())
		}
	}
	if e := w.err.Load(); e != nil {
		return errors.New(*e)
	}
	return nil
}

func (w *ChunkWriter) setErr(s string) { w.err.CompareAndSwap(nil, &s) }

func (w *ChunkWriter) write(c int) {
	if _, err := w.f.Write(w.cs[c].buf); err != nil {
		w.setErr("journal write: " + err.Error())
	}
	w.cs[c].buf = w.cs[c].buf[:0]
}

func (w *ChunkWriter) sync(written uint64) {
	if w.err.Load() != nil {
		return
	}
	t0 := time.Now()
	if err := w.f.Sync(); err != nil {
		w.setErr("journal fsync: " + err.Error())
		return
	}
	w.io.record(uint64(time.Since(t0)))
	w.durable.Store(written)
}

func (w *ChunkWriter) ioLoop() {
	defer close(w.done)
	var unsynced uint64
	written := w.flushed.Load()
	lastSync := time.Now()
	idle := time.Duration(1<<63 - 1)
	if w.fsync != nil {
		idle = w.fsync.Idle
	}
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		var m int
		if unsynced > 0 {
			timer.Reset(idle)
			select {
			case m = <-w.toIO:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				w.sync(written)
				unsynced, lastSync = 0, time.Now()
				continue
			}
		} else {
			m = <-w.toIO
		}
		stop := false
	drain: // write everything queued, then one fsync decision
		for {
			if m == stopChunk {
				stop = true
				break
			}
			if m == rotateChunk {
				if unsynced > 0 && w.fsync != nil && w.fsync.Mode != FsyncNever {
					w.sync(written)
				}
				unsynced = 0
				if err := w.f.Close(); err != nil {
					w.setErr("journal close: " + err.Error())
				}
				w.f = <-w.rotations
				select {
				case m = <-w.toIO:
					continue
				default:
					break drain
				}
			}
			w.write(m)
			written = w.cs[m].last
			w.flushed.Store(written)
			if r := w.cs[m].records; r > 0 {
				unsynced += r
			} else {
				unsynced++
			}
			w.free <- m
			select {
			case m = <-w.toIO:
			default:
				break drain
			}
		}
		due := false
		switch {
		case w.fsync == nil || w.fsync.Mode == FsyncNever:
			w.durable.Store(written)
			unsynced = 0
		case w.fsync.Mode == FsyncEveryN:
			due = unsynced >= w.fsync.N
		default:
			due = time.Since(lastSync) >= w.fsync.Interval
		}
		if due || (stop && unsynced > 0) {
			w.sync(written)
			unsynced, lastSync = 0, time.Now()
		}
		if stop {
			return
		}
	}
}
