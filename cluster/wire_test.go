package cluster

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/abhijitkrm/orderer-go/consensus"
	"github.com/abhijitkrm/orderer-go/matcher"
)

// The runtime's storage and wire layer against fixtures written by orderer-rust
// (vectors/replication-wire, from `wirefix`): messages, WAL frame bodies,
// command batches, WAL files in every damaged state, and snapshots. Decoding
// must give the same values, re-encoding the same bytes, recovery the same
// disk image, and files written here must equal the Rust files byte for byte.

const wireDir = "../vectors/replication-wire"

func hexs(d []byte) string {
	if len(d) == 0 {
		return "-"
	}
	return fmt.Sprintf("%x", d)
}

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	if s == "-" || s == "" {
		return nil
	}
	b := make([]byte, len(s)/2)
	for i := range b {
		v, err := strconv.ParseUint(s[2*i:2*i+2], 16, 8)
		if err != nil {
			t.Fatalf("bad hex %q", s)
		}
		b[i] = byte(v)
	}
	return b
}

func fmtEntries(es []consensus.Entry) string {
	if len(es) == 0 {
		return "-"
	}
	p := make([]string, len(es))
	for i, e := range es {
		p[i] = fmt.Sprintf("%d:%s", e.Term, hexs(e.Data))
	}
	return strings.Join(p, ",")
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func fmtMsg(m consensus.Msg) string {
	switch m.Kind {
	case consensus.MsgVote:
		return fmt.Sprintf("vote %d %d %d %d", m.Term, m.LastIndex, m.LastTerm, b2i(m.Pre))
	case consensus.MsgVoteReply:
		return fmt.Sprintf("votereply %d %d %d", m.Term, b2i(m.Granted), b2i(m.Pre))
	case consensus.MsgAppend:
		return fmt.Sprintf("append %d %d %d %d %s", m.Term, m.PrevIndex, m.PrevTerm, m.Commit, fmtEntries(m.Entries))
	default:
		return fmt.Sprintf("appendreply %d %d %d", m.Term, b2i(m.Success), m.Index)
	}
}

func fmtDisk(d consensus.Disk) string {
	v := "-"
	if d.VotedFor != consensus.None {
		v = strconv.Itoa(d.VotedFor)
	}
	return fmt.Sprintf("%d:%s:%d:%d:%s", d.Term, v, d.Base, d.BaseTerm, fmtEntries(d.Log))
}

func fixtureLines(t testing.TB, name string) [][2]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(wireDir, name))
	if err != nil {
		t.Fatal(err)
	}
	var out [][2]string
	for _, line := range strings.Split(string(data), "\n") {
		if i := strings.Index(line, " | "); i >= 0 {
			out = append(out, [2]string{line[:i], line[i+3:]})
		}
	}
	return out
}

func slurp(t testing.TB, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCRCMatchesTheCheckValue(t *testing.T) {
	if got := crc32c([]byte("123456789")); got != 0xE3069283 {
		t.Fatalf("crc32c check value: %08x", got)
	}
	data := make([]byte, 5000)
	rand.New(rand.NewSource(7)).Read(data)
	for _, step := range []int{1, 3, 8, 13, 999} {
		var c uint32
		for i := 0; i < len(data); i += step {
			c = crc32cUpdate(c, data[i:min(i+step, len(data))])
		}
		if c != crc32c(data) {
			t.Fatalf("incremental crc, step %d", step)
		}
	}
}

func TestSessionsDetectTamperingReplayReorderingAndReflection(t *testing.T) {
	var k Digest
	for i := range k {
		k[i] = 7
	}
	a, b := newSession(k, 0), newSession(k, 1)
	one, two, twp := []byte("one"), []byte("two"), []byte("twp")
	m1 := a.seal(1, one)
	m2 := a.seal(1, two)
	if !b.open(1, one, m1) {
		t.Fatal("first frame rejected")
	}
	if b.open(1, one, m1) {
		t.Fatal("replay accepted")
	}
	if b.open(1, twp, m2) {
		t.Fatal("altered body accepted")
	}
	if b.open(2, two, m2) {
		t.Fatal("altered kind accepted")
	}
	if !b.open(1, two, m2) {
		t.Fatal("rejections must not advance state")
	}
	a.seal(1, one)
	m4 := a.seal(1, one)
	if b.open(1, one, m4) {
		t.Fatal("a dropped frame went unnoticed")
	}
	a2 := newSession(k, 0)
	if m := a2.seal(1, one); a2.open(1, one, m) {
		t.Fatal("reflection accepted")
	}
	k2 := k
	k2[0] = 8
	if newSession(k2, 1).open(1, one, newSession(k, 0).seal(1, one)) {
		t.Fatal("wrong key accepted")
	}
}

func TestMessagesDecodeAndReEncodeExactlyAsRustWroteThem(t *testing.T) {
	lines := fixtureLines(t, "msgs.txt")
	if len(lines) < 8 {
		t.Fatalf("only %d message fixtures", len(lines))
	}
	for _, l := range lines {
		bytesIn := unhex(t, l[0])
		m, err := DecodeMsg(bytesIn)
		if err != nil {
			t.Fatalf("%s: %v", l[1], err)
		}
		if got := fmtMsg(m); got != l[1] {
			t.Fatalf("decoded %q, want %q", got, l[1])
		}
		if again := EncodeMsg(m, nil); !bytes.Equal(again, bytesIn) {
			t.Fatalf("%s: re-encoded differently", l[1])
		}
		for n := 0; n < len(bytesIn); n++ {
			if _, err := DecodeMsg(bytesIn[:n]); err == nil {
				t.Fatalf("%s: prefix of %d bytes decoded", l[1], n)
			}
		}
	}
}

func TestHostileInputNeverCrashesTheDecoders(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 20000; i++ {
		junk := make([]byte, rng.Intn(64))
		rng.Read(junk)
		DecodeMsg(junk)
		decodePersist(junk)
		DecodeBatch(junk)
	}
	// A hostile entry count must not allocate.
	evil := append([]byte{3}, make([]byte, 24)...)
	evil = putU32(evil, 0xFFFFFFFF)
	if _, err := DecodeMsg(evil); err == nil {
		t.Fatal("hostile entry count accepted")
	}
}

func TestWalFrameBodiesRoundTrip(t *testing.T) {
	for _, l := range fixtureLines(t, "persists.txt") {
		body := unhex(t, l[0])
		p, err := decodePersist(body)
		if err != nil {
			t.Fatalf("%s: %v", l[1], err)
		}
		vote, comp := "-", "-"
		if p.VotedFor != consensus.None {
			vote = strconv.Itoa(p.VotedFor)
		}
		if p.HasCompact {
			comp = fmt.Sprintf("%d:%d", p.CompactIdx, p.CompactTrm)
		}
		if got := fmt.Sprintf("%d %s %d %s %s", p.Term, vote, p.From, comp, fmtEntries(p.Entries)); got != l[1] {
			t.Fatalf("decoded %q, want %q", got, l[1])
		}
		if !bytes.Equal(encodePersist(p, nil), body) {
			t.Fatalf("%s: re-encoded differently", l[1])
		}
	}
}

func TestCommandBatchesRoundTripIncludingSessions(t *testing.T) {
	tifs := []string{"gtc", "ioc", "fok", "post_only"}
	for _, l := range fixtureLines(t, "batches.txt") {
		body := unhex(t, l[0])
		words := strings.Fields(l[1])
		cmds, err := DecodeBatch(body)
		if err != nil {
			t.Fatalf("%s: %v", l[0], err)
		}
		s, hasSession := SessionOf(body)
		if words[0] == "-" {
			if hasSession {
				t.Fatalf("plain batch decoded as a session")
			}
		} else if c, _ := strconv.ParseUint(words[0], 10, 64); !hasSession || s.Client != c || s.Seq != u64(words[1]) {
			t.Fatalf("session %+v, want %s %s", s, words[0], words[1])
		}
		want := words[3:] // "client seq |" then commands
		if len(cmds) != len(want) {
			t.Fatalf("%d commands, want %d", len(cmds), len(want))
		}
		for i, sc := range cmds {
			c := sc.Cmd
			kind := map[matcher.CmdKind]string{matcher.CmdNew: "new", matcher.CmdCancel: "cancel", matcher.CmdReplace: "replace"}[c.Kind]
			got := fmt.Sprintf("%d:%s:", sc.Sym, kind)
			if c.Kind == matcher.CmdNew {
				side, ot := "bid", "limit"
				if c.Side == matcher.Ask {
					side = "ask"
				}
				if c.OType == matcher.Market {
					ot = "market"
				}
				got += fmt.Sprintf("%s:%s:%s", side, ot, tifs[c.Tif])
			} else {
				got += "-:-:-"
			}
			got += fmt.Sprintf(":%d:%d:%d", c.OrderID, c.Price, c.Qty)
			if got != want[i] {
				t.Fatalf("command %d: %s, want %s", i, got, want[i])
			}
		}
		if len(body) > 0 {
			var sk *SessionKey
			if hasSession {
				sk = &s
			}
			if !bytes.Equal(EncodeBatch(cmds, nil, sk), body) {
				t.Fatalf("re-encoded %s differently", l[0][:min(40, len(l[0]))])
			}
		}
	}
}

func u64(s string) uint64 { v, _ := strconv.ParseUint(s, 10, 64); return v }

func TestWalRecoveryMatchesRustForEveryDamagePattern(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join(wireDir, "wal-*.bin"))
	sort.Strings(files)
	if len(files) < 15 {
		t.Fatalf("only %d WAL fixtures", len(files))
	}
	for _, f := range files {
		tag := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "wal-"), ".bin")
		want := strings.TrimRight(string(slurp(t, filepath.Join(wireDir, "wal-"+tag+".expect"))), "\n")
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "raft.wal"), slurp(t, f), 0o644); err != nil {
			t.Fatal(err)
		}
		var got string
		w, disk, rep, err := OpenWal(dir)
		if err != nil {
			we := err.(*WalError)
			switch we.Kind {
			case WalCorrupt:
				got = fmt.Sprintf("corrupt %d", we.Offset)
			case WalBadHeader:
				got = "badheader"
			default:
				got = "io " + we.Msg
			}
		} else {
			got = fmt.Sprintf("ok %s frames=%d torn=%d", fmtDisk(disk), rep.Frames, rep.TornBytes)
			w.Close()
		}
		if got != want {
			t.Errorf("%s:\n  want %s\n  got  %s", tag, want, got)
		}
	}
}

func TestAWalWrittenHereIsByteIdenticalToTheOneRustWrote(t *testing.T) {
	e := func(term consensus.Term, d ...byte) consensus.Entry { return consensus.Entry{Term: term, Data: d} }
	mk := func(term consensus.Term, vote int, from consensus.Index, es []consensus.Entry, comp *[2]uint64) consensus.Persist {
		p := consensus.Persist{Term: term, VotedFor: vote, From: from, Entries: es}
		if comp != nil {
			p.HasCompact, p.CompactIdx, p.CompactTrm = true, consensus.Index(comp[0]), consensus.Term(comp[1])
		}
		return p
	}
	dir := t.TempDir()
	w, _, _, err := OpenWal(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(mk(1, 1, 1, []consensus.Entry{e(1, 1), e(1, 2), e(1, 3)}, nil))
	w.Append(mk(2, 2, 3, []consensus.Entry{e(2, 9, 9)}, nil))
	w.Append(mk(2, 2, 4, []consensus.Entry{e(2), e(2, 4)}, nil))
	w.Append(mk(3, consensus.None, 6, nil, &[2]uint64{3, 2}))
	if err := w.Sync(true); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if !bytes.Equal(slurp(t, filepath.Join(dir, "raft.wal")), slurp(t, filepath.Join(wireDir, "wal-good.bin"))) {
		t.Fatal("WAL bytes differ from Rust's")
	}
	// Appending after recovery lands cleanly; a rewrite keeps the state.
	w, _, _, err = OpenWal(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(mk(3, consensus.None, 6, []consensus.Entry{e(3, 7)}, nil))
	w.Sync(true)
	if err := w.Rewrite(); err != nil {
		t.Fatal(err)
	}
	w.Append(mk(3, consensus.None, 7, []consensus.Entry{e(3, 8)}, nil))
	w.Sync(true)
	w.Close()
	w, disk, rep, err := OpenWal(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if disk.Base != 3 || disk.BaseTerm != 2 || len(disk.Log) != 4 || rep.TornBytes != 0 {
		t.Fatalf("recovered %s, torn %d", fmtDisk(disk), rep.TornBytes)
	}
}

func TestSnapshotsReadAndWriteIdenticallyToRust(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenSnapStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []string{"42", "7"} {
		f := strings.Fields(string(slurp(t, filepath.Join(wireDir, "snap-"+idx+".expect"))))
		index, term, n := consensus.Index(u64(f[0])), consensus.Term(u64(f[1])), u64(f[2])
		var data []byte
		if len(f) > 4 {
			data = unhex(t, f[4])
		}
		name := fmt.Sprintf("snap-%020d.snap", index)
		if err := os.WriteFile(filepath.Join(dir, name), slurp(t, filepath.Join(wireDir, name)), 0o644); err != nil {
			t.Fatal(err)
		}
		m, got, ok := store.Get(index)
		if !ok || m.Term != term || m.Len != n || !bytes.Equal(got, data) {
			t.Fatalf("snapshot %s: ok=%v meta=%+v", idx, ok, m)
		}
		// Write the same snapshot here: the file must equal Rust's.
		s2, _ := OpenSnapStore(t.TempDir())
		if err := s2.Put(index, term, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(slurp(t, s2.path(index)), slurp(t, filepath.Join(wireDir, name))) {
			t.Fatalf("snapshot %s differs from Rust's", idx)
		}
	}
	if l := store.List(); len(l) != 2 || l[0] != 7 || l[1] != 42 {
		t.Fatalf("list: %v", l)
	}
	// Corruption and truncation are detected.
	p := store.path(42)
	b := slurp(t, p)
	b[len(b)-1] ^= 1
	os.WriteFile(p, b, 0o644)
	if _, _, ok := store.Get(42); ok {
		t.Fatal("bit flip not detected")
	}
	os.WriteFile(p, b[:len(b)-3], 0o644)
	if _, _, ok := store.Get(42); ok {
		t.Fatal("truncation not detected")
	}
	// Streaming receive verifies length and checksum.
	blob := make([]byte, 100000)
	for i := range blob {
		blob[i] = byte(i)
	}
	crc := crc32c(blob)
	in, err := store.Receive(9, 1, uint64(len(blob)), crc)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(blob); i += 4096 {
		if err := in.Write(blob[i:min(i+4096, len(blob))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := in.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, got, ok := store.Get(9); !ok || !bytes.Equal(got, blob) {
		t.Fatal("received snapshot unreadable")
	}
	bad, _ := store.Receive(10, 1, 4, crc)
	bad.Write([]byte("abcd"))
	if bad.Finish() == nil {
		t.Fatal("checksum mismatch accepted")
	}
	if _, _, ok := store.Get(10); ok {
		t.Fatal("rejected snapshot was published")
	}
}
