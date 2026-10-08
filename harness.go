package orderer

// Shared plumbing for the spec/HARNESS.md tools (cmd/*).

import (
	"fmt"
	"os"
	"strings"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// Die reports a usage / input / config / corruption error: exit 2 (spec/HARNESS.md §5).
func Die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

// Fail reports an internal failure: exit 1.
func Fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// Corpus is a parsed command file (spec/HARNESS.md §1).
type Corpus struct {
	Book   matcher.BookConfig
	Engine bool
	Cmds   []SymCmd
}

// ReadText reads a whole file or dies.
func ReadText(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		Die(path + ": cannot read")
	}
	return string(b)
}

// ParseCorpus parses a command file strictly.
func ParseCorpus(text, path string) (*Corpus, error) {
	lines := strings.Split(text, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s: empty file", path)
	}
	hdr := strings.TrimSuffix(lines[0], "\r")
	c := &Corpus{Book: ParseHeader(hdr)}
	e, _ := Get(hdr, "engine")
	c.Engine = e == "true"
	c.Cmds = make([]SymCmd, 0, len(lines))
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSuffix(lines[i], "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		cmd, ok := ParseCommand(line)
		if !ok {
			return nil, fmt.Errorf("%s:%d: malformed command: %s", path, i+1, line)
		}
		var sym uint32
		if c.Engine {
			s, ok := U64(line, "symbol")
			if !ok || s > 0xFFFFFFFF {
				return nil, fmt.Errorf("%s:%d: missing symbol: %s", path, i+1, line)
			}
			sym = uint32(s)
		}
		c.Cmds = append(c.Cmds, SymCmd{sym, cmd})
	}
	return c, nil
}

// LoadCorpus reads and parses a command file or dies.
func LoadCorpus(path string) *Corpus {
	c, err := ParseCorpus(ReadText(path), path)
	if err != nil {
		Die(err.Error())
	}
	return c
}

// Args is a minimal argv parser: positionals plus --flag / --opt value.
type Args struct {
	Positional []string
	opts       map[string]string
	flags      map[string]bool
}

func ParseArgs(argv []string, usage string, valued, flags []string) *Args {
	a := &Args{opts: map[string]string{}, flags: map[string]bool{}}
	has := func(v []string, s string) bool {
		for _, x := range v {
			if x == s {
				return true
			}
		}
		return false
	}
	for i := 0; i < len(argv); i++ {
		s := argv[i]
		switch {
		case !strings.HasPrefix(s, "--"):
			a.Positional = append(a.Positional, s)
		case has(valued, s):
			if i+1 >= len(argv) {
				Die(s + " needs a value\nusage: " + usage)
			}
			i++
			a.opts[s] = argv[i]
		case has(flags, s):
			a.flags[s] = true
		default:
			Die("unknown option " + s + "\nusage: " + usage)
		}
	}
	return a
}

func (a *Args) Get(k string) (string, bool) { v, ok := a.opts[k]; return v, ok }
func (a *Args) Flag(k string) bool          { return a.flags[k] }

// Num parses an unsigned option no larger than max, or dies.
func (a *Args) Num(k string, def, max uint64) uint64 {
	v, ok := a.opts[k]
	if !ok {
		return def
	}
	n, ok := ParseU64(v)
	if !ok || n > max {
		Die(k + ": not a number: " + v)
	}
	return n
}

var (
	CommonValued = []string{"--partitions", "--partition-map", "--journal-dir"}
	CommonFlags  = []string{"--binary"}
)

// PartitionMapArg builds the map from --partitions / --partition-map, or dies.
func PartitionMapArg(a *Args) *PartitionMap {
	p := uint32(a.Num("--partitions", 1, 0xFFFFFFFF))
	if path, ok := a.Get("--partition-map"); ok {
		m, err := ParsePartitionTable(ReadText(path), p)
		if err != nil {
			Die(path + ": " + err.Error())
		}
		return m
	}
	m, err := NewPartitionMap(p, nil)
	if err != nil {
		Die(err.Error())
	}
	return m
}

// Common is the options every harness tool shares.
type Common struct {
	Map     *PartitionMap
	Journal *JournalConfig
}

func CommonArgs(a *Args) Common {
	_, hasDir := a.Get("--journal-dir")
	if a.Flag("--binary") && !hasDir {
		Die("--binary requires --journal-dir")
	}
	c := Common{Map: PartitionMapArg(a)}
	if d, ok := a.Get("--journal-dir"); ok {
		f := Jsonl
		if a.Flag("--binary") {
			f = Binary
		}
		j := NewJournalConfig(d, f)
		j.Fsync = FsyncNeverPolicy() // harness runs need complete files, not power-loss safety
		c.Journal = &j
	}
	return c
}

// RunCorpus runs a corpus through a fresh pipeline (one producer, file order),
// drains, optionally snapshots, and shuts down. It returns the spec/HARNESS.md §3 listing.
func RunCorpus(c *Corpus, com Common, tagged, snapshot bool) (string, *Snapshot) {
	f, events := Collect(tagged)
	b := NewBuilder(NewFifoCore).BookConfig(c.Book).PartitionMap(com.Map).Egress(f)
	if com.Journal != nil {
		b.Journal(*com.Journal)
	}
	p, err := b.Build()
	if err != nil {
		Die(err.Error())
	}
	if p.PublishBatch(c.Cmds) != Ok {
		Fail("pipeline closed")
	}
	if err := p.Drain(); err != nil {
		Fail(err.Error())
	}
	var snap *Snapshot
	if snapshot {
		if snap, err = p.Snapshot(); err != nil {
			Fail(err.Error())
		}
	}
	if err := p.Shutdown(); err != nil {
		Fail(err.Error())
	}
	return events.Listing(), snap
}

// Print writes s to stdout, exiting quietly on a closed pipe.
func Print(s string) {
	if _, err := os.Stdout.WriteString(s); err != nil {
		os.Exit(0)
	}
}
