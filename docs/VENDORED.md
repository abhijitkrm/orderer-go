# VENDORED — upstream sources

orderer-go vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `f54438147277b8834eba0b3facde71b5356f81c7`
- **tag**: `orderer-spec/1.3`
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`matcher/` is [matcher-go](https://github.com/abhijitkrm/matcher-go)'s root
package (every non-test `.go` file) at `81542bee88408fb2c0e15c6b470f6786d5e0ff03`, byte for byte.
That commit includes three fixes found while building orderer-go:

- `53b222a`: `depth` sized its result by the requested count, so
  `RestingOrders` (which asks for every level) allocated 16 GiB per side, per book,
  per snapshot.
- `46852c8`: events are delivered through one reused `Event` per book
  instead of escaping to the heap: the pipeline's hot path went from 77
  bytes per command to zero.
- `81542be`: the ladder rescans the next best price through a summary bitmap, and an emptied side resets the cursor at once (it used to scan the whole ladder).

matcher-go never had the OrderMap deletion bug fixed in matcher-rust and
matcher-cpp; `vectors/regress/001_dense_map_churn` pins that.

To check it:

```bash
for f in ../matcher-go/*.go; do case $f in *_test.go) ;; *) cmp "$f" "matcher/$(basename "$f")";; esac; done
git -C ../matcher-go diff --stat 81542bee88408fb2c0e15c6b470f6786d5e0ff03 -- '*.go'
```

orderer's strict parsing (`flat.go`) wraps the core rather than changing
it. matcher-go's own parsing is lenient: malformed fields become 0. The
orderer harnesses must reject them (spec/HARNESS.md §5).
