# VENDORED — upstream sources

orderer-go vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `ead0d43f8fd9e6ca5bdcfcf807049d4f2415e4a2`
- **tag**: `orderer-spec/1.2` (draft)
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`matcher/` is [matcher-go](https://github.com/abhijitkrm/matcher-go)'s root
package (every non-test `.go` file) at `46852c894e073e721e14202d9360495a91e07cc2`, byte for byte.
That commit includes two fixes found while building orderer-go:

- `53b222a`: `depth` sized its result by the requested count, so
  `RestingOrders` (which asks for every level) allocated 16 GiB per side, per book,
  per snapshot.
- `46852c8`: events are delivered through one reused `Event` per book
  instead of escaping to the heap: the pipeline's hot path went from 77
  bytes per command to zero.

matcher-go never had the OrderMap deletion bug fixed in matcher-rust and
matcher-cpp; `vectors/regress/001_dense_map_churn` pins that.

To check it:

```bash
for f in ../matcher-go/*.go; do case $f in *_test.go) ;; *) cmp "$f" "matcher/$(basename "$f")";; esac; done
git -C ../matcher-go diff --stat 46852c894e073e721e14202d9360495a91e07cc2 -- '*.go'
```

orderer's strict parsing (`flat.go`) wraps the core rather than changing
it. matcher-go's own parsing is lenient: malformed fields become 0. The
orderer harnesses must reject them (spec/HARNESS.md §5).
