# VENDORED — upstream sources

orderer-go vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `41019c69abadfcbee131bc17debd5154aea9608e`
- **tag**: `orderer-spec/1.1`
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`matcher/` is [matcher-go](https://github.com/abhijitkrm/matcher-go)'s root
package (every non-test `.go` file) at `53b222afd1d408a036a0594ee658dde41a6814b0`, byte for byte.
That commit includes one fix found while building orderer-go:

- `53b222a`: `depth` sized its result by the requested count, so
  `RestingOrders` (which asks for every level) allocated 24 GB per book
  per snapshot.

matcher-go never had the OrderMap deletion bug fixed in matcher-rust and
matcher-cpp; `vectors/regress/001_dense_map_churn` pins that.

To check it:

```bash
for f in ../matcher-go/*.go; do case $f in *_test.go) ;; *) cmp "$f" "matcher/$(basename "$f")";; esac; done
git -C ../matcher-go diff --stat 53b222afd1d408a036a0594ee658dde41a6814b0 -- '*.go'
```

orderer's strict parsing (`flat.go`) wraps the core rather than changing
it. matcher-go's own parsing is lenient: malformed fields become 0. The
orderer harnesses must reject them (spec/HARNESS.md §5).
