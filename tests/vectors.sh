#!/usr/bin/env bash
# vectors.sh — conformance with the vendored orderer vectors, through the
# harness binaries (spec/HARNESS.md CLI contract): pipeline listings and
# per-partition journals byte-exact in both encodings at P=1,2,4;
# journal-form recovery; snapshot + cross-P recovery; exit codes.
#   tests/vectors.sh <bin-dir> <vectors-dir>
set -uo pipefail
BIN=$1; V=$2
W=$(mktemp -d /tmp/orderer-go-vectors.XXXXXX)
trap 'rm -rf "$W"' EXIT
fail=0
bad() { echo "FAIL $*"; fail=1; }
bysym() { awk 'match($0, /"symbol":[0-9]+/) { print substr($0, RSTART+9, RLENGTH-9) "\t" $0 }' "$1" | sort -s -n -k1,1 | cut -f2-; }

for input in multisymbol fuzz_s11; do
  for P in 1 2 4; do
    want=$V/pipeline/$input/P$P
    for sub in jsonl binary; do
      flag=""; [ $sub = binary ] && flag=--binary
      "$BIN/orderrun" "$V/pipeline/$input.cmd.jsonl" --partitions $P --journal-dir "$W/$sub" $flag > "$W/listing" \
        || bad "$input P=$P $sub: orderrun failed"
      cmp -s "$W/listing" "$want/listing.evt" || bad "$input P=$P $sub: listing"
      diff -r -q "$W/$sub" "$want/$sub" > /dev/null || bad "$input P=$P $sub: journals differ byte-for-byte"
      "$BIN/orderrecover" --journal-dir "$W/$sub" $flag --partitions $P > "$W/rec" || bad "$input P=$P $sub: recover failed"
      cmp -s <(bysym "$W/rec") <(bysym "$W/listing") || bad "$input P=$P $sub: journal-form recovery"
      rm -rf "$W/$sub"
    done
  done
  echo "ok   pipeline/$input"
done

d=$V/recovery/fuzz_s11
"$BIN/orderrun" "$d/prefix.cmd.jsonl" --partitions 3 --snap "$W/prefix.snap" > /dev/null
cmp -s "$W/prefix.snap" "$d/prefix.snap" || bad "recovery: snapshot body"
cmp -s "$W/prefix.snap.meta" "$d/prefix.snap.meta" || bad "recovery: snapshot sidecar"
for P in 1 3; do
  "$BIN/orderrecover" "$d/prefix.snap" "$d/tail.cmd.jsonl" --partitions $P | cmp -s - "$d/recov.P$P.evt" \
    || bad "recovery at P=$P"
done
echo "ok   recovery/fuzz_s11"

for f in "$V"/regress/*.cmd.jsonl; do
  "$BIN/ordererfuzz" "$f" | cmp -s - <(tail -n +2 "${f%.cmd.jsonl}.evt.jsonl") || bad "regress $(basename "$f")"
done
echo "ok   regress"

expect2() { "$@" > /dev/null 2>&1; [ $? = 2 ] || bad "exit code != 2: $*"; }
in=$V/pipeline/multisymbol.cmd.jsonl
expect2 "$BIN/orderrun"
expect2 "$BIN/orderrun" "$in" --bogus
expect2 "$BIN/orderrun" "$in" --partitions 0
expect2 "$BIN/orderrun" "$in" --partitions 3 --partition-map "$V/routing/table.map.jsonl"
expect2 "$BIN/ordererfuzz" "$in" --binary
expect2 "$BIN/orderrun" /nonexistent/file
printf '{"cmd":"cancel","symbol":1,"order_id":5}\n{"cmd":"new","sym' > "$W/trunc"
expect2 "$BIN/orderrecover" "$d/prefix.snap" "$W/trunc"
expect2 "$BIN/orderrecover" --journal-dir "$W/empty"
echo "ok   exit codes"
[ $fail = 0 ] && echo "vectors: all passed" || { echo "vectors: FAILURES"; exit 1; }
