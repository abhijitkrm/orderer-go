#!/usr/bin/env bash
# build-harness.sh — spec/HARNESS.md §6: build the five tools into
# harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}.
#
#   scripts/build-harness.sh            # optimized
#   CHECKED=1 scripts/build-harness.sh  # with the race detector (HARNESS.md §4.2)
set -euo pipefail
cd "$(dirname "$0")/.."
FLAGS=()
[ "${CHECKED:-0}" = 1 ] && FLAGS=(-race)
mkdir -p harness/bin
for t in orderrun ordererfuzz orderrecover ordersnap orderbench; do
  go build ${FLAGS[@]+"${FLAGS[@]}"} -o "harness/bin/$t" "./cmd/$t"
done
