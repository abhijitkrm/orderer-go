#!/usr/bin/env bash
# Mixed-language cluster test: real replica processes, any mix of orderer-rust
# (orderer-node), orderer-cpp (orderer_node) and orderer-go (orderernode),
# sharing one cluster, under load, with `kill -9` of the current leader several
# times. Passes only if every acknowledged batch survives on every replica, in
# the same order and exactly once, and all replicas (every language) end at the
# same state digest.
#
#   scripts/interop.sh                        # nodes: go rust go
#   IMPLS="go rust cpp" SECS=30 KILLS=6 scripts/interop.sh
#   ORDERER_KEY=secret scripts/interop.sh     # authenticated transport
#   COMPACT=100 DOWN=5 scripts/interop.sh     # force snapshot transfer between languages
#   SM=recorder scripts/interop.sh            # language-neutral recorder instead of the matching pipeline
#
# Needs ../orderer-rust (built here with cargo); ../orderer-cpp only when IMPLS names cpp.
set -euo pipefail

cd "$(dirname "$0")/.."
IMPLS=${IMPLS:-"go rust go"}
SECS=${SECS:-25}
KILLS=${KILLS:-5}
SM=${SM:-pipeline}
COMPACT=${COMPACT:-0}   # >0: snapshot and compact the log every N entries
DOWN=${DOWN:-1}         # seconds a killed leader stays down
BASE=${BASE_PORT:-48900}
RUST=${RUST_DIR:-../orderer-rust}
CPP=${CPP_DIR:-../orderer-cpp}

read -r -a KINDS <<< "$IMPLS"
N=${#KINDS[@]}

TMP=$(mktemp -d)
(cd "$RUST" && cargo build --release -q -p orderer-cluster --bin orderer-node --bin orderer-load)
RUST_NODE="$RUST/target/release/orderer-node"
LOAD="$RUST/target/release/orderer-load"
GO_NODE="$TMP/orderernode"
go build -o "$GO_NODE" ./cmd/orderernode
CPP_NODE="$CPP/build/orderer_node"
if [[ " $IMPLS " == *" cpp "* ]]; then
  cmake -S "$CPP" -B "$CPP/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "$CPP/build" --target orderer_node -j >/dev/null
fi

MEMBERS=""
CLIENTS=""
for ((i = 0; i < N; i++)); do
  MEMBERS+="${MEMBERS:+,}$i=127.0.0.1:$((BASE + i))"
  CLIENTS+="${CLIENTS:+,}127.0.0.1:$((BASE + 100 + i))"
done
declare -a PIDS
for ((i = 0; i < N; i++)); do PIDS[i]=0; done

cleanup() {
  for ((i = 0; i < N; i++)); do kill -9 "${PIDS[$i]}" 2>/dev/null || true; done
  [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null || true
  [[ -z "${KEEP:-}" ]] && rm -rf "$TMP"
}
trap cleanup EXIT

start_node() {
  local i=$1 bin
  case "${KINDS[$i]}" in
    cpp) bin=$CPP_NODE ;;
    go) bin=$GO_NODE ;;
    *) bin=$RUST_NODE ;;
  esac
  "$bin" --id "$i" --members "$MEMBERS" --client "127.0.0.1:$((BASE + 100 + i))" \
    --dir "$TMP/n$i" --audit --digest-every 64 --fsync on --sm "$SM" --compact-every "$COMPACT" >>"$TMP/n$i.log" 2>&1 &
  PIDS[i]=$!
}

leader() {
  "$LOAD" status --clients "$CLIENTS" 2>/dev/null | grep "role=Leader" | head -1 \
    | sed -E 's/.*id=([0-9]+).*/\1/'
}

for ((i = 0; i < N; i++)); do start_node "$i"; done
echo "started $N nodes (${IMPLS// /, }); logs in $TMP"
for _ in $(seq 1 100); do [[ -n "$(leader)" ]] && break; sleep 0.1; done
[[ -n "$(leader)" ]] || { echo "FAIL: no leader elected"; exit 1; }

"$LOAD" run --clients "$CLIENTS" --secs "$SECS" --acked "$TMP/acked" --maybe "$TMP/maybe" \
  --batch 50 --threads 3 >"$TMP/load.out" 2>&1 &
LOAD_PID=$!

for k in $(seq 1 "$KILLS"); do
  sleep $(( (SECS - 4) / (KILLS + 1) + 1 ))
  L=$(leader || true)
  [[ -z "$L" ]] && { sleep 1; L=$(leader || true); }
  [[ -z "$L" ]] && { echo "FAIL: no leader before kill $k"; exit 1; }
  echo "kill $k/$KILLS: kill -9 leader node $L (${KINDS[$L]})"
  kill -9 "${PIDS[$L]}"
  wait "${PIDS[$L]}" 2>/dev/null || true
  sleep "$DOWN"
  start_node "$L"
done

wait "$LOAD_PID"
LOAD_PID=
cat "$TMP/load.out"

"$LOAD" settle --clients "$CLIENTS" --timeout 90
if [[ "$COMPACT" -gt 0 ]]; then
  # After a snapshot restore a node's audit log lacks the entries the snapshot
  # covers, so the per-batch comparison does not apply; the state digests
  # (settle, above) are the check. Show that logs were compacted.
  "$LOAD" status --clients "$CLIENTS"
  SAME=0; CROSS=0
  for ((i = 0; i < N; i++)); do
    while read -r from; do
      [[ -z "$from" ]] && continue
      if [[ "${KINDS[$i]}" == "${KINDS[$from]}" ]]; then SAME=$((SAME + 1)); else CROSS=$((CROSS + 1)); fi
    done < <(sed -n -E 's/.*installing snapshot [0-9]+ from node ([0-9]+).*/\1/p' "$TMP/n$i.log")
  done
  echo "snapshot installs: $SAME between same-language replicas, $CROSS across languages"
  BASES=$("$LOAD" status --clients "$CLIENTS" | sed -E 's/.* base=([0-9]+) .*/\1/' | sort -n | tail -1)
  [[ "$BASES" -gt 0 ]] || { echo "FAIL: no node compacted its log"; exit 1; }
  if [[ "${REQUIRE_CROSS:-0}" == 1 && "$CROSS" -eq 0 ]]; then echo "FAIL: no snapshot crossed a language boundary"; exit 1; fi
  echo "PASS: all replicas, across languages, hold the same state digest after snapshot transfer (highest log base $BASES)"
  exit 0
fi
APPLIED=""
for ((i = 0; i < N; i++)); do APPLIED+="${APPLIED:+,}$TMP/n$i/applied.log"; done
"$LOAD" verify --acked "$TMP/acked" --maybe "$TMP/maybe" --applied "$APPLIED"
