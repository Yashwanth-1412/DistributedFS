#!/usr/bin/env bash
# Local demo: 1 coordinator + N storage nodes.
# Usage: ./run.sh <num_nodes> [chunk_size] [threads]
set -euo pipefail

N=${1:-4}
CHUNK=${2:-4194304}   # 4MB
THREADS=${3:-8}

ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/build"
COORD_PORT=9000
BASE_NODE_PORT=9100

echo "== building =="
cmake -S "$ROOT" -B "$BIN" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$BIN" -j >/dev/null

# clear any orphans from an aborted previous run (they'd hold the ports)
pkill -f dfs_coordinator 2>/dev/null || true
pkill -f dfs_node 2>/dev/null || true
sleep 0.2

cleanup() {
  echo "== stopping =="
  kill "${PIDS[@]}" $COORD_PID 2>/dev/null || true
  pkill -f dfs_node 2>/dev/null || true
}
trap cleanup EXIT INT TERM

rm -rf /tmp/dfs_data
mkdir -p /tmp/dfs_data

echo "== starting coordinator on :$COORD_PORT =="
"$BIN/dfs_coordinator" $COORD_PORT 5000 2 /tmp/dfs_data/coord.journal > /tmp/dfs_data/coord.log 2>&1 &
COORD_PID=$!

sleep 0.3

PIDS=()
for i in $(seq 1 "$N"); do
  PORT=$((BASE_NODE_PORT + i))
  "$BIN/dfs_node" 127.0.0.1 $COORD_PORT $PORT /tmp/dfs_data/node$i \
      > /tmp/dfs_data/node$i.log 2>&1 &
  PIDS+=($!)
done
echo "== started $N storage nodes (ports $((BASE_NODE_PORT+1))..$((BASE_NODE_PORT+N))) =="

sleep 0.5

# demo file
dd if=/dev/urandom of=/tmp/dfs_demo.bin bs=1M count=16 status=none
echo "== demo: PUT 16MB file =="
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT put /tmp/dfs_demo.bin demo.bin $CHUNK $THREADS

echo "== demo: GET =="
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT get demo.bin /tmp/dfs_demo.out $THREADS

cmp /tmp/dfs_demo.bin /tmp/dfs_demo.out && echo "OK: round-trip verified (identical bytes)"

echo "== bench: ${1:-4} nodes, 128MB, $THREADS threads, 1 client =="
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT bench 128 $CHUNK $THREADS 1

# done: the EXIT trap tears down coordinator + nodes
