#!/usr/bin/env bash
# Cold / disk-bound benchmark. /tmp is a tmpfs, so node data must live on the
# NVMe (here: /home). Measures: buffered PUT, hot GET, cold GET (drop_caches),
# and durable PUT (fsync before ack).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/build"
COORD_PORT=9000
BASE_NODE_PORT=9100
N=${N:-4}
CHUNK=${CHUNK:-4194304}
THREADS=${THREADS:-16}
SIZE_MB=${SIZE_MB:-256}
DATA_ROOT=${DATA_ROOT:-/home/yami/dfs_bench}   # NVMe, NOT tmpfs

pkill -f dfs_ 2>/dev/null || true
sleep 0.2

echo "== building =="
cmake -S "$ROOT" -B "$BIN" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$BIN" -j >/dev/null

rm -rf "$DATA_ROOT"; mkdir -p "$DATA_ROOT/nodes"
dd if=/dev/urandom of="$DATA_ROOT/src.bin" bs=1M count=$SIZE_MB status=none

start_coord() {
  "$BIN/dfs_coordinator" $COORD_PORT 5000 2 "$DATA_ROOT/coord.journal" >"$DATA_ROOT/coord.log" 2>&1 &
  sleep 0.3
}

start_nodes() {
  local fsync_flag=${1:-0}
  for i in $(seq 1 $N); do
    "$BIN/dfs_node" 127.0.0.1 $COORD_PORT $((BASE_NODE_PORT+i)) "$DATA_ROOT/nodes/n$i" $fsync_flag >"$DATA_ROOT/nodes/n$i.log" 2>&1 &
  done
  sleep 0.5
}

drop_caches() { sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; }

echo "================ BUFFERED (page cache on) ================"
start_coord
start_nodes 0

echo "--- buffered PUT ---"
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT put "$DATA_ROOT/src.bin" f $CHUNK $THREADS

echo "--- HOT GET ---"
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT get f "$DATA_ROOT/hot.out" $THREADS

echo "--- COLD GET (drop_caches) ---"
drop_caches
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT get f "$DATA_ROOT/cold.out" $THREADS

pkill -f dfs_ 2>/dev/null || true
sleep 0.3

echo "================ DURABLE (fsync before ack) ================"
rm -rf "$DATA_ROOT/nodes"; mkdir -p "$DATA_ROOT/nodes"
start_coord
start_nodes 1
drop_caches

echo "--- durable PUT (fsync) ---"
"$BIN/dfs_client" 127.0.0.1 $COORD_PORT put "$DATA_ROOT/src.bin" g $CHUNK $THREADS

pkill -f dfs_ 2>/dev/null || true
echo "== done =="
