#!/usr/bin/env bash
# End-to-end check of the Classic TE metrics: run a transfer_engine_bench target
# and initiator with MC_TE_METRIC=1, scrape their /metrics/json, and let
# bench_te_metrics_check.py compare the counters with what the bench reports.
#
# Usage: ./scripts/bench_te_metrics.sh
# Env:   BUILD_DIR (build)   DURATION (1 s/case)  OUT (<BUILD_DIR>/bench_te_metrics_out)
#        PROTOCOL (tcp)     e.g. PROTOCOL=rdma
#        DEVICE  (mlx5_0)   RDMA device name (rdma only, see ibv_devices)
#        HOST    (127.0.0.1) IP the benches bind to; use the IB interface IP for rdma
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD_DIR=${BUILD_DIR:-build}
case "$BUILD_DIR" in /*) ;; *) BUILD_DIR=$ROOT/$BUILD_DIR ;; esac   # relative to the repo root
BENCH=$BUILD_DIR/mooncake-transfer-engine/example/transfer_engine_bench
OUT=${OUT:-$BUILD_DIR/bench_te_metrics_out}         # logs and scraped JSON land here
DURATION=${DURATION:-1}                             # seconds per read/write case
BLOCK=$((1 << 20)) BATCH=16                         # transfer shape, also fed to the checker
PROTOCOL=${PROTOCOL:-tcp}
DEVICE=${DEVICE:-mlx5_0}
HOST=${HOST:-127.0.0.1}

die() { echo "ERROR: $*" >&2; exit 1; }

[ -x "$BENCH" ] || die "no $BENCH; build transfer_engine_bench first"
rm -rf "$OUT" && mkdir -p "$OUT"

# --device_name is only meaningful for RDMA-style protocols.
FLAGS=""
case "$PROTOCOL" in rdma|barex|ub|efa) FLAGS="--device_name=$DEVICE" ;; esac

export GLOG_logtostderr=1                           # bench logs go to the per-process log file
trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null' EXIT   # no stray benches on exit

echo "config: $BATCH requests/batch x $BLOCK B/request, 2 threads, ${DURATION}s per case, protocol=$PROTOCOL"

# start <metrics-port> <log> <bench-args...>
# Run one bench with metrics enabled, set PID, and return once its metrics
# endpoint answers. Fail fast if the process dies before that.
start() {
    local port=$1 log=$2; shift 2
    MC_TE_METRIC=1 MC_TE_METRIC_HTTP_HOST=127.0.0.1 MC_TE_METRIC_HTTP_PORT=$port \
        "$BENCH" --protocol=$PROTOCOL $FLAGS --metadata_server=P2PHANDSHAKE \
        --buffer_size=$((64 << 20)) "$@" >"$log" 2>&1 &
    PID=$!
    for _ in {1..80}; do
        curl -sf "http://127.0.0.1:$port/health" >/dev/null && return
        kill -0 $PID 2>/dev/null || break
        sleep 0.25
    done
    tail -3 "$log" >&2
    die "bench on :$port never served metrics (built with WITH_METRICS=ON?); see $log"
}

# wait_log <log> <pattern> [seconds]: block until the bench logs <pattern>,
# dies, or the timeout (default 20 s) passes.
wait_log() {
    for _ in $(seq $(( ${3:-20} * 4 ))); do
        grep -q "$2" "$1" && return
        kill -0 $PID 2>/dev/null || break
        sleep 0.25
    done
    tail -3 "$1" >&2
    die "bench never logged '$2'; see $1"
}

# One passive target serves both cases. It should record no transfers itself.
# Its metrics endpoint comes up during engine construction, before init() and
# buffer registration, so wait for the ready line rather than for /health.
start 19101 "$OUT/target.log" --mode=target --local_server_name=$HOST:17812
wait_log "$OUT/target.log" "Target ready"
# P2PHANDSHAKE picks the target's RPC port at random; read it back from the log.
SEGMENT=$(grep -oP 'P2P handshake, listening on \K[0-9.]+:[0-9]+' "$OUT/target.log" | head -1)
[ -n "$SEGMENT" ] || die "could not read the target's RPC address from $OUT/target.log"
echo "target up, segment $SEGMENT"

# One fresh initiator per operation, so each scrape holds exactly that case's counters.
for op in read write; do
    # --linger keeps the process, and with it the metrics endpoint, alive after
    # the workers have joined, so the scrape below sees the final counters.
    start 19102 "$OUT/$op.log" --mode=initiator --local_server_name=$HOST:17813 \
        --segment_id="$SEGMENT" --operation=$op --block_size=$BLOCK --batch_size=$BATCH \
        --threads=2 --duration="$DURATION" --linger=30
    echo "running $op for ${DURATION}s"
    wait_log "$OUT/$op.log" "Test completed" $((DURATION + 20))
    curl -sf "http://127.0.0.1:19102/metrics/json" >"$OUT/$op.json" || die "could not scrape initiator $op"
    kill $PID; wait $PID 2>/dev/null
done
curl -sf "http://127.0.0.1:19101/metrics/json" >"$OUT/target.json" || die "could not scrape target; see $OUT/target.log"

# Compare the scraped counters with the batch counts the benches printed.
python3 "$ROOT/scripts/bench_te_metrics_check.py" "$OUT" $BATCH $BLOCK
