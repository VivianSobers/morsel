#!/bin/bash
# Final benchmark on worker-2: all four engines, SF1 then SF10, same thread count, one engine at a time.
# usage: morsel_bench.sh COMMIT THREADS
set -u
c=$1; t=$2
d=${TPCH_DIR:?set TPCH_DIR to the directory holding sf1, sf10 and sf10_refs}
out=$HOME/morsel-bench/bench/results/final
export PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=/dev/shm/morsel-bench-target CARGO_BUILD_JOBS=16
cd ~/morsel-bench && git checkout -q --detach $c || exit 1
nice -n 5 cargo build -q --release -p morsel-cli || exit 1
mkdir -p $out
py=$HOME/morsel/.venv/bin/python
echo "bench commit $(git rev-parse --short HEAD) threads $t start $(date +%FT%T)"
echo "gpu: $(nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader)"; uptime
for sf in 1 10; do
  refs=$d/sf${sf}_refs; [ $sf = 1 ] && refs=bench/refs/sf1
  for e in morsel duckdb datafusion polars; do
    echo "== $e sf$sf $(date +%T) load $(cut -d' ' -f1-3 /proc/loadavg)"
    $py bench/baselines.py --engine $e --sf $sf --data-dir $d/sf$sf --refs-dir $refs --threads $t \
      --morsel $CARGO_TARGET_DIR/release/morsel --out $out/$e-sf$sf.jsonl \
      --note "final: worker-2, no other jobs, threads $t, commit $c" > $out/$e-sf$sf.log 2>&1
    echo "   exit $? $(grep -c PASS $out/$e-sf$sf.log) PASS, $(grep -ci 'fail\|error' $out/$e-sf$sf.log) fail/error lines"
  done
done
echo BENCH_DONE $(date +%FT%T)
