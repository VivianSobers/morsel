#!/usr/bin/env python3
import argparse
import csv
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from bench.check import compare, read_csv_file

TPCH_TABLES = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]


def find_morsel_fuzz_binary(custom_path: Optional[str] = None) -> str:
    if custom_path and os.path.isfile(custom_path):
        return custom_path

    candidates = [
        "/dev/shm/morsel/target-fuzz/release/morsel-fuzz",
        "/dev/shm/morsel/target-fuzz/debug/morsel-fuzz",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target/release/morsel-fuzz"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target/debug/morsel-fuzz"),
    ]
    for c in candidates:
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c

    raise FileNotFoundError("Could not find morsel-fuzz binary. Run cargo build first.")


def init_duckdb(data_dir: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute("SET memory_limit='2GB'")
    for t in TPCH_TABLES:
        pq = os.path.join(data_dir, f"{t}.parquet")
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM '{pq}'")
    return con


def evaluate_seed(
    seed: int,
    out_dir: str,
    con: duckdb.DuckDBPyConnection,
) -> Tuple[str, Optional[str], str]:
    """
    Evaluates seed results in out_dir against DuckDB.
    Returns (outcome, first_difference, shape_summary).
    """
    shape_path = os.path.join(out_dir, f"{seed}.shape")
    shape = ""
    if os.path.exists(shape_path):
        with open(shape_path, "r", encoding="utf-8") as f:
            shape = f.read().strip()

    sql_path = os.path.join(out_dir, f"{seed}.sql")
    err_path = os.path.join(out_dir, f"{seed}.err")
    csv_path = os.path.join(out_dir, f"{seed}.csv")

    if os.path.exists(err_path):
        with open(err_path, "r", encoding="utf-8") as f:
            err_msg = f.read().strip()
        return "morsel_error", err_msg, shape

    if not os.path.exists(sql_path):
        return "morsel_error", "Missing SQL file", shape

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    duck_csv_path = os.path.join(out_dir, f"{seed}_duck.csv")
    try:
        con.execute(f"COPY ({sql}) TO '{duck_csv_path}' (HEADER, DELIMITER ',')")
    except Exception as e:
        return "duckdb_error", str(e), shape

    try:
        _, act_rows = read_csv_file(csv_path)
        _, exp_rows = read_csv_file(duck_csv_path)
        col_count = len(act_rows[0]) if act_rows else (len(exp_rows[0]) if exp_rows else 0)
        diffs = compare(act_rows, exp_rows, order_key_count=col_count)
        if diffs:
            return "mismatch", diffs[0], shape
        return "pass", None, shape
    except Exception as e:
        return "mismatch", f"Comparison exception: {e}", shape


def worker_loop(
    worker_id: int,
    bin_path: str,
    data_dir: str,
    found_dir: str,
    task_queue: mp.Queue,
    result_queue: mp.Queue,
):
    try:
        con = init_duckdb(data_dir)
    except Exception as e:
        print(f"Worker {worker_id} failed to initialize DuckDB: {e}", file=sys.stderr)
        return

    while True:
        chunk = task_queue.get()
        if chunk is None:
            break

        c_start, c_end = chunk
        with tempfile.TemporaryDirectory(prefix=f"morsel_fuzz_w{worker_id}_") as chunk_dir:
            cmd = [
                bin_path,
                "run",
                "--data",
                data_dir,
                "--seeds",
                f"{c_start}..{c_end}",
                "--out",
                chunk_dir,
                "--threads",
                "1",
            ]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if proc.returncode != 0:
                print(f"morsel-fuzz failed on chunk {c_start}..{c_end}: {proc.stderr}", file=sys.stderr)

            for seed in range(c_start, c_end):
                outcome, diff, shape = evaluate_seed(seed, chunk_dir, con)

                if outcome != "pass":
                    # Copy failing files to found_dir/<seed>
                    seed_found_dir = os.path.join(found_dir, str(seed))
                    os.makedirs(seed_found_dir, exist_ok=True)
                    for ext in [".sql", ".txt", ".shape", ".err", ".csv"]:
                        src = os.path.join(chunk_dir, f"{seed}{ext}")
                        if os.path.exists(src):
                            shutil.copy2(src, os.path.join(seed_found_dir, f"{seed}{ext}"))
                    duck_csv = os.path.join(chunk_dir, f"{seed}_duck.csv")
                    if os.path.exists(duck_csv):
                        shutil.copy2(duck_csv, os.path.join(seed_found_dir, f"{seed}_duck.csv"))

                result_queue.put((seed, shape, outcome, diff))


def writer_loop(result_file: str, total_seeds: int, result_queue: mp.Queue, status_queue: mp.Queue):
    os.makedirs(os.path.dirname(os.path.abspath(result_file)), exist_ok=True)
    counts = {"pass": 0, "mismatch": 0, "morsel_error": 0, "duckdb_error": 0}
    seen = 0

    with open(result_file, "a", encoding="utf-8") as rf:
        while seen < total_seeds:
            item = result_queue.get()
            if item is None:
                break
            seed, shape, outcome, diff = item
            record = {
                "seed": seed,
                "shape": shape,
                "outcome": outcome,
                "diff": diff,
            }
            rf.write(json.dumps(record) + "\n")
            rf.flush()

            counts[outcome] = counts.get(outcome, 0) + 1
            seen += 1

            if seen % 500 == 0 or seen == total_seeds:
                status_queue.put((seen, counts.copy()))


def minimize_seed(
    seed: int,
    bin_path: str,
    data_dir: str,
    found_dir: str,
):
    print(f"Minimizing seed {seed}...")
    con = init_duckdb(data_dir)

    # 1. Get list of droppable pieces
    cmd = [bin_path, "list-drops", "--data", data_dir, "--seed", str(seed)]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        print(f"Error querying droppable pieces: {res.stderr}", file=sys.stderr)
        return

    pieces = [p.strip() for p in res.stdout.strip().split(",") if p.strip()]
    print(f"Droppable pieces: {pieces}")

    # 2. Get baseline failure
    with tempfile.TemporaryDirectory(prefix="morsel_min_base_") as base_dir:
        cmd = [
            bin_path,
            "run",
            "--data",
            data_dir,
            "--seeds",
            str(seed),
            "--out",
            base_dir,
            "--threads",
            "1",
        ]
        subprocess.run(cmd, check=True)
        base_outcome, base_diff, _ = evaluate_seed(seed, base_dir, con)
        print(f"Baseline outcome: {base_outcome}, diff: {base_diff[:80] if base_diff else None}")

    if base_outcome == "pass":
        print(f"Seed {seed} passes; nothing to minimize.")
        return

    # 3. Greedy drop
    active_drops = set()

    for piece in pieces:
        test_drops = active_drops | {piece}
        drop_str = ",".join(sorted(test_drops))

        with tempfile.TemporaryDirectory(prefix="morsel_min_step_") as step_dir:
            cmd = [
                bin_path,
                "run",
                "--data",
                data_dir,
                "--seeds",
                str(seed),
                "--out",
                step_dir,
                "--threads",
                "1",
                "--drop",
                drop_str,
            ]
            subprocess.run(cmd)
            outcome, diff, _ = evaluate_seed(seed, step_dir, con)

            # Check if it still reproduces failure
            if outcome == base_outcome:
                active_drops.add(piece)
                print(f"  Dropped '{piece}' -> still fails with {outcome}")
            else:
                print(f"  Keeping '{piece}' (dropping gave {outcome})")

    # 4. Generate final minimized query
    final_drop_str = ",".join(sorted(active_drops))
    print(f"Final drop set: {final_drop_str}")

    seed_found_dir = os.path.join(found_dir, str(seed))
    os.makedirs(seed_found_dir, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="morsel_min_final_") as fin_dir:
        cmd = [
            bin_path,
            "run",
            "--data",
            data_dir,
            "--seeds",
            str(seed),
            "--out",
            fin_dir,
            "--threads",
            "1",
        ]
        if final_drop_str:
            cmd.extend(["--drop", final_drop_str])
        subprocess.run(cmd, check=True)

        fin_sql = os.path.join(fin_dir, f"{seed}.sql")
        fin_txt = os.path.join(fin_dir, f"{seed}.txt")

        min_sql = os.path.join(seed_found_dir, "min.sql")
        min_txt = os.path.join(seed_found_dir, "min.txt")

        if os.path.exists(fin_sql):
            shutil.copy2(fin_sql, min_sql)
            print(f"Saved {min_sql}")
        if os.path.exists(fin_txt):
            shutil.copy2(fin_txt, min_txt)
            print(f"Saved {min_txt}")


def generate_found_md(found_dir: str, output_md: str):
    """
    Generates bench/fuzz/FOUND.md grouping distinct engine bugs.
    """
    if not os.path.exists(found_dir):
        return

    # Group failures by operator and message
    # Key: (operator/category, error_message_signature)
    bugs = {}

    for seed_str in sorted(os.listdir(found_dir)):
        seed_path = os.path.join(found_dir, seed_str)
        if not os.path.isdir(seed_path):
            continue

        try:
            seed = int(seed_str)
        except ValueError:
            continue

        err_file = os.path.join(seed_path, f"{seed}.err")
        sql_file = os.path.join(seed_path, "min.sql")
        if not os.path.exists(sql_file):
            sql_file = os.path.join(seed_path, f"{seed}.sql")

        min_sql_text = ""
        if os.path.exists(sql_file):
            with open(sql_file, "r", encoding="utf-8") as f:
                min_sql_text = f.read().strip()

        if os.path.exists(err_file):
            with open(err_file, "r", encoding="utf-8") as f:
                morsel_ret = f.read().strip()
            duck_ret = "Query executed successfully"
            # Extract operator signature
            if "unsupported operator in scan pipeline: Join" in morsel_ret:
                sig = "Join in scan pipeline (Project/Filter over Join unsupported)"
            elif "unsupported operator" in morsel_ret:
                sig = morsel_ret[:80]
            elif "Panic" in morsel_ret:
                sig = morsel_ret[:80]
            else:
                sig = morsel_ret[:80]
        else:
            morsel_csv = os.path.join(seed_path, f"{seed}.csv")
            duck_csv = os.path.join(seed_path, f"{seed}_duck.csv")
            morsel_ret = "Returned incorrect rows/values (mismatch)"
            duck_ret = "Reference answer"
            sig = "Result mismatch against DuckDB"

        if sig not in bugs:
            bugs[sig] = {
                "seed": seed,
                "sql": min_sql_text,
                "morsel": morsel_ret,
                "duck": duck_ret,
                "count": 1,
            }
        else:
            bugs[sig]["count"] += 1

    with open(output_md, "w", encoding="utf-8") as f:
        f.write("# Discovered Engine Bugs\n\n")
        f.write("| Seed | Operator / Bug Summary | Count | Minimized SQL | Morsel Result | DuckDB Result |\n")
        f.write("|:---|:---|:---|:---|:---|:---|\n")
        for sig, info in sorted(bugs.items()):
            # format sql as compact inline or block
            sql_compact = info["sql"].replace("\n", " ").replace("|", "\\|")
            if len(sql_compact) > 100:
                sql_compact = sql_compact[:97] + "..."
            morsel_compact = info["morsel"].replace("\n", " ").replace("|", "\\|")
            if len(morsel_compact) > 80:
                morsel_compact = morsel_compact[:77] + "..."
            f.write(
                f"| `{info['seed']}` | {sig} | {info['count']} | `{sql_compact}` | {morsel_compact} | {info['duck']} |\n"
            )


def main():
    parser = argparse.ArgumentParser(description="Differential SQL query fuzzer driver")
    parser.add_argument("--data", default="/dev/shm/morsel/tpch/fuzz_sf01", help="Parquet data directory")
    parser.add_argument("--start", type=int, default=0, help="Starting seed")
    parser.add_argument("--count", type=int, default=1000, help="Total seeds to run")
    parser.add_argument("--workers", "-w", type=int, default=12, help="Worker count")
    parser.add_argument("--chunk", "-c", type=int, default=50, help="Chunk size")
    parser.add_argument(
        "--results",
        default="/dev/shm/morsel/fuzz/results.jsonl",
        help="Path to results.jsonl",
    )
    parser.add_argument(
        "--found-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "fuzz/found"),
        help="Directory to save failing cases",
    )
    parser.add_argument("--bin", default=None, help="Path to morsel-fuzz binary")
    parser.add_argument("--minimize", type=int, default=None, help="Minimize a specific failing seed")
    parser.add_argument("--update-found-md", action="store_true", help="Generate/update FOUND.md")
    args = parser.parse_args()

    bin_path = find_morsel_fuzz_binary(args.bin)

    if args.minimize is not None:
        minimize_seed(args.minimize, bin_path, args.data, args.found_dir)
        found_md_path = os.path.join(os.path.dirname(args.found_dir), "FOUND.md")
        generate_found_md(args.found_dir, found_md_path)
        return

    if args.update_found_md:
        found_md_path = os.path.join(os.path.dirname(args.found_dir), "FOUND.md")
        generate_found_md(args.found_dir, found_md_path)
        print(f"Updated {found_md_path}")
        return

    os.makedirs(args.found_dir, exist_ok=True)

    task_queue = mp.Queue()
    result_queue = mp.Queue()
    status_queue = mp.Queue()

    # Enqueue chunks
    num_chunks = 0
    curr = args.start
    end = args.start + args.count
    while curr < end:
        next_curr = min(curr + args.chunk, end)
        task_queue.put((curr, next_curr))
        curr = next_curr
        num_chunks += 1

    # Add stop sentinels for workers
    for _ in range(args.workers):
        task_queue.put(None)

    # Start writer process
    writer_proc = mp.Process(
        target=writer_loop,
        args=(args.results, args.count, result_queue, status_queue),
    )
    writer_proc.start()

    # Start worker processes
    workers = []
    for w_id in range(args.workers):
        p = mp.Process(
            target=worker_loop,
            args=(
                w_id,
                bin_path,
                args.data,
                args.found_dir,
                task_queue,
                result_queue,
            ),
        )
        p.start()
        workers.append(p)

    print(
        f"Started fuzzer: {args.count} seeds ({args.start}..{end}), {args.workers} workers, chunk {args.chunk}."
    )

    # Monitor status
    last_reported = 0
    while any(p.is_alive() for p in workers) or not status_queue.empty():
        try:
            seen, counts = status_queue.get(timeout=1.0)
            print(
                f"Progress: {seen}/{args.count} | pass: {counts.get('pass', 0)} | "
                f"mismatch: {counts.get('mismatch', 0)} | morsel_error: {counts.get('morsel_error', 0)} | "
                f"duckdb_error: {counts.get('duckdb_error', 0)}"
            )
            last_reported = seen
        except Exception:
            pass

    for p in workers:
        p.join()

    writer_proc.join()

    # Final FOUND.md generation
    found_md_path = os.path.join(os.path.dirname(args.found_dir), "FOUND.md")
    generate_found_md(args.found_dir, found_md_path)
    print(f"Fuzzing run completed. Results written to {args.results}. FOUND.md generated.")


if __name__ == "__main__":
    main()
