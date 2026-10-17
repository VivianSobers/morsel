import argparse
import csv
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench.check import compare, ORDER_KEY_COUNTS, read_csv_file

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


def find_default_morsel() -> str:
    candidates = [
        os.environ.get("MORSEL_BIN"),
        "/dev/shm/morsel/target/release/morsel",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "target",
            "release",
            "morsel",
        ),
        "target/release/morsel",
    ]
    for c in candidates:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return "target/release/morsel"


def get_morsel_version(morsel_bin: str) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if out:
            return out
    except Exception:
        pass
    return "0.1.0"


def parse_query_selection(sel_str: Optional[str]) -> List[int]:
    if not sel_str:
        return list(range(1, 23))
    selected = set()
    parts = sel_str.replace(" ", "").split(",")
    for part in parts:
        if not part:
            continue
        part = part.lower().lstrip("q")
        if "-" in part:
            start_s, end_s = part.split("-", 1)
            start, end = int(start_s), int(end_s)
            for q in range(start, end + 1):
                selected.add(q)
        else:
            selected.add(int(part))
    return sorted(selected)


def get_cpu_model() -> str:
    try:
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if "model name" in line:
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.processor() or "unknown"


def get_ram_total() -> str:
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if "MemTotal" in line:
                    kb = int(line.split()[1])
                    gb = kb / (1024 * 1024)
                    return f"{gb:.1f} GB"
    except Exception:
        pass
    return "unknown"


def rows_to_string_grid(rows: List[Any]) -> List[List[str]]:
    grid = []
    for row in rows:
        grid_row = []
        for val in row:
            if val is None:
                grid_row.append("NULL")
            else:
                grid_row.append(str(val))
        grid.append(grid_row)
    return grid


def run_duckdb(args, queries: List[Tuple[int, str, str]]) -> Tuple[str, Any]:
    try:
        import duckdb
    except ImportError:
        print("DuckDB is not installed, skipping.")
        return "", None

    print(f"Loading tables into DuckDB (threads={args.threads})...")
    conn = duckdb.connect()
    conn.execute(f"SET threads={args.threads}")
    conn.execute("SET memory_limit='30GB'")

    for table in TPCH_TABLES:
        parquet_path = os.path.join(args.data_dir, f"{table}.parquet")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{parquet_path}')")

    def execute_fn(sql: str) -> List[List[Any]]:
        return conn.execute(sql).fetchall()

    return duckdb.__version__, execute_fn


def run_polars(args, queries: List[Tuple[int, str, str]]) -> Tuple[str, Any]:
    try:
        import polars as pl
    except ImportError:
        print("Polars is not installed, skipping.")
        return "", None

    print(f"Loading tables into Polars SQLContext...")
    os.environ["POLARS_MAX_THREADS"] = str(args.threads)
    ctx = pl.SQLContext()
    for table in TPCH_TABLES:
        parquet_path = os.path.join(args.data_dir, f"{table}.parquet")
        ctx.register(table, pl.scan_parquet(parquet_path))

    def execute_fn(sql: str) -> List[List[Any]]:
        res_df = ctx.execute(sql).collect()
        return res_df.rows()

    return pl.__version__, execute_fn


def run_datafusion(args, queries: List[Tuple[int, str, str]]) -> Tuple[str, Any]:
    try:
        import datafusion as df
    except ImportError:
        print("DataFusion is not installed, skipping.")
        return "", None

    print(f"Loading tables into DataFusion SessionContext (threads={args.threads})...")
    ctx = df.SessionContext()
    for table in TPCH_TABLES:
        parquet_path = os.path.join(args.data_dir, f"{table}.parquet")
        ctx.register_parquet(table, parquet_path)

    def execute_fn(sql: str) -> List[List[Any]]:
        res_df = ctx.sql(sql).collect()
        # Convert PyArrow RecordBatches to rows
        rows = []
        for batch in res_df:
            pydict = batch.to_pydict()
            cols = list(pydict.keys())
            batch_len = len(pydict[cols[0]]) if cols else 0
            for i in range(batch_len):
                rows.append([pydict[c][i] for c in cols])
        return rows

    return df.__version__, execute_fn


def main():
    parser = argparse.ArgumentParser(description="TPC-H baseline runners")
    parser.add_argument(
        "--engine",
        required=True,
        choices=["duckdb", "polars", "datafusion", "morsel"],
        help="Engine to benchmark",
    )
    parser.add_argument("--sf", type=float, required=True, help="Scale factor")
    parser.add_argument("--data-dir", required=True, help="Directory with Parquet files")
    parser.add_argument("--threads", type=int, required=True, help="Thread count")
    parser.add_argument("--out", required=True, help="Output JSON Lines file")
    parser.add_argument(
        "--queries-dir",
        default="queries/tpch",
        help="Directory with TPC-H query SQL files",
    )
    parser.add_argument(
        "--refs-dir",
        default=None,
        help="Directory with reference CSV answer files",
    )
    parser.add_argument(
        "--note",
        default=None,
        help="Optional note string to include in every output record",
    )
    parser.add_argument(
        "--morsel",
        default=find_default_morsel(),
        help="Path to morsel binary",
    )
    parser.add_argument(
        "--queries",
        default=None,
        help="Query range or list, e.g. '1-22' or 'q1,q3' (default all 22)",
    )
    args = parser.parse_args()

    refs_dir = args.refs_dir
    if not refs_dir:
        sf_int = int(args.sf) if args.sf.is_integer() else args.sf
        refs_dir = f"bench/refs/sf{sf_int}"

    # Load query files (engine-specific query text if present, otherwise default)
    query_nums = parse_query_selection(args.queries)
    queries = []
    for q_nr in query_nums:
        query_filename = f"q{q_nr:02d}.sql"
        engine_sql_file = os.path.join(args.queries_dir, args.engine, query_filename)
        default_sql_file = os.path.join(args.queries_dir, query_filename)
        if os.path.exists(engine_sql_file):
            target_sql_file = engine_sql_file
        elif os.path.exists(default_sql_file):
            target_sql_file = default_sql_file
        else:
            print(f"Error: query file not found: {default_sql_file}")
            sys.exit(1)
        with open(target_sql_file, "r", encoding="utf-8") as f:
            queries.append((q_nr, f.read().strip().rstrip(";"), target_sql_file))

    cpu_model = get_cpu_model()
    ram_total = get_ram_total()
    hostname = socket.gethostname()

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if args.engine == "morsel":
        version = get_morsel_version(args.morsel)
        print(f"Running benchmarks for morsel (v{version}) on SF={args.sf}...")
        for q_nr, sql, sql_path in queries:
            query_name = f"q{q_nr:02d}"
            print(f"Running {query_name}...")

            with tempfile.TemporaryDirectory() as tmp_dir:
                timing_json_path = os.path.join(tmp_dir, f"timing_{query_name}.json")
                out_csv_path = os.path.join(tmp_dir, f"out_{query_name}.csv")

                cmd = [
                    args.morsel,
                    "query",
                    "--data",
                    args.data_dir,
                    "--sql-file",
                    sql_path,
                    "--threads",
                    str(args.threads),
                    "--repeat",
                    "6",
                    "--timing-json",
                    timing_json_path,
                    "--out",
                    out_csv_path,
                ]

                proc = subprocess.run(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                if proc.returncode != 0:
                    err_msg = (proc.stderr.strip() or proc.stdout.strip()).replace(
                        "\n", " "
                    )
                    print(f"  Morsel failed: {err_msg}")
                    record = {
                        "engine": "morsel",
                        "engine_version": version,
                        "sf": args.sf,
                        "threads": args.threads,
                        "query": query_name,
                        "times": [],
                        "median": None,
                        "min": None,
                        "check": f"ERROR: {err_msg}",
                        "cpu_model": cpu_model,
                        "ram": ram_total,
                        "hostname": hostname,
                        "date": datetime.now(timezone.utc).isoformat(),
                    }
                    if args.note is not None:
                        record["note"] = args.note
                    with open(args.out, "a", encoding="utf-8") as f:
                        f.write(json.dumps(record) + "\n")
                    continue

                timing_lines = []
                if os.path.exists(timing_json_path):
                    with open(timing_json_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if line.strip():
                                timing_lines.append(json.loads(line))

                if len(timing_lines) >= 6:
                    times = [item["execute_ms"] / 1000.0 for item in timing_lines[1:6]]
                    if (
                        timing_lines[0].get("git_commit")
                        and timing_lines[0]["git_commit"] != "unknown"
                    ):
                        version = timing_lines[0]["git_commit"]
                elif timing_lines:
                    times = [item["execute_ms"] / 1000.0 for item in timing_lines]
                else:
                    times = []

                if not times:
                    print(f"  No timing produced")
                    record = {
                        "engine": "morsel",
                        "engine_version": version,
                        "sf": args.sf,
                        "threads": args.threads,
                        "query": query_name,
                        "times": [],
                        "median": None,
                        "min": None,
                        "check": "ERROR: no timing produced",
                        "cpu_model": cpu_model,
                        "ram": ram_total,
                        "hostname": hostname,
                        "date": datetime.now(timezone.utc).isoformat(),
                    }
                    if args.note is not None:
                        record["note"] = args.note
                    with open(args.out, "a", encoding="utf-8") as f:
                        f.write(json.dumps(record) + "\n")
                    continue

                med_time = statistics.median(times)
                min_time = min(times)

                actual_rows = []
                if os.path.exists(out_csv_path) and os.path.getsize(out_csv_path) > 0:
                    _, actual_rows = read_csv_file(out_csv_path)
                elif proc.stdout.strip():
                    lines = [l for l in proc.stdout.splitlines() if l.strip()]
                    reader = csv.reader(lines)
                    next(reader, None)
                    actual_rows = list(reader)

                check_result = "SKIPPED: no refs dir"
                if refs_dir:
                    ref_csv = os.path.join(refs_dir, f"{query_name}.csv")
                    if os.path.exists(ref_csv):
                        try:
                            _, exp_rows = read_csv_file(ref_csv)
                            order_keys = ORDER_KEY_COUNTS.get(q_nr, 0)
                            diffs = compare(actual_rows, exp_rows, order_keys)
                            if not diffs:
                                check_result = "PASS"
                            else:
                                check_result = f"FAIL: {diffs[0]}"
                        except Exception as e:
                            check_result = f"ERROR: checker failed: {e}"

                print(
                    f"  median: {med_time*1000:.1f}ms, min: {min_time*1000:.1f}ms, check: {check_result}"
                )

                record = {
                    "engine": "morsel",
                    "engine_version": version,
                    "sf": args.sf,
                    "threads": args.threads,
                    "query": query_name,
                    "times": times,
                    "median": med_time,
                    "min": min_time,
                    "check": check_result,
                    "cpu_model": cpu_model,
                    "ram": ram_total,
                    "hostname": hostname,
                    "date": datetime.now(timezone.utc).isoformat(),
                }
                if args.note is not None:
                    record["note"] = args.note

                with open(args.out, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record) + "\n")

        print(f"All done! Results saved to {args.out}")
        return

    # Initialize non-morsel engine
    if args.engine == "duckdb":
        version, exec_fn = run_duckdb(args, queries)
    elif args.engine == "polars":
        version, exec_fn = run_polars(args, queries)
    elif args.engine == "datafusion":
        version, exec_fn = run_datafusion(args, queries)
    else:
        print(f"Unknown engine {args.engine}")
        sys.exit(1)

    if exec_fn is None:
        print(f"Skipping {args.engine}: not installed")
        sys.exit(0)

    print(f"Running benchmarks for {args.engine} (v{version}) on SF={args.sf}...")
    for q_nr, sql, _ in queries:
        query_name = f"q{q_nr:02d}"
        print(f"Running {query_name}...")

        # 1 warm-up run
        try:
            res = exec_fn(sql)
            actual_rows = rows_to_string_grid(res)
        except Exception as e:
            print(f"  Warm-up failed: {e}")
            record = {
                "engine": args.engine,
                "engine_version": version,
                "sf": args.sf,
                "threads": args.threads,
                "query": query_name,
                "times": [],
                "median": None,
                "min": None,
                "check": f"ERROR: {e}",
                "cpu_model": cpu_model,
                "ram": ram_total,
                "hostname": hostname,
                "date": datetime.now(timezone.utc).isoformat(),
            }
            if args.note is not None:
                record["note"] = args.note
            with open(args.out, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
            continue

        # 5 timed runs
        times = []
        timed_err = None
        for run_idx in range(5):
            try:
                t0 = time.perf_counter()
                _ = exec_fn(sql)
                t1 = time.perf_counter()
                times.append(t1 - t0)
            except Exception as e:
                timed_err = str(e)
                print(f"  Timed run {run_idx} failed: {e}")
                break

        if timed_err is not None:
            record = {
                "engine": args.engine,
                "engine_version": version,
                "sf": args.sf,
                "threads": args.threads,
                "query": query_name,
                "times": times,
                "median": None,
                "min": None,
                "check": f"ERROR in timed run: {timed_err}",
                "cpu_model": cpu_model,
                "ram": ram_total,
                "hostname": hostname,
                "date": datetime.now(timezone.utc).isoformat(),
            }
            if args.note is not None:
                record["note"] = args.note
            with open(args.out, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
            continue

        med_time = statistics.median(times)
        min_time = min(times)

        # Answer check against references if available
        check_result = "SKIPPED: no refs dir"
        if refs_dir:
            ref_csv = os.path.join(refs_dir, f"{query_name}.csv")
            if os.path.exists(ref_csv):
                try:
                    _, exp_rows = read_csv_file(ref_csv)
                    order_keys = ORDER_KEY_COUNTS.get(q_nr, 0)
                    diffs = compare(actual_rows, exp_rows, order_keys)
                    if not diffs:
                        check_result = "PASS"
                    else:
                        check_result = f"FAIL: {diffs[0]}"
                except Exception as e:
                    check_result = f"ERROR: checker failed: {e}"
        print(
            f"  median: {med_time*1000:.1f}ms, min: {min_time*1000:.1f}ms, check: {check_result}"
        )

        record = {
            "engine": args.engine,
            "engine_version": version,
            "sf": args.sf,
            "threads": args.threads,
            "query": query_name,
            "times": times,
            "median": med_time,
            "min": min_time,
            "check": check_result,
            "cpu_model": cpu_model,
            "ram": ram_total,
            "hostname": hostname,
            "date": datetime.now(timezone.utc).isoformat(),
        }
        if args.note is not None:
            record["note"] = args.note

        with open(args.out, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    print(f"All done! Results saved to {args.out}")


if __name__ == "__main__":
    main()
