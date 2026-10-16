#!/usr/bin/env python3
"""
TPC-H End-to-End Runner for Morsel.
Runs TPC-H queries through Morsel CLI, checks results against reference CSVs,
classifies outcomes, and outputs per-query JSON lines and a summary table.
"""

import argparse
import csv
import io
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

# Ensure repository root is on sys.path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from bench.check import ORDER_KEY_COUNTS, compare, read_csv_file


def parse_query_selection(selection_str: str) -> List[int]:
    """
    Parse a query selection string like "1-22", "1,2,3", "q1-q5,q7", "1-22".
    Returns a sorted list of unique integer query numbers.
    """
    queries = set()
    parts = selection_str.split(",")
    for part in parts:
        part = part.strip().lower()
        if not part:
            continue
        if part.startswith("q"):
            part = part[1:]
        if "-" in part:
            sub = part.split("-", 1)
            start_s = sub[0].strip().lower()
            end_s = sub[1].strip().lower()
            if start_s.startswith("q"):
                start_s = start_s[1:]
            if end_s.startswith("q"):
                end_s = end_s[1:]
            start = int(start_s)
            end = int(end_s)
            for q in range(start, end + 1):
                queries.add(q)
        else:
            queries.add(int(part))
    return sorted(queries)


def get_git_commit() -> str:
    """Get current git commit hash (short)."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        c = out.strip()
        if c:
            return c
    except Exception:
        pass
    return "unknown"


def infer_scale_factor(data_dir: str, refs_dir: str, explicit_sf: Optional[float] = None) -> float:
    """Infer scale factor from directories or explicit argument."""
    if explicit_sf is not None:
        return explicit_sf

    combined = f"{data_dir} {refs_dir}".lower()
    m = re.search(r"sf([0-9]+(?:\.[0-9]+)?)", combined)
    if m:
        try:
            val = float(m.group(1))
            return int(val) if val.is_integer() else val
        except ValueError:
            pass
    return 1


def find_morsel_binary(explicit_path: Optional[str] = None) -> str:
    """Locate morsel binary executable."""
    if explicit_path:
        p = os.path.abspath(explicit_path)
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
        raise FileNotFoundError(f"Specified morsel binary not found or not executable: {explicit_path}")

    # Candidate locations
    candidates = []
    if "CARGO_TARGET_DIR" in os.environ:
        candidates.append(os.path.join(os.environ["CARGO_TARGET_DIR"], "release", "morsel"))
    candidates.extend([
        "/dev/shm/morsel-alt/target/release/morsel",
        os.path.join(REPO_ROOT, "target", "release", "morsel"),
    ])

    which_morsel = shutil.which("morsel")
    if which_morsel:
        candidates.append(which_morsel)

    for c in candidates:
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c

    raise FileNotFoundError(
        "Could not find morsel binary. Please build it (`cargo build --release -p morsel-cli`) "
        "or specify `--morsel PATH`."
    )


def classify_result(
    timed_out: bool,
    returncode: Optional[int],
    stdout: str,
    stderr: str,
    ref_csv_path: Optional[str],
    q_nr: int,
) -> Tuple[str, str]:
    """
    Classifies the outcome of running a query.
    Outcomes: 'pass', 'mismatch', 'plan_error', 'exec_error', 'timeout', 'oom'
    Returns: (outcome, error_or_difference_text)
    """
    if timed_out:
        return "timeout", "query timed out"

    combined = (stderr + "\n" + stdout).lower()

    # OOM detection
    oom_indicators = [
        "out of memory",
        "cannot allocate memory",
        "memory allocation of",
        "failed to allocate",
        "alloc error",
        "out_of_memory",
    ]
    is_oom = any(ind in combined for ind in oom_indicators)
    # Signal 9 (SIGKILL) is returncode -9 or 137 (128 + 9)
    if returncode in (-9, 137) or is_oom:
        err_msg = stderr.strip() or stdout.strip() or "Process killed due to memory limit (OOM)"
        return "oom", err_msg

    # Plan error detection
    plan_indicators = [
        "plan error:",
        "plan panic:",
        "binder error:",
        "bind error:",
        "optimizer error:",
        "optimize error:",
    ]
    is_plan_err = any(ind in combined for ind in plan_indicators) or stderr.strip().startswith("plan ")

    if returncode != 0:
        err_msg = stderr.strip() or stdout.strip() or f"Process failed with exit code {returncode}"
        if is_plan_err:
            return "plan_error", err_msg
        return "exec_error", err_msg

    # returncode == 0: check output against reference
    if not ref_csv_path or not os.path.exists(ref_csv_path):
        return "exec_error", f"Reference file not found: {ref_csv_path}"

    try:
        act_reader = csv.reader(io.StringIO(stdout))
        act_rows_all = [row for row in act_reader if row]
        if not act_rows_all:
            return "mismatch", "Actual output is empty"
        act_header = act_rows_all[0]
        act_rows = act_rows_all[1:]

        exp_header, exp_rows = read_csv_file(ref_csv_path)

        if len(act_header) != len(exp_header):
            return (
                "mismatch",
                f"Mismatch in header column count: expected {len(exp_header)}, got {len(act_header)}",
            )

        for i, (a_h, e_h) in enumerate(zip(act_header, exp_header)):
            if a_h.strip().lower() != e_h.strip().lower():
                return "mismatch", f"Header mismatch at column {i}: expected '{e_h}', got '{a_h}'"

        order_keys = ORDER_KEY_COUNTS.get(q_nr, 0)
        diffs = compare(act_rows, exp_rows, order_keys)
        if diffs:
            return "mismatch", diffs[0]

        return "pass", ""
    except Exception as e:
        return "exec_error", f"Error during result comparison: {e}"


def run_single_query(
    morsel_bin: str,
    sql_path: str,
    data_dir: str,
    ref_csv_path: Optional[str],
    q_nr: int,
    threads: int,
    timeout: float,
    mem_gb: float,
) -> Dict[str, Any]:
    """Run a single query under memory cap and timeout, and classify result."""
    cmd = [
        morsel_bin,
        "query",
        "--data",
        data_dir,
        "--sql-file",
        sql_path,
        "--threads",
        str(threads),
    ]

    def set_limits():
        if mem_gb is not None and mem_gb > 0:
            bytes_limit = int(mem_gb * 1024 * 1024 * 1024)
            try:
                import resource
                resource.setrlimit(resource.RLIMIT_AS, (bytes_limit, bytes_limit))
            except Exception:
                pass

    t0 = time.perf_counter()
    timed_out = False
    stdout = ""
    stderr = ""
    returncode = None

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=set_limits if os.name != "nt" else None,
        )
        stdout, stderr = proc.communicate(timeout=timeout)
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.kill()
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except Exception:
            pass

    t1 = time.perf_counter()
    wall_ms = round((t1 - t0) * 1000.0, 2)

    outcome, error_text = classify_result(
        timed_out=timed_out,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        ref_csv_path=ref_csv_path,
        q_nr=q_nr,
    )

    return {
        "outcome": outcome,
        "error": error_text,
        "wall_ms": wall_ms,
    }


def find_sql_file(queries_dir: str, q_nr: int) -> Optional[str]:
    """Find query file like q01.sql or q1.sql."""
    candidates = [
        os.path.join(queries_dir, f"q{q_nr:02d}.sql"),
        os.path.join(queries_dir, f"q{q_nr}.sql"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def find_ref_csv(refs_dir: str, q_nr: int) -> Optional[str]:
    """Find reference csv file like q01.csv or q1.csv."""
    candidates = [
        os.path.join(refs_dir, f"q{q_nr:02d}.csv"),
        os.path.join(refs_dir, f"q{q_nr}.csv"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def print_summary_table(
    results: List[Dict[str, Any]],
    sf: Any,
    threads: int,
    commit: str,
) -> None:
    """Print standard Markdown summary table."""
    print()
    print(f"| Query | Outcome | Wall (ms) | Details |")
    print(f"|:---|:---|---:|:---|")

    total_wall_ms = 0.0
    pass_count = 0

    for r in results:
        q_name = r["query"]
        outcome = r["outcome"]
        wall_ms = r["wall_ms"]
        total_wall_ms += wall_ms
        if outcome == "pass":
            pass_count += 1
        err = r.get("error") or ""
        # Truncate details if very long
        if len(err) > 60:
            err = err[:57] + "..."
        err = err.replace("|", "\\|").replace("\n", " ")
        print(f"| {q_name:<5} | {outcome:<9} | {wall_ms:>9.1f} | {err} |")

    print(
        f"| Total | {pass_count}/{len(results)} pass | {total_wall_ms:>9.1f} | sf={sf}, threads={threads}, commit={commit} |"
    )
    print()


def main():
    parser = argparse.ArgumentParser(description="Morsel TPC-H End-to-End Runner")
    parser.add_argument("--data", required=True, help="Path to parquet data directory")
    parser.add_argument("--refs", required=True, help="Path to reference CSV directory")
    parser.add_argument(
        "--queries",
        default="1-22",
        help="Query numbers to run (e.g. 1-22, 1,2,3, q1-q5). Default: 1-22",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=12,
        help="Number of query execution threads (default: 12)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Timeout in seconds per query (default: 300)",
    )
    parser.add_argument(
        "--mem-gb",
        type=float,
        default=20.0,
        help="Memory limit in GB per query (default: 20)",
    )
    parser.add_argument(
        "--out",
        help="Output JSONL file path to write results",
    )
    parser.add_argument(
        "--morsel",
        help="Path to morsel binary executable",
    )
    parser.add_argument(
        "--queries-dir",
        default=os.path.join(REPO_ROOT, "queries", "tpch"),
        help="Directory containing query SQL files (default: queries/tpch)",
    )
    parser.add_argument(
        "--sf",
        type=float,
        default=None,
        help="Explicit scale factor (inferred from --data / --refs if omitted)",
    )

    args = parser.parse_args()

    morsel_bin = find_morsel_binary(args.morsel)
    data_dir = os.path.abspath(args.data)
    refs_dir = os.path.abspath(args.refs)
    queries_dir = os.path.abspath(args.queries_dir)

    sf = infer_scale_factor(data_dir, refs_dir, args.sf)
    commit = get_git_commit()
    host = socket.gethostname()

    selected_queries = parse_query_selection(args.queries)

    if args.out:
        out_dir = os.path.dirname(os.path.abspath(args.out))
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        # Clear or truncate output file at start of run
        open(args.out, "w").close()

    results = []

    print(f"Running TPC-H E2E: SF={sf}, Threads={args.threads}, Morsel={morsel_bin}")
    print(f"Queries: {selected_queries}")
    print("-" * 60)

    for q_nr in selected_queries:
        q_name = f"q{q_nr:02d}"
        sql_path = find_sql_file(queries_dir, q_nr)
        ref_csv = find_ref_csv(refs_dir, q_nr)

        if not sql_path:
            res_rec = {
                "query": q_name,
                "outcome": "exec_error",
                "error": f"SQL file not found in {queries_dir}",
                "first_difference": f"SQL file not found in {queries_dir}",
                "wall_ms": 0.0,
                "commit": commit,
                "host": host,
                "sf": sf,
                "threads": args.threads,
            }
        else:
            run_res = run_single_query(
                morsel_bin=morsel_bin,
                sql_path=sql_path,
                data_dir=data_dir,
                ref_csv_path=ref_csv,
                q_nr=q_nr,
                threads=args.threads,
                timeout=args.timeout,
                mem_gb=args.mem_gb,
            )
            res_rec = {
                "query": q_name,
                "outcome": run_res["outcome"],
                "error": run_res["error"],
                "first_difference": run_res["error"],
                "wall_ms": run_res["wall_ms"],
                "commit": commit,
                "host": host,
                "sf": sf,
                "threads": args.threads,
            }

        results.append(res_rec)
        print(
            f"[{q_name}] {res_rec['outcome']:<10} {res_rec['wall_ms']:>8.1f} ms  {res_rec['error'][:40]}"
        )

        if args.out:
            with open(args.out, "a", encoding="utf-8") as f:
                f.write(json.dumps(res_rec) + "\n")

    print_summary_table(results, sf=sf, threads=args.threads, commit=commit)

    # Return non-zero exit code if any query failed
    all_passed = all(r["outcome"] == "pass" for r in results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
