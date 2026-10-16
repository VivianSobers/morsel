#!/usr/bin/env python3
import argparse
import glob
import json
import math
import os
import sys
from typing import Any, Dict, List, Optional, Tuple


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "N/A"
    if seconds < 1.0:
        return f"{seconds * 1000.0:.1f} ms"
    return f"{seconds:.2f} s"


def load_results(results_dir: str) -> Tuple[Dict[Tuple[str, float, str], Dict[str, Any]], Dict[str, Any]]:
    pattern = os.path.join(results_dir, "*.jsonl")
    files = glob.glob(pattern)
    records: Dict[Tuple[str, float, str], Dict[str, Any]] = {}
    metadata: Dict[str, Any] = {
        "engines": {},
        "cpu_model": "unknown",
        "ram": "unknown",
        "threads": None,
        "notes": set(),
    }

    for fpath in sorted(files):
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                engine = rec.get("engine")
                sf = float(rec.get("sf", 1.0))
                query = rec.get("query")
                if not engine or not query:
                    continue

                records[(engine, sf, query)] = rec

                if engine not in metadata["engines"]:
                    metadata["engines"][engine] = rec.get("engine_version", "unknown")
                if rec.get("cpu_model"):
                    metadata["cpu_model"] = rec["cpu_model"]
                if rec.get("ram"):
                    metadata["ram"] = rec["ram"]
                if rec.get("threads"):
                    metadata["threads"] = rec["threads"]
                if rec.get("note"):
                    metadata["notes"].add(rec["note"])

    return records, metadata


def generate_markdown(records: Dict[Tuple[str, float, str], Dict[str, Any]], metadata: Dict[str, Any]) -> str:
    engines = ["duckdb", "polars", "datafusion"]
    scale_factors = [1.0, 10.0]

    lines = []
    lines.append("# Preliminary Baseline Benchmark Results")
    lines.append("")
    if metadata.get("notes"):
        for note in sorted(metadata["notes"]):
            lines.append(f"> **Note:** {note}")
        lines.append("")

    lines.append("## Environment")
    lines.append("")
    lines.append(f"- **CPU:** {metadata.get('cpu_model', 'unknown')}")
    lines.append(f"- **RAM:** {metadata.get('ram', 'unknown')}")
    lines.append(f"- **Threads:** {metadata.get('threads', 'unknown')}")
    lines.append("- **Engine Versions:**")
    for eng in engines:
        ver = metadata["engines"].get(eng, "not run")
        lines.append(f"  - `{eng}`: {ver}")
    lines.append("")

    lines.append("## Per-Query Median Execution Times (5 timed runs, 1 warm-up)")
    lines.append("")

    header_cols = ["Query"]
    for sf in scale_factors:
        sf_str = f"SF{int(sf)}" if sf.is_integer() else f"SF{sf}"
        for eng in engines:
            header_cols.append(f"{eng} ({sf_str})")

    lines.append("| " + " | ".join(header_cols) + " |")
    lines.append("|" + "|".join(["---:"] + ["---:"] * (len(header_cols) - 1)) + "|")

    # Accumulators for geometric mean and total
    totals: Dict[Tuple[str, float], float] = {(eng, sf): 0.0 for eng in engines for sf in scale_factors}
    log_sums: Dict[Tuple[str, float], float] = {(eng, sf): 0.0 for eng in engines for sf in scale_factors}
    counts: Dict[Tuple[str, float], int] = {(eng, sf): 0 for eng in engines for sf in scale_factors}

    for q_nr in range(1, 23):
        q_name = f"q{q_nr:02d}"
        row = [f"**{q_name.upper()}**"]
        for sf in scale_factors:
            for eng in engines:
                key = (eng, sf, q_name)
                rec = records.get(key)
                if rec is None:
                    row.append("-")
                elif rec.get("median") is not None:
                    med = rec["median"]
                    check = rec.get("check", "PASS")
                    suffix = "" if check == "PASS" else " (check failed)"
                    row.append(f"{format_duration(med)}{suffix}")
                    totals[(eng, sf)] += med
                    log_sums[(eng, sf)] += math.log(med)
                    counts[(eng, sf)] += 1
                else:
                    err = rec.get("check", "ERROR")
                    row.append(f"ERR")
        lines.append("| " + " | ".join(row) + " |")

    # Total row
    total_row = ["**Total**"]
    for sf in scale_factors:
        for eng in engines:
            c = counts[(eng, sf)]
            if c == 22:
                total_row.append(f"**{format_duration(totals[(eng, sf)])}**")
            else:
                total_row.append("-")
    lines.append("| " + " | ".join(total_row) + " |")

    # Geometric mean row
    geomean_row = ["**Geomean**"]
    for sf in scale_factors:
        for eng in engines:
            c = counts[(eng, sf)]
            if c > 0:
                geo = math.exp(log_sums[(eng, sf)] / c)
                geomean_row.append(f"**{format_duration(geo)}**")
            else:
                geomean_row.append("-")
    lines.append("| " + " | ".join(geomean_row) + " |")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Summarize TPC-H baseline results")
    parser.add_argument(
        "--results-dir",
        default="bench/results/prelim",
        help="Directory containing JSONL results",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output README.md path (defaults to stdout or <results-dir>/README.md if specified)",
    )
    args = parser.parse_args()

    records, metadata = load_results(args.results_dir)
    md_content = generate_markdown(records, metadata)

    if args.out:
        out_dir = os.path.dirname(args.out)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"Summary written to {args.out}")
    else:
        print(md_content)


if __name__ == "__main__":
    main()
