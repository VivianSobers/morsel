import argparse
import csv
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
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


def sha256_file(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H data and reference answers")
    parser.add_argument("--sf", type=float, required=True, help="Scale factor (e.g. 1 or 10)")
    parser.add_argument("--data-dir", type=str, required=True, help="Directory to output Parquet files")
    parser.add_argument("--refs-dir", type=str, required=True, help="Directory to output CSV references and manifest")
    parser.add_argument(
        "--queries-dir",
        type=str,
        default="queries/tpch",
        help="Directory to write query text files",
    )
    args = parser.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.refs_dir, exist_ok=True)
    os.makedirs(args.queries_dir, exist_ok=True)

    print(f"Starting DuckDB dbgen for SF={args.sf}...")
    conn = duckdb.connect()
    conn.execute("SET memory_limit='30GB'")
    conn.execute("SET threads=12")
    conn.execute(f"CALL dbgen(sf={args.sf})")

    # 1. Export Parquet tables
    table_rows = {}
    parquet_shas = {}
    for table in TPCH_TABLES:
        parquet_path = os.path.join(args.data_dir, f"{table}.parquet")
        print(f"Exporting {table} to {parquet_path}...")
        conn.execute(f"COPY {table} TO '{parquet_path}' (FORMAT PARQUET)")
        row_count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        table_rows[table] = row_count
        parquet_shas[table] = sha256_file(parquet_path)
        print(f"  {table}: {row_count} rows, sha256: {parquet_shas[table][:16]}...")

    # 2. Extract queries from tpch_queries()
    queries = conn.execute(
        "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
    ).fetchall()

    for nr, sql_text in queries:
        sql_path = os.path.join(args.queries_dir, f"q{nr:02d}.sql")
        if not os.path.exists(sql_path):
            with open(sql_path, "w", encoding="utf-8") as f:
                f.write(sql_text.strip() + "\n")

    # 3. Run each query and export reference answer CSV
    answer_shas = {}
    print("Running queries and exporting reference answers...")
    for nr, sql_text in queries:
        clean_sql = sql_text.strip().rstrip(";")
        csv_filename = f"q{nr:02d}.csv"
        csv_path = os.path.join(args.refs_dir, csv_filename)
        conn.execute(f"COPY ({clean_sql}) TO '{csv_path}' (HEADER, DELIMITER ',')")
        answer_shas[csv_filename] = sha256_file(csv_path)

    # 4. Compare with tpch_answers() at SF1
    if abs(args.sf - 1.0) < 1e-6:
        print("Comparing answers with official tpch_answers() at SF1...")
        for nr in range(1, 23):
            csv_filename = f"q{nr:02d}.csv"
            csv_path = os.path.join(args.refs_dir, csv_filename)
            _, act_rows = read_csv_file(csv_path)

            ans_text = conn.execute(
                "SELECT answer FROM tpch_answers() WHERE query_nr=? AND scale_factor=1.0",
                [nr],
            ).fetchone()[0]
            lines = [l.strip() for l in ans_text.strip().split("\n") if l.strip()]
            reader = csv.reader(lines, delimiter="|")
            exp_rows_all = list(reader)
            exp_rows = exp_rows_all[1:]  # skip header

            order_key_count = ORDER_KEY_COUNTS.get(nr, 0)
            diffs = compare(act_rows, exp_rows, order_key_count)
            if diffs:
                print(f"Error: Mismatch for Q{nr:02d} with tpch_answers(): {diffs[0]}")
                sys.exit(1)
        print("All 22 queries passed comparison with tpch_answers()!")

    # 5. Write MANIFEST.json
    manifest = {
        "duckdb_version": duckdb.__version__,
        "scale_factor": args.sf,
        "row_counts": table_rows,
        "parquet_sha256": parquet_shas,
        "answer_sha256": answer_shas,
        "tables": {
            tbl: {"rows": table_rows[tbl], "sha256": parquet_shas[tbl]}
            for tbl in TPCH_TABLES
        },
        "answers": answer_shas,
    }

    manifest_path = os.path.join(args.refs_dir, "MANIFEST.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote manifest to {manifest_path}")
    print("Done!")


if __name__ == "__main__":
    main()
