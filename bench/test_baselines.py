#!/usr/bin/env python3
import csv
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench.baselines import parse_query_selection
from bench.summarize import load_results, generate_markdown


class TestBaselinesWithFakeMorsel(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.test_dir, "data")
        os.makedirs(self.data_dir)
        self.refs_dir = os.path.join(self.test_dir, "refs")
        os.makedirs(self.refs_dir)
        self.queries_dir = os.path.join(self.test_dir, "queries")
        os.makedirs(self.queries_dir)

        # Create reference CSVs and SQL files for queries 1..2
        for q in range(1, 3):
            q_name = f"q{q:02d}"
            with open(os.path.join(self.queries_dir, f"{q_name}.sql"), "w") as f:
                f.write(f"SELECT {q};\n")
            with open(os.path.join(self.refs_dir, f"{q_name}.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["val"])
                writer.writerow([str(q)])

        # Create fake morsel binary
        self.fake_morsel = os.path.join(self.test_dir, "fake_morsel.py")
        script_content = """#!/usr/bin/env python3
import json
import os
import sys

sql_file = ""
timing_json = ""
out_file = ""

i = 1
while i < len(sys.argv):
    arg = sys.argv[i]
    if arg == "--sql-file" and i + 1 < len(sys.argv):
        sql_file = sys.argv[i + 1]
        i += 1
    elif arg == "--timing-json" and i + 1 < len(sys.argv):
        timing_json = sys.argv[i + 1]
        i += 1
    elif arg == "--out" and i + 1 < len(sys.argv):
        out_file = sys.argv[i + 1]
        i += 1
    i += 1

if timing_json:
    # Write 6 timing records (1 warm-up + 5 timed runs: 10ms, 12ms, 11ms, 13ms, 14ms)
    with open(timing_json, "a") as f:
        f.write(json.dumps({"load_ms": 100.0, "plan_ms": 1.0, "execute_ms": 10.0, "git_commit": "fake123", "hostname": "fake"}) + "\\n")
        f.write(json.dumps({"load_ms": 100.0, "plan_ms": 1.0, "execute_ms": 12.0, "git_commit": "fake123", "hostname": "fake"}) + "\\n")
        f.write(json.dumps({"load_ms": 100.0, "plan_ms": 1.0, "execute_ms": 11.0, "git_commit": "fake123", "hostname": "fake"}) + "\\n")
        f.write(json.dumps({"load_ms": 100.0, "plan_ms": 1.0, "execute_ms": 13.0, "git_commit": "fake123", "hostname": "fake"}) + "\\n")
        f.write(json.dumps({"load_ms": 100.0, "plan_ms": 1.0, "execute_ms": 14.0, "git_commit": "fake123", "hostname": "fake"}) + "\\n")
        f.write(json.dumps({"load_ms": 100.0, "plan_ms": 1.0, "execute_ms": 12.5, "git_commit": "fake123", "hostname": "fake"}) + "\\n")

if "q01.sql" in sql_file:
    # PASS
    csv_content = "val\\n1\\n"
elif "q02.sql" in sql_file:
    # FAIL / MISMATCH
    csv_content = "val\\n999\\n"
else:
    sys.exit(1)

if out_file:
    with open(out_file, "w") as f:
        f.write(csv_content)
else:
    sys.stdout.write(csv_content)

sys.exit(0)
"""
        with open(self.fake_morsel, "w") as f:
            f.write(script_content)
        st = os.stat(self.fake_morsel)
        os.chmod(self.fake_morsel, st.st_mode | stat.S_IEXEC)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_parse_query_selection(self):
        self.assertEqual(parse_query_selection("1-3"), [1, 2, 3])
        self.assertEqual(parse_query_selection("q1,q3,q5"), [1, 3, 5])
        self.assertEqual(parse_query_selection(None), list(range(1, 23)))

    def test_baselines_with_fake_morsel(self):
        out_jsonl = os.path.join(self.test_dir, "results.jsonl")
        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "baselines.py"),
            "--engine",
            "morsel",
            "--morsel",
            self.fake_morsel,
            "--sf",
            "1",
            "--data-dir",
            self.data_dir,
            "--threads",
            "4",
            "--queries-dir",
            self.queries_dir,
            "--refs-dir",
            self.refs_dir,
            "--queries",
            "1-2",
            "--out",
            out_jsonl,
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(proc.returncode, 0, f"baselines.py failed: {proc.stderr}")

        # Verify JSONL records
        self.assertTrue(os.path.exists(out_jsonl))
        records = {}
        with open(out_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line)
                    records[rec["query"]] = rec

        self.assertIn("q01", records)
        self.assertIn("q02", records)

        q1 = records["q01"]
        self.assertEqual(q1["engine"], "morsel")
        self.assertEqual(q1["engine_version"], "fake123")
        self.assertEqual(q1["sf"], 1.0)
        self.assertEqual(q1["threads"], 4)
        self.assertEqual(q1["check"], "PASS")
        # 5 timed runs from index 1..5: 12.0, 11.0, 13.0, 14.0, 12.5 ms -> / 1000.0
        self.assertEqual(len(q1["times"]), 5)
        self.assertAlmostEqual(q1["median"], 0.0125, places=5)
        self.assertAlmostEqual(q1["min"], 0.011, places=5)

        q2 = records["q02"]
        self.assertEqual(q2["engine"], "morsel")
        self.assertTrue(q2["check"].startswith("FAIL:"))

        # Verify summarize.py generates table with morsel column
        results_dir = os.path.dirname(out_jsonl)
        recs, meta = load_results(results_dir)
        md = generate_markdown(recs, meta)
        self.assertIn("morsel (SF1)", md)
        self.assertIn("**Q01**", md)


if __name__ == "__main__":
    unittest.main()
