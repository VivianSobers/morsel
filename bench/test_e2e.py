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

from bench.e2e import classify_result, parse_query_selection


class TestE2EClassification(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.ref_csv = os.path.join(self.test_dir, "q01.csv")
        with open(self.ref_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["col1", "col2"])
            writer.writerow(["1", "100.00"])

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_pass(self):
        stdout = "col1,col2\n1,100.00\n"
        outcome, err = classify_result(
            timed_out=False,
            returncode=0,
            stdout=stdout,
            stderr="",
            ref_csv_path=self.ref_csv,
            q_nr=1,
        )
        self.assertEqual(outcome, "pass")
        self.assertEqual(err, "")

    def test_mismatch(self):
        stdout = "col1,col2\n1,999.99\n"
        outcome, err = classify_result(
            timed_out=False,
            returncode=0,
            stdout=stdout,
            stderr="",
            ref_csv_path=self.ref_csv,
            q_nr=1,
        )
        self.assertEqual(outcome, "mismatch")
        self.assertIn("100.00", err)
        self.assertIn("999.99", err)

    def test_plan_error(self):
        outcome, err = classify_result(
            timed_out=False,
            returncode=1,
            stdout="",
            stderr="plan error: column 'foo' not found in table 'lineitem'",
            ref_csv_path=self.ref_csv,
            q_nr=1,
        )
        self.assertEqual(outcome, "plan_error")
        self.assertIn("plan error", err)

    def test_exec_error(self):
        outcome, err = classify_result(
            timed_out=False,
            returncode=1,
            stdout="",
            stderr="execute error: division by zero in morsel evaluation",
            ref_csv_path=self.ref_csv,
            q_nr=1,
        )
        self.assertEqual(outcome, "exec_error")
        self.assertIn("execute error", err)

    def test_timeout(self):
        outcome, err = classify_result(
            timed_out=True,
            returncode=None,
            stdout="",
            stderr="",
            ref_csv_path=self.ref_csv,
            q_nr=1,
        )
        self.assertEqual(outcome, "timeout")

    def test_oom(self):
        outcome, err = classify_result(
            timed_out=False,
            returncode=137,
            stdout="",
            stderr="memory allocation of 8589934592 bytes failed",
            ref_csv_path=self.ref_csv,
            q_nr=1,
        )
        self.assertEqual(outcome, "oom")

    def test_parse_query_selection(self):
        self.assertEqual(parse_query_selection("1-3"), [1, 2, 3])
        self.assertEqual(parse_query_selection("q1,q3,q5"), [1, 3, 5])
        self.assertEqual(parse_query_selection("1-2, 5, 8-10"), [1, 2, 5, 8, 9, 10])


class TestE2EWithFakeMorsel(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.test_dir, "data")
        os.makedirs(self.data_dir)
        self.refs_dir = os.path.join(self.test_dir, "refs")
        os.makedirs(self.refs_dir)
        self.queries_dir = os.path.join(self.test_dir, "queries")
        os.makedirs(self.queries_dir)

        # Create reference CSVs and SQL files for queries 1..6
        for q in range(1, 7):
            q_name = f"q{q:02d}"
            with open(os.path.join(self.queries_dir, f"{q_name}.sql"), "w") as f:
                f.write(f"SELECT {q};\n")
            with open(os.path.join(self.refs_dir, f"{q_name}.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["val"])
                writer.writerow([str(q)])

        # Create fake morsel script
        self.fake_morsel = os.path.join(self.test_dir, "fake_morsel.py")
        script_content = """#!/usr/bin/env python3
import sys
import time

# Parse --sql-file
sql_file = ""
for i, arg in enumerate(sys.argv):
    if arg == "--sql-file" and i + 1 < len(sys.argv):
        sql_file = sys.argv[i + 1]

if "q01.sql" in sql_file:
    # PASS
    print("val")
    print("1")
    sys.exit(0)
elif "q02.sql" in sql_file:
    # MISMATCH
    print("val")
    print("999")
    sys.exit(0)
elif "q03.sql" in sql_file:
    # PLAN ERROR
    sys.stderr.write("plan error: failed to resolve table\\n")
    sys.exit(1)
elif "q04.sql" in sql_file:
    # EXEC ERROR
    sys.stderr.write("execute error: hash join build error\\n")
    sys.exit(1)
elif "q05.sql" in sql_file:
    # OOM
    sys.stderr.write("fatal runtime error: memory allocation of 1000000000 bytes failed\\n")
    sys.exit(137)
elif "q06.sql" in sql_file:
    # TIMEOUT
    time.sleep(5)
    sys.exit(0)
else:
    sys.stderr.write("unknown query\\n")
    sys.exit(1)
"""
        with open(self.fake_morsel, "w") as f:
            f.write(script_content)
        st = os.stat(self.fake_morsel)
        os.chmod(self.fake_morsel, st.st_mode | stat.S_IEXEC)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_fake_morsel_end_to_end(self):
        out_jsonl = os.path.join(self.test_dir, "results.jsonl")
        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "e2e.py"),
            "--data",
            self.data_dir,
            "--refs",
            self.refs_dir,
            "--queries-dir",
            self.queries_dir,
            "--queries",
            "1-6",
            "--morsel",
            self.fake_morsel,
            "--timeout",
            "0.5",
            "--out",
            out_jsonl,
        ]
        # Since queries 2-6 fail, the runner should exit with non-zero
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(proc.returncode, 1)

        # Read JSONL output
        self.assertTrue(os.path.exists(out_jsonl))
        records = {}
        with open(out_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line)
                    records[rec["query"]] = rec

        self.assertEqual(len(records), 6)
        self.assertEqual(records["q01"]["outcome"], "pass")
        self.assertEqual(records["q02"]["outcome"], "mismatch")
        self.assertEqual(records["q03"]["outcome"], "plan_error")
        self.assertEqual(records["q04"]["outcome"], "exec_error")
        self.assertEqual(records["q05"]["outcome"], "oom")
        self.assertEqual(records["q06"]["outcome"], "timeout")


if __name__ == "__main__":
    unittest.main()
