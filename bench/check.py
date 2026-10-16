import csv
import math
import sys
import argparse
from typing import List, Optional, Tuple, Any

# Number of leading ORDER BY keys for TPC-H queries Q1-Q22
ORDER_KEY_COUNTS = {
    1: 2,
    2: 4,
    3: 2,
    4: 1,
    5: 1,
    6: 0,
    7: 3,
    8: 1,
    9: 2,
    10: 1,
    11: 1,
    12: 1,
    13: 2,
    14: 0,
    15: 1,
    16: 4,
    17: 0,
    18: 2,
    19: 0,
    20: 1,
    21: 2,
    22: 1,
}


def parse_query_number(q_str: str) -> int:
    s = q_str.strip().lower()
    if s.startswith("q"):
        s = s[1:]
    return int(s)


def cell_equal(actual: str, expected: str) -> bool:
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False

    actual_s = str(actual).strip()
    expected_s = str(expected).strip()

    # Null vs empty string
    if (actual_s.upper() == "NULL" and expected_s == "") or (
        actual_s == "" and expected_s.upper() == "NULL"
    ):
        return False

    if actual_s.upper() == "NULL" and expected_s.upper() == "NULL":
        return True

    if actual_s == expected_s:
        return True

    # Numeric comparison
    try:
        a_val = float(actual_s)
        b_val = float(expected_s)
        if math.isnan(a_val) and math.isnan(b_val):
            return True
        tol = max(0.01, 1e-9 * abs(b_val))
        return abs(a_val - b_val) <= tol
    except ValueError:
        pass

    return actual_s == expected_s


def rows_equal(actual_row: List[str], expected_row: List[str]) -> Tuple[bool, Optional[int]]:
    if len(actual_row) != len(expected_row):
        return False, min(len(actual_row), len(expected_row))
    for c, (a_cell, e_cell) in enumerate(zip(actual_row, expected_row)):
        if not cell_equal(a_cell, e_cell):
            return False, c
    return True, None


def compare(
    actual_rows: List[List[str]],
    expected_rows: List[List[str]],
    order_key_count: int = 0,
) -> List[str]:
    diffs = []
    if len(actual_rows) != len(expected_rows):
        return [
            f"Row count mismatch: expected {len(expected_rows)} rows, got {len(actual_rows)} rows"
        ]

    if not expected_rows:
        return []

    # Check column counts across all rows
    expected_col_count = len(expected_rows[0])
    for r, row in enumerate(expected_rows):
        if len(row) != expected_col_count:
            return [
                f"Expected row {r} column count mismatch: expected {expected_col_count}, got {len(row)}"
            ]
    for r, row in enumerate(actual_rows):
        if len(row) != expected_col_count:
            return [
                f"Actual row {r} column count mismatch: expected {expected_col_count}, got {len(row)}"
            ]

    k = min(order_key_count, expected_col_count)

    # Group expected rows by order keys (consecutive identical order keys)
    i = 0
    total = len(expected_rows)
    while i < total:
        start = i
        if k == 0:
            end = total
        else:
            while end_candidate := i + 1:
                if end_candidate >= total:
                    break
                # Check if next expected row has same order keys
                same_keys = True
                for col_idx in range(k):
                    if not cell_equal(
                        expected_rows[end_candidate][col_idx],
                        expected_rows[start][col_idx],
                    ):
                        same_keys = False
                        break
                if not same_keys:
                    break
                i = end_candidate
            end = i + 1

        # Block is [start, end)
        exp_block = expected_rows[start:end]
        act_block = actual_rows[start:end]

        # First, ensure actual block rows share the expected order keys
        for r_offset, act_row in enumerate(act_block):
            row_idx = start + r_offset
            for col_idx in range(k):
                if not cell_equal(act_row[col_idx], exp_block[0][col_idx]):
                    return [
                        f"Row {row_idx}, column {col_idx}: order mismatch (expected key '{exp_block[0][col_idx]}', got '{act_row[col_idx]}')"
                    ]

        # If block size == 1, exact row match
        if len(exp_block) == 1:
            eq, diff_col = rows_equal(act_block[0], exp_block[0])
            if not eq:
                return [
                    f"Row {start}, column {diff_col}: expected '{exp_block[0][diff_col]}', got '{act_block[0][diff_col]}'"
                ]
        else:
            # Tie group: multiset match between exp_block and act_block
            unmatched_act = list(range(len(act_block)))
            for exp_idx, exp_row in enumerate(exp_block):
                found = False
                for a_i, act_idx in enumerate(unmatched_act):
                    eq, _ = rows_equal(act_block[act_idx], exp_row)
                    if eq:
                        unmatched_act.pop(a_i)
                        found = True
                        break
                if not found:
                    # Report first differing column against first remaining unmatched actual row
                    first_unmatched = unmatched_act[0] if unmatched_act else 0
                    _, diff_col = rows_equal(act_block[first_unmatched], exp_row)
                    return [
                        f"Row {start + first_unmatched}, column {diff_col}: tie group mismatch (expected row matching '{exp_row}')"
                    ]

        i = end

    return diffs


def read_csv_file(path: str) -> Tuple[List[str], List[List[str]]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = [row for row in reader if row]
    if not rows:
        return [], []
    return rows[0], rows[1:]


def main():
    parser = argparse.ArgumentParser(description="TPC-H answer checker")
    parser.add_argument("actual", help="Path to actual CSV")
    parser.add_argument("expected", help="Path to expected CSV")
    parser.add_argument("--query", "-q", help="Query number (1-22 or q01-q22)", required=False)
    parser.add_argument("--order-keys", "-k", type=int, help="Number of order keys", default=None)
    args = parser.parse_args()

    if args.order_keys is not None:
        order_key_count = args.order_keys
    elif args.query is not None:
        q_nr = parse_query_number(args.query)
        order_key_count = ORDER_KEY_COUNTS.get(q_nr, 0)
    else:
        order_key_count = 0

    act_header, act_rows = read_csv_file(args.actual)
    exp_header, exp_rows = read_csv_file(args.expected)

    if act_header != exp_header:
        # Check column count
        if len(act_header) != len(exp_header):
            print(
                f"Mismatch in header column count: expected {len(exp_header)}, got {len(act_header)}"
            )
            sys.exit(1)
        for i, (a_h, e_h) in enumerate(zip(act_header, exp_header)):
            if a_h.strip().lower() != e_h.strip().lower():
                print(f"Header mismatch at column {i}: expected '{e_h}', got '{a_h}'")
                sys.exit(1)

    diffs = compare(act_rows, exp_rows, order_key_count)
    if diffs:
        print(f"Mismatch: {diffs[0]}")
        sys.exit(1)

    print("OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
