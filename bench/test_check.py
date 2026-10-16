import unittest
from bench.check import compare


class TestCheck(unittest.TestCase):
    def test_exact_match(self):
        expected = [["a", "1", "10.50"], ["b", "2", "20.00"]]
        actual = [["a", "1", "10.50"], ["b", "2", "20.00"]]
        diffs = compare(actual, expected, order_key_count=1)
        self.assertEqual(diffs, [])

    def test_tolerance_cent(self):
        # 0.005 apart passes, 0.02 fails for values under 1e7
        expected = [["100.00"]]
        actual_pass = [["100.005"]]
        self.assertEqual(compare(actual_pass, expected, order_key_count=0), [])

        actual_fail = [["100.02"]]
        diffs = compare(actual_fail, expected, order_key_count=0)
        self.assertTrue(len(diffs) > 0)

    def test_tolerance_relative_large(self):
        # 1e12 values 500 apart pass (1e-9 * 1e12 = 1000)
        expected = [["1000000000000.00"]]
        actual = [["1000000000500.00"]]
        self.assertEqual(compare(actual, expected, order_key_count=0), [])

    def test_wrong_order_fails(self):
        expected = [["a", "1"], ["b", "2"]]
        actual = [["b", "2"], ["a", "1"]]
        diffs = compare(actual, expected, order_key_count=1)
        self.assertTrue(len(diffs) > 0)

    def test_ties_may_reorder(self):
        # rows equal on all order keys may come in any order
        expected = [["a", "1", "x"], ["a", "2", "y"], ["b", "3", "z"]]
        actual = [["a", "2", "y"], ["a", "1", "x"], ["b", "3", "z"]]
        diffs = compare(actual, expected, order_key_count=1)
        self.assertEqual(diffs, [])

    def test_column_count_mismatch_fails(self):
        expected = [["a", "1"]]
        actual = [["a", "1", "extra"]]
        diffs = compare(actual, expected, order_key_count=1)
        self.assertTrue(len(diffs) > 0)

    def test_null_vs_empty_string_differ(self):
        expected = [["NULL"]]
        actual = [[""]]
        diffs = compare(actual, expected, order_key_count=0)
        self.assertTrue(len(diffs) > 0)


if __name__ == "__main__":
    unittest.main()
