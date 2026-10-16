# Discovered Engine Bugs

| Seed | Operator / Bug Summary | Count | Status | Current Outcome | Notes |
|:---|:---|:---|:---|:---|:---|
| `9061`, `36132` | Panic: user-provided comparison function does not correctly implement a total order | 4 | Fixed by 1404b71 | Pass | Hash aggregate key sorting panic in `compare_scalars`. Fixed by removing redundant sort in aggregate. |
| `57084` | Panic: user-provided comparison function does not correctly implement a total order | 1 | Fixed by 1404b71 | Pass | Left outer join followed by aggregate (`Scan(customer) -> Join(orders, Left) -> Agg(2, 3) -> Sort -> Limit(16)`). Unmatched rows produced NULL keys triggering the same aggregate sort panic. |
| `204596`, `306516`, `315388` | Result mismatch against DuckDB (float sort-key order) | 11 | Fixed by 493f832 | Pass | Not an engine bug. Checker's 0.01 tolerance equalled the gap between adjacent float sort keys. Fixed in `bench/check.py` by tightening order key tolerance (`atol = 1e-12`). |

## Details and Verification

### Seeds 9061, 36132: Sort Total Order Panic
- **Summary**: `Panic: user-provided comparison function does not correctly implement a total order` in `crates/morsel-exec/src/aggregate.rs`.
- **Cause**: Hash aggregate previously attempted to sort output entries by group keys using `compare_scalars`. `compare_scalars` did not establish a valid total order when comparing floats, NaNs, or partial types, leading to a panic in `slice::sort_by`.
- **Fix**: Commit `1404b71` (`perf: remove redundant aggregate sorting`) eliminated the redundant sorting pass in hash aggregate execution.
- **Verification**: Re-run against current release build passes without errors.

### Seed 57084: Left Outer Join + Hash Aggregate Panic
- **Summary**: `Panic: user-provided comparison function does not correctly implement a total order`.
- **Shape**: `Scan(customer) -> Join(orders, Left) -> Agg(2, 3) -> Sort -> Limit(16)`.
- **Cause**: Left outer join produces NULL values for unmatched build rows. When grouped and aggregated, the grouping keys containing NULLs were subjected to the pre-`1404b71` aggregate key sort, triggering the same non-total order comparison panic.
- **Fix**: Fixed by `1404b71` which removed aggregate key sorting.
- **Verification**: Re-run against current release build passes without errors.

### Seeds 204596, 306516, 315388: Float Sort-Key Checker Tolerance
- **Summary**: Result mismatch against DuckDB on sorted float columns (e.g. `order mismatch (expected key '9069.54', got '9069.550000000')`).
- **Cause**: Checker artifact in `bench/check.py`, not an engine execution bug. The default float comparison tolerance of `0.01` matched the distance between adjacent sorted keys, causing the checker's tie-group logic to conflate adjacent keys and report ordering violations.
- **Fix**: Commit `493f832` (`fix: tighten tolerance for sort keys`) updated `bench/check.py` to use `atol = 1e-12` when comparing sort keys (`is_order_key=True`).
- **Verification**: Re-run of all seeds against current release build with updated checker passes without errors.
