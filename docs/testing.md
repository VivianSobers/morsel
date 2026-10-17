# Morsel Testing and Results

Morsel uses a multi-layered verification strategy combining workspace unit/integration tests, full-suite TPC-H end-to-end execution against reference answers, large-scale differential fuzzing against DuckDB, and mutation testing to measure test coverage depth.

---

## 1. Unit and Integration Tests

Workspace tests are invoked with:
```bash
cargo test --workspace
```

The workspace suite comprises 108 passing tests across 14 test binaries, 0 failures, and 1 ignored test (`measure_sf10_load` in `crates/morsel-exec/tests/load.rs`):

- **`morsel-cli`** (`tests/cli.rs`): 3 passed
  - Parameter validation, exit codes on unknown columns, CSV output formatting across all data types, and `--timing-json` emission.
- **`morsel-core`** (`tests/core.rs`): 5 passed
  - `Batch` creation and schema consistency, panic on mismatched column length, `Scalar` round-trips, `take` kernel index permutation with null preservation, and `Utf8` offset calculations.
- **`morsel-exec`**: 60 passed, 1 ignored
  - `tests/aggregate.rs` (9 passed): Global aggregations on empty input, multi-key group-by, distinct count aggregation, decimal exact sum, average float coercion, incompatible scalar state merging, parallel tree merge, and thread invariance.
  - `tests/eval.rs` (24 passed): Three-valued boolean logic, decimal scale alignment, decimal multiplication overflow checks, integer/float/decimal cross-type comparisons, non-zero division semantics, string LIKE pattern matching and escape rules, and substring indexing.
  - `tests/join.rs` (13 passed): Inner, left, semi, anti, and cross joins, multi-threaded probe partitioning, parallel hash table build with lock-free chain allocation, empty build side, all-null keys, duplicate keys, and mismatched key types/scales.
  - `tests/load.rs` (5 passed, 1 ignored): Parquet file catalog loading, stem-derived table names, column type mapping, negative scale error rejection, and SF1 column fidelity verification.
  - `tests/pipeline.rs` (9 passed): Morsel-driven scheduler execution, single-row batches, non-batch-multiple lengths, thread scaling (1, 3, 16, 32 threads), limit truncation, and worker panic propagation.
  - `tests/sort.rs` (5 passed): Multi-key ASC/DESC sort, bytewise UTF-8 ordering, NULLS FIRST/LAST semantics, top-k heap optimization equivalence to full sort plus limit, and thread invariance.
  - `tests/tpch_hand.rs` (4 passed): Hand-constructed physical operator plans executing TPC-H queries Q1, Q3–Q6, Q10, Q12–Q14, and Q18 against parquet data.
- **`morsel-fuzz`** (`tests/fuzz_smoke.rs`): 1 passed
  - Plan generation, operator tree construction, and differential execution smoke test.
- **`morsel-plan`** (`tests/schema.rs`): 7 passed
  - Schema projection derivation, scan column ordering, decimal multiplication scale expansion rules, division float promotion, aggregate schema ordering, and join nullability rules.
- **`morsel-sql`**: 23 passed
  - `tests/bind.rs` (8 passed): Column resolution, alias tracking, date interval folding, BETWEEN expansion, numeric type coercions in expressions, COALESCE, and CASE statements.
  - `tests/subquery.rs` (6 passed): Correlated and uncorrelated subquery decorrelation, EXISTS and IN rewrites to semi-joins, NOT EXISTS rewrites to anti-joins, and scalar subquery aggregation join rewrites.
  - `tests/tpch_plans.rs` (9 passed): End-to-end plan generation for all 22 TPC-H queries, scan filter pushdown, cross-join elimination, projection pruning in decorrelated subqueries, and build-side selection on smaller inputs.

---

## 2. TPC-H Reference Verification and End-to-End Runner

End-to-end verification evaluates compiled query execution against canonical TPC-H answer sets using `bench/check.py` and `bench/e2e.py`.

### Comparison and Tolerance Rules (`bench/check.py`)

1. **Cell Equality**:
   - `NULL` values must match `NULL` strictly.
   - Strings, booleans, and dates require exact byte/value equality.
   - Standard numeric and floating-point comparisons use relative and absolute tolerance:
     $$\text{tol} = \max(0.01, 10^{-9} \times |b|)$$
2. **Sort-Key Tolerance Rule**:
   - For order key columns (`is_order_key=True` for the first $k$ columns specified in `ORDER_KEY_COUNTS`), tolerance is tightened to:
     $$\text{atol} = 10^{-12}$$
   - This prevents adjacent floating-point sort keys with differences near $0.01$ (such as adjacent revenue totals) from being collapsed into identical keys or falsely swapped.
3. **Tie-Group Multiset Matching**:
   - Rows sharing identical order keys form a tie group. Within each tie group, rows may appear in any order. The checker groups consecutive rows with identical order keys and performs multiset matching against expected rows.

### End-to-End Run (`bench/e2e.py`)

Executed at Scale Factor 1 (SF1) using 16 execution threads against reference CSVs in `bench/refs/sf1/`:

```
Running TPC-H E2E: SF=1, Threads=16, Morsel=/dev/shm/morsel/target/release/morsel
Queries: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
------------------------------------------------------------
[q01] pass         1120.9 ms  
[q02] pass          748.9 ms  
[q03] pass          822.0 ms  
[q04] pass          874.0 ms  
[q05] pass          942.8 ms  
[q06] pass          888.2 ms  
[q07] pass         1817.1 ms  
[q08] pass         1051.4 ms  
[q09] pass         1665.5 ms  
[q10] pass         1277.3 ms  
[q11] pass          751.6 ms  
[q12] pass          799.7 ms  
[q13] pass         1882.2 ms  
[q14] pass          880.1 ms  
[q15] pass          862.3 ms  
[q16] pass          785.5 ms  
[q17] pass         1662.8 ms  
[q18] pass         3142.4 ms  
[q19] pass         1102.3 ms  
[q20] pass         1923.7 ms  
[q21] pass         2178.8 ms  
[q22] pass          747.0 ms  

| Query | Outcome | Wall (ms) | Details |
|:---|:---|---:|:---|
| q01   | pass      |    1120.9 |  |
| q02   | pass      |     748.9 |  |
| q03   | pass      |     822.0 |  |
| q04   | pass      |     874.0 |  |
| q05   | pass      |     942.8 |  |
| q06   | pass      |     888.2 |  |
| q07   | pass      |    1817.1 |  |
| q08   | pass      |    1051.4 |  |
| q09   | pass      |    1665.5 |  |
| q10   | pass      |    1277.3 |  |
| q11   | pass      |     751.6 |  |
| q12   | pass      |     799.7 |  |
| q13   | pass      |    1882.2 |  |
| q14   | pass      |     880.1 |  |
| q15   | pass      |     862.3 |  |
| q16   | pass      |     785.5 |  |
| q17   | pass      |    1662.8 |  |
| q18   | pass      |    3142.4 |  |
| q19   | pass      |    1102.3 |  |
| q20   | pass      |    1923.7 |  |
| q21   | pass      |    2178.8 |  |
| q22   | pass      |     747.0 |  |
| Total | 22/22 pass |   27926.5 | sf=1, threads=16, commit=bc90437 |
```

All 22 queries match their reference answer sets within established tolerances.

---

## 3. Differential Fuzzing

The differential fuzzer (`bench/fuzz.py`, `crates/morsel-fuzz`) generates pseudorandom valid relational plans (scans, filters, projects, joins, aggregations, sorts, limits) from deterministic integer seeds, executes them on Morsel, and compares the CSV output to DuckDB executing equivalent generated SQL.

### Long Run (`fuzz2`)

Executed against commit `5f61ef1` on 12 worker processes using SF0.1 parquet data (`/dev/shm/morsel/tpch/fuzz_sf01`):
- **Results file**: `/dev/shm/morsel/fuzz2/results.jsonl`
- **Total seeds recorded**: 519,198
- **Pass**: 519,187
- **Mismatch**: 11 (seeds 204596, 306516, 315388, 449271, 475421, 512495, 526496, 563854, 573562, 617560, 618383)
- **Morsel error**: 0
- **DuckDB error**: 0

All 11 reported mismatches were order mismatches on floating-point sort keys where the gap between adjacent keys equalled the checker's 0.01 tolerance window (for example, DuckDB emitted `9069.54` followed by `9069.55`, which the checker treated as indistinguishable under 0.01 tolerance). When evaluated using the tightened sort-key tolerance introduced in `42537d2`, all 11 seeds pass without engine changes.

### Initial Run (`f1`)

Recorded in `~/morsel_status_f1.md` against commit `bab82d1`:
- **Total seeds**: 11,000
- **Pass**: 10,999
- **Mismatch**: 0
- **Morsel error**: 1 (seed 9061)
- **DuckDB error**: 0
- **Distinct bug found**: 1 (panic in `crates/morsel-exec/src/aggregate.rs` where hash aggregate attempted to sort grouped keys using `compare_scalars`, violating total ordering on floats/nulls; fixed by commit `1404b71`).

---

## 4. Mutation Testing

Mutation testing was executed with `cargo-mutants` 27.1.0 on commit `5ad04a6` targeting three crates: `morsel-core`, `morsel-exec`, and `morsel-plan`.

### Outcome Summary (`/dev/shm/morsel/mutants/mutants.out/outcomes.json`)

From `outcomes.json`:
- **Total mutants**: 420
- **Caught**: 143
- **Missed**: 249
- **Timeout**: 2
- **Unviable**: 26
- **Baseline**: 1 entry with outcome `Success` (the unmutated build and tests passed before mutation began)

### Missed Mutants by File (`missed.txt`)

Analysis of `missed.txt` reveals the distribution of the 249 missed mutants:

| File | Missed Mutants |
|:---|---:|
| `crates/morsel-exec/src/eval.rs` | 185 |
| `crates/morsel-exec/src/aggregate.rs` | 44 |
| `crates/morsel-core/src/types.rs` | 11 |
| `crates/morsel-core/src/column.rs` | 5 |
| `crates/morsel-core/src/batch.rs` | 2 |
| `crates/morsel-exec/src/catalog.rs` | 2 |
| **Total** | **249** |

### Assessment

249 missed mutants is plainly a gap, not a pass. The concentration in `eval.rs` (185) and `aggregate.rs` (44) reflects branches and operator specializations (such as edge cases in decimal overflow detection, ternary logic fallbacks, and unused accumulator state branches) that are never exercised by the test suite despite all integration tests passing. While the engine achieves correct results across standard benchmark workloads, the unit suite requires additional dedicated tests targeting isolated expression evaluation and aggregation paths to close these coverage gaps.
