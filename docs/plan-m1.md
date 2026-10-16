# morsel M1 implementation plan

**Goal:** A working engine core and SQL frontend that run hand-built TPC-H plans correctly and plan all 22 TPC-H
queries, with reference data, reference answers and an answer checker in place.

**Architecture:** Cargo workspace (`morsel-core`, `morsel-plan`, `morsel-exec`, `morsel-sql`, later `morsel-cli`)
plus Python tooling in `bench/`. Part A builds the engine (core, plan IR, exec). Part B builds the data and
reference tooling and the SQL frontend. The two parts touch disjoint files; the shared contract is the code in
Tasks A1 and A2, which is fixed by this plan.

**Tech Stack:** Rust 1.99 stable, edition 2021; crates `parquet` + `arrow` (Parquet I/O only), `sqlparser`,
`hashbrown`, `ahash`, `crossbeam-deque`, `anyhow`, `thiserror`; Python 3.10 with `duckdb` in a project venv.

**Spec:** `docs/design.md`

## Global Constraints

- Rust stable 1.99, edition 2021, `#![forbid(unsafe_code)]` in every crate for M1.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass before every commit.
- No Arrow compute kernels, no `rayon`, no `polars`/`datafusion` crates in the engine. Arrow types appear only
  inside the Parquet loader.
- `BATCH_ROWS = 2048`, `MORSEL_ROWS = 122_880`.
- Decimal values are `i128` with the scale carried in `DataType::Decimal`. `Date32` is days since 1970-01-01.
- Division (`BinOp::Div`) always yields `Float64`.
- Number comparison in the checker: `abs(a - b) <= max(0.01, 1e-9 * abs(b))`.
- Commit messages: `<type>: <4-6 words>`, type one of feat, fix, test, docs, chore, perf, refactor; no trailers.
- Part A owns `Cargo.toml` (workspace root), `crates/morsel-core`, `crates/morsel-plan`, `crates/morsel-exec`,
  `crates/morsel-cli`. Part B owns `bench/`, `queries/`, `crates/morsel-sql`. Each part edits only its own paths;
  Part B asks for workspace membership of `morsel-sql` by adding it in its own commit to the root `Cargo.toml`
  `members` list and nothing else in that file.

## Review Focus

1. A filter that selects zero rows, or an empty table, feeding a global aggregate: one output row, `count` = 0,
   `sum`/`avg`/`min`/`max` = NULL. (Test in A6.)
2. NULLs produced by a LEFT join flowing into `count(col)` (TPC-H Q13): NULLs are not counted. (Test in A6/A7.)
3. Row counts that are not a multiple of `BATCH_ROWS`, tables smaller than one morsel, and more threads than
   morsels: same result as a single thread. (Test in A5.)
4. Decimal multiplication growing the scale (`l_extendedprice * (1 - l_discount)` is scale 4; times
   `(1 + l_tax)` is scale 6) and sums over 60M rows: no overflow, exact result. (Test in A4 and A6.)
5. `LIKE` patterns with several `%` segments and literal characters next to them (`'%special%requests%'`,
   `'forest%'`, `'%BRASS'`). (Test in A4.)

---

## Part A: engine

### Task A1: workspace and core types

**Files:**
- Create: `Cargo.toml`, `crates/morsel-core/Cargo.toml`, `crates/morsel-core/src/lib.rs`,
  `crates/morsel-core/src/types.rs`, `crates/morsel-core/src/column.rs`, `crates/morsel-core/src/batch.rs`
- Test: `crates/morsel-core/tests/core.rs`

**Interfaces (fixed contract, copy exactly; add methods freely, do not change these):**

```rust
pub const BATCH_ROWS: usize = 2048;
pub const MORSEL_ROWS: usize = 122_880;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataType { Bool, Int32, Int64, Float64, Decimal { precision: u8, scale: u8 }, Date32, Utf8 }

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar { Null, Bool(bool), Int32(i32), Int64(i64), Float64(f64), Decimal(i128, u8), Date32(i32), Utf8(String) }

#[derive(Clone, Debug, PartialEq)]
pub struct Field { pub name: String, pub dtype: DataType, pub nullable: bool }

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Schema { pub fields: Vec<Field> }

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnData {
    Bool(Vec<bool>), Int32(Vec<i32>), Int64(Vec<i64>), Float64(Vec<f64>),
    Decimal(Vec<i128>), Date32(Vec<i32>), Utf8 { offsets: Vec<u32>, bytes: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column { pub dtype: DataType, pub data: ColumnData, pub validity: Option<Vec<bool>> }

#[derive(Clone, Debug)]
pub struct Batch { pub schema: std::sync::Arc<Schema>, pub columns: Vec<std::sync::Arc<Column>>, pub len: usize }
```

Required methods: `Column::len(&self) -> usize`, `Column::is_null(&self, i: usize) -> bool`,
`Column::get(&self, i: usize) -> Scalar`, `Column::from_scalars(dtype: DataType, values: &[Scalar]) -> Column`,
`Column::take(&self, indices: &[u32]) -> Column`, `Column::utf8_value(&self, i: usize) -> &str`,
`Batch::new(schema: Arc<Schema>, columns: Vec<Arc<Column>>) -> Batch` (panics if lengths differ),
`Batch::slice(&self, offset: usize, len: usize) -> Batch`, `Schema::index_of(&self, name: &str) -> Option<usize>`.

- [ ] **Step 1: Write the failing tests** in `crates/morsel-core/tests/core.rs`:
  `scalar_round_trip` (from_scalars then get returns the same scalars for every type, including `Scalar::Null`
  which sets validity), `utf8_offsets` (`["", "ab", "c"]` gives offsets `[0, 0, 2, 3]`), `take_reorders_and_keeps_nulls`,
  `batch_slice_shares_schema` (slice(2048, 5) of a 4100-row batch has len 5 and first value = row 2048),
  `batch_new_panics_on_length_mismatch` (`#[should_panic]`).
- [ ] **Step 2: Run** `cargo test -p morsel-core` and confirm the tests fail to compile or fail.
- [ ] **Step 3: Implement** the contract above in `types.rs`, `column.rs`, `batch.rs`, re-exported from `lib.rs`.
- [ ] **Step 4: Run** `cargo test -p morsel-core`: all pass; `cargo clippy --all-targets -- -D warnings` clean.
- [ ] **Step 5: Commit** `feat: add core column types`

### Task A2: plan IR

**Files:**
- Create: `crates/morsel-plan/Cargo.toml`, `crates/morsel-plan/src/lib.rs`, `crates/morsel-plan/src/expr.rs`,
  `crates/morsel-plan/src/plan.rs`
- Test: `crates/morsel-plan/tests/schema.rs`

**Interfaces (fixed contract, copy exactly):**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp { Add, Sub, Mul, Div, Eq, NotEq, Lt, LtEq, Gt, GtEq, And, Or }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateField { Year }

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(usize),
    Literal(Scalar),
    Binary { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    Not(Box<Expr>),
    Negate(Box<Expr>),
    IsNull(Box<Expr>),
    Like { expr: Box<Expr>, pattern: String, negated: bool },
    InList { expr: Box<Expr>, list: Vec<Scalar>, negated: bool },
    Case { whens: Vec<(Expr, Expr)>, else_expr: Option<Box<Expr>> },
    Cast { expr: Box<Expr>, to: DataType },
    Extract { field: DateField, expr: Box<Expr> },
    Substring { expr: Box<Expr>, start: i64, len: i64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc { Sum, Avg, Min, Max, Count, CountStar, CountDistinct }

#[derive(Clone, Debug, PartialEq)]
pub struct AggExpr { pub func: AggFunc, pub arg: Option<Expr> }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType { Inner, Left, Semi, Anti }

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey { pub expr: Expr, pub asc: bool, pub nulls_first: bool }

#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    Scan { table: String, columns: Vec<usize>, table_schema: Schema },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, exprs: Vec<(Expr, String)> },
    Aggregate { input: Box<LogicalPlan>, group_by: Vec<(Expr, String)>, aggs: Vec<(AggExpr, String)> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, join_type: JoinType, on: Vec<(Expr, Expr)>, filter: Option<Expr> },
    CrossJoin { left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Sort { input: Box<LogicalPlan>, keys: Vec<SortKey> },
    Limit { input: Box<LogicalPlan>, n: usize },
}

pub trait SchemaProvider { fn table_schema(&self, name: &str) -> Option<Schema>; }
```

Required: `Expr::data_type(&self, input: &Schema) -> anyhow::Result<DataType>` and
`LogicalPlan::schema(&self) -> anyhow::Result<Schema>`. Typing rules: comparisons, AND/OR, NOT, LIKE, IN, IS NULL
give Bool; Add/Sub on decimals give scale max(s1, s2); Mul gives scale s1 + s2; precision is 38 for every derived
decimal; Div gives Float64; Int32 op Int64 gives Int64; integer op decimal gives decimal; Extract gives Int64;
Avg gives Float64; Count/CountStar/CountDistinct give Int64; Sum of Int32/Int64 gives Int64, Sum of Decimal gives
Decimal{38, s}; Min/Max keep the input type. Aggregate output is group columns then aggregate columns; Join output
follows `docs/design.md`; Scan output is `columns` picked from `table_schema` in that order. Output fields of a
Left join's right side become nullable.

- [ ] **Step 1: Write the failing tests** in `crates/morsel-plan/tests/schema.rs`: `decimal_mul_scale_adds`
  (Decimal{15,2} * Decimal{15,2} is Decimal{38,4}), `div_is_float`, `aggregate_schema_order`,
  `left_join_right_side_nullable`, `semi_join_outputs_left_only`, `scan_picks_columns_in_order`.
- [ ] **Step 2: Run** `cargo test -p morsel-plan`: fails.
- [ ] **Step 3: Implement** the IR and typing rules.
- [ ] **Step 4: Run** `cargo test -p morsel-plan`: passes; clippy clean.
- [ ] **Step 5: Commit** `feat: add logical plan ir`

### Task A3: catalog and Parquet loader

**Files:** Create `crates/morsel-exec/Cargo.toml`, `crates/morsel-exec/src/lib.rs`, `crates/morsel-exec/src/catalog.rs`,
`crates/morsel-exec/src/parquet_load.rs`. Test: `crates/morsel-exec/tests/load.rs`.

**Interfaces:**
- Produces: `pub struct Table { pub schema: Arc<Schema>, pub batches: Vec<Batch>, pub rows: usize }`,
  `pub struct Catalog { pub tables: HashMap<String, Table> }` implementing `SchemaProvider`,
  `pub fn load_parquet_dir(dir: &Path, threads: usize) -> anyhow::Result<Catalog>` (one table per `*.parquet`,
  table name = file stem; files load in parallel).
- Type mapping: Parquet/Arrow Int32 → Int32, Int64 → Int64, Float64 → Float64, Decimal128(p, s) → Decimal{p, s},
  Date32 → Date32, Utf8/LargeUtf8 → Utf8, Boolean → Bool; anything else is an error naming the column.

- [ ] **Step 1: Failing tests:** `loads_types_and_rows` (write a 5000-row Parquet file in a temp dir with one
  column of each type using `parquet::arrow::ArrowWriter`, load it, assert dtypes, `rows == 5000`, every batch
  `len <= BATCH_ROWS`, values at rows 0, 2047, 2048, 4999), `table_name_is_file_stem`, `unsupported_type_errors`.
- [ ] **Step 2: Run** `cargo test -p morsel-exec --test load`: fails.
- [ ] **Step 3: Implement** the loader with scoped std threads.
- [ ] **Step 4: Run** the tests: pass.
- [ ] **Step 5: Commit** `feat: load parquet into catalog`

### Task A4: expression evaluation

**Files:** Create `crates/morsel-exec/src/eval.rs`. Test: `crates/morsel-exec/tests/eval.rs`.

**Interfaces:** `pub fn eval(expr: &Expr, batch: &Batch) -> anyhow::Result<Column>`;
`pub fn eval_predicate(expr: &Expr, batch: &Batch) -> anyhow::Result<Vec<u32>>` (indices of rows where the
predicate is true; NULL counts as false).

- [ ] **Step 1: Failing tests:** `decimal_scale_rules` (1.50 * (1 - 0.06) at scales 2 and 2 equals 1.4100 at
  scale 4; times (1 + 0.08) equals 1.522800 at scale 6), `decimal_add_aligns_scales`, `div_gives_float`,
  `three_valued_and_or` (NULL AND false = false, NULL OR true = true, NULL AND true = NULL),
  `like_patterns` (`'%special%requests%'` matches "xx special yy requests zz" and not "requests special";
  `'forest%'`, `'%BRASS'`, `'PROMO%'`; `_` matches one byte), `in_list_and_negated`, `case_when_else_and_no_else`
  (no ELSE gives NULL), `extract_year` (Date32 for 1998-12-01 gives 1998), `substring_one_based`
  (`SUBSTRING('13-123' FROM 1 FOR 2)` = "13"), `cast_int_to_decimal`, `predicate_null_is_false`.
- [ ] **Step 2: Run** `cargo test -p morsel-exec --test eval`: fails.
- [ ] **Step 3: Implement** column-at-a-time evaluation; comparisons between decimals of different scale rescale
  first; dates compare as integers.
- [ ] **Step 4: Run**: pass.
- [ ] **Step 5: Commit** `feat: evaluate vectorized expressions`

### Task A5: morsel scheduler, scan, filter, project, limit

**Files:** Create `crates/morsel-exec/src/scheduler.rs`, `crates/morsel-exec/src/pipeline.rs`,
`crates/morsel-exec/src/execute.rs`. Test: `crates/morsel-exec/tests/pipeline.rs`.

**Interfaces:** `pub fn execute(plan: &LogicalPlan, catalog: &Catalog, threads: usize) -> anyhow::Result<Vec<Batch>>`
(supports every `LogicalPlan` variant by the end of A8; returns `Err` naming the variant until then).
Scheduler: a shared queue of morsels (`MORSEL_ROWS` rows of the source, cut at batch boundaries), `threads` worker
threads taking morsels until the queue is empty, per-thread sinks merged after the pipeline.

- [ ] **Step 1: Failing tests:** `same_result_for_1_3_16_threads` (filter + project over a 300_001-row table),
  `table_smaller_than_one_morsel`, `empty_table`, `more_threads_than_morsels`, `limit_returns_n_rows`,
  `rows_not_multiple_of_batch`.
- [ ] **Step 2: Run** `cargo test -p morsel-exec --test pipeline`: fails.
- [ ] **Step 3: Implement** pipelines for Scan → Filter → Project and Limit.
- [ ] **Step 4: Run**: pass.
- [ ] **Step 5: Commit** `feat: add morsel driven pipelines`

### Task A6: hash aggregate

**Files:** Create `crates/morsel-exec/src/aggregate.rs`. Test: `crates/morsel-exec/tests/aggregate.rs`.

**Interfaces:** `execute` supports `LogicalPlan::Aggregate`. Thread-local hash tables keyed on the group values,
merged after the pipeline. Global aggregate (no group keys) always returns exactly one row.

- [ ] **Step 1: Failing tests:** `global_agg_on_empty_input` (count = 0, count(*) = 0, sum/avg/min/max NULL),
  `group_by_two_keys_matches_naive` (random data, compare with a single-threaded reference computed in the test),
  `count_ignores_nulls`, `count_distinct`, `avg_is_float`, `decimal_sum_exact` (sum of 60_000_000 copies of 104949.50
  at scale 2 equals 6_296_970_000_000.00 exactly; generate in batches, do not allocate 60M values at once),
  `threads_do_not_change_result`.
- [ ] **Step 2: Run**: fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run**: pass.
- [ ] **Step 5: Commit** `feat: add parallel hash aggregate`

### Task A7: joins

**Files:** Create `crates/morsel-exec/src/join.rs`. Test: `crates/morsel-exec/tests/join.rs`.

**Interfaces:** `execute` supports `Join` (Inner, Left, Semi, Anti, with optional residual `filter`) and
`CrossJoin`. Build side = right input, materialized into one shared hash table; probe side = left input, probed
by morsels in parallel. NULL keys never match.

- [ ] **Step 1: Failing tests:** `inner_join_duplicate_build_keys`, `null_keys_never_match`,
  `left_join_fills_nulls_and_count_skips_them` (Q13 shape: left join then `count(right_col)` per left key gives 0 for
  unmatched), `semi_join_emits_left_once`, `anti_join_with_residual_filter` (Q21 shape: exists another row with the
  same key and a different value), `cross_join_with_single_row`, `join_threads_do_not_change_result`.
- [ ] **Step 2: Run**: fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run**: pass.
- [ ] **Step 5: Commit** `feat: add parallel hash joins`

### Task A8: sort and top-k

**Files:** Create `crates/morsel-exec/src/sort.rs`. Test: `crates/morsel-exec/tests/sort.rs`.

**Interfaces:** `execute` supports `Sort`, and `Limit` over `Sort` runs as a top-k.

- [ ] **Step 1: Failing tests:** `multi_key_asc_desc`, `nulls_first_and_last`, `topk_equals_sort_then_limit`
  (random data, k = 1, 10, 100, more than row count), `sort_utf8_bytewise`, `sort_threads_do_not_change_result`.
- [ ] **Step 2: Run**: fails.
- [ ] **Step 3: Implement** per-thread runs plus k-way merge.
- [ ] **Step 4: Run**: pass.
- [ ] **Step 5: Commit** `feat: add parallel sort and topk`

### Task A9: hand-built TPC-H Q1 and Q6

**Files:** Create `crates/morsel-exec/tests/tpch_hand.rs`.

Builds the plans for Q1 and Q6 directly as `LogicalPlan` values, executes them on the data in
`$MORSEL_TPCH_DIR` (skip with a printed note when unset), writes CSV and compares with
`bench/refs/sf1/q01.csv` and `q06.csv` by calling `python bench/check.py` (Part B, Task B2).

- [ ] **Step 1: Write the test** (it fails until the plans are right).
- [ ] **Step 2: Run** `MORSEL_TPCH_DIR=<sf1 dir> cargo test -p morsel-exec --release --test tpch_hand`: passes.
- [ ] **Step 3: Commit** `test: check tpch q1 and q6`

---

## Part B: data, reference answers, SQL frontend

### Task B1: data and reference answers

**Files:** Create `bench/requirements.txt` (pinned `duckdb==<version installed>`), `bench/gen_tpch.py`,
`queries/tpch/q01.sql` … `q22.sql`, `bench/refs/sf1/q01.csv` … `q22.csv`, `bench/refs/sf1/MANIFEST.json`.

`python bench/gen_tpch.py --sf 1 --data-dir DIR --refs-dir bench/refs/sf1` generates the data with
`CALL dbgen(sf=1)`, writes `DIR/<table>.parquet` for the 8 tables, writes the 22 query texts from `tpch_queries()`
to `queries/tpch/` (only if absent), runs each query and writes its answer as CSV (header row; decimals printed with
their full scale; dates as `YYYY-MM-DD`), and writes `MANIFEST.json` with the duckdb version, scale factor, row count
per table, sha256 per Parquet file and per answer file. At SF1 it also compares every answer with `tpch_answers()`
using the checker from B2 and exits non-zero on any mismatch. SF10 answers go to a data directory, not the repo.

- [ ] **Step 1: Run** it at SF1; expect 8 Parquet files, 22 query files, 22 answers, and a passing comparison with
  `tpch_answers()`.
- [ ] **Step 2: Run** it at SF10 with `--refs-dir <data dir>/refs`.
- [ ] **Step 3: Commit** `feat: generate tpch data and answers`

### Task B2: answer checker

**Files:** Create `bench/check.py`, `bench/test_check.py`.

**Interfaces:** `python bench/check.py ACTUAL.csv EXPECTED.csv --query N` exits 0 on match, 1 on mismatch with a
message naming the first differing row and column. Library function
`compare(actual_rows, expected_rows, order_key_count) -> list[str]` (empty list = match). The order rule: the number
of leading ORDER BY keys for each query is a table in `check.py` derived from the 22 query texts.

- [ ] **Step 1: Failing tests** (stdlib `unittest`): `exact_match`, `tolerance_cent` (0.005 apart passes, 0.02
  fails for values under 1e7), `tolerance_relative_large` (1e12 values 500 apart pass), `wrong_order_fails`,
  `ties_may_reorder` (rows equal on all order keys may come in any order), `column_count_mismatch_fails`,
  `null_vs_empty_string_differ`.
- [ ] **Step 2: Run** `python -m unittest bench/test_check.py`: fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run**: passes.
- [ ] **Step 5: Commit** `feat: add tpch answer checker`

### Task B3: baseline runners (SF1 dry runs only)

**Files:** Create `bench/baselines.py`.

`python bench/baselines.py --engine duckdb|polars|datafusion --sf N --data-dir DIR --threads T --out FILE.jsonl`
loads the Parquet files into memory, runs each query 1 warm-up + 5 timed runs, checks the answer with B2, and
appends one JSON line per query with: engine, engine version, sf, threads, query, times, median, min, answer check
result, CPU model, RAM, hostname, date. Engines whose package is not installed are skipped with a message.

- [ ] **Step 1: Run** for duckdb at SF1 with 4 threads; expect 22 lines with passing checks.
- [ ] **Step 2: Commit** `feat: add baseline benchmark runners`

### Task B4: SQL frontend, parse and bind

Requires `crates/morsel-core` and `crates/morsel-plan` from Part A (Tasks A1, A2) on `main`.

**Files:** Create `crates/morsel-sql/Cargo.toml`, `crates/morsel-sql/src/lib.rs`, `crates/morsel-sql/src/bind.rs`;
add `"crates/morsel-sql"` to the root `members`. Test: `crates/morsel-sql/tests/bind.rs`.

**Interfaces:** `pub fn plan_sql(sql: &str, schemas: &dyn SchemaProvider) -> anyhow::Result<LogicalPlan>`.
Parse with `sqlparser` (GenericDialect or PostgreSqlDialect, whichever parses all 22 queries). Date literals and
`date +/- interval` fold to `Scalar::Date32` at bind time. `BETWEEN` becomes two comparisons. Names resolve
case-insensitively; an unknown table or column is an error naming it.

- [ ] **Step 1: Failing tests:** `binds_q6_shape` (Aggregate over Filter over Scan of lineitem with the 4 columns
  used), `date_interval_folds` (`date '1998-12-01' - interval '90' day` = Date32 of 1998-09-02),
  `between_is_two_comparisons`, `unknown_column_errors`, `output_names_match_aliases`.
- [ ] **Step 2: Run** `cargo test -p morsel-sql`: fails.
- [ ] **Step 3: Implement** binding for SELECT/FROM/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT, joins and aliases.
- [ ] **Step 4: Run**: passes.
- [ ] **Step 5: Commit** `feat: bind sql to logical plans`

### Task B5: subqueries and decorrelation

**Files:** Create `crates/morsel-sql/src/subquery.rs`. Test: `crates/morsel-sql/tests/subquery.rs`.

EXISTS / NOT EXISTS become Semi / Anti joins with the correlated predicates as `on` keys (equalities) and `filter`
(the rest); `IN (subquery)` / `NOT IN (subquery)` become Semi / Anti joins (NOT IN over a non-null column, as in
TPC-H); uncorrelated scalar subqueries become a `CrossJoin` with a one-row plan; correlated scalar subqueries
(Q2, Q17, Q20) become an Aggregate grouped by the correlation keys joined back on those keys.

- [ ] **Step 1: Failing tests:** `exists_becomes_semi` (Q4), `not_exists_with_residual_becomes_anti` (Q21),
  `in_subquery_becomes_semi` (Q18), `scalar_uncorrelated_becomes_cross` (Q11, Q22), `correlated_scalar_becomes_agg_join` (Q17).
- [ ] **Step 2: Run**: fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run**: passes.
- [ ] **Step 5: Commit** `feat: decorrelate tpch subqueries`

### Task B6: optimizer and all 22 queries plan

**Files:** Create `crates/morsel-sql/src/optimize.rs`. Test: `crates/morsel-sql/tests/tpch_plans.rs`.

Rules in order: constant folding, predicate pushdown (through projections and into join sides), join ordering
(greedy by estimated rows, using exact base-table counts from a `TableStats` map passed in), projection pruning.
`pub fn optimize(plan: LogicalPlan, stats: &HashMap<String, usize>) -> anyhow::Result<LogicalPlan>`.

- [ ] **Step 1: Failing test:** `all_22_queries_plan` (each `queries/tpch/qNN.sql` plans and optimizes without
  error against the TPC-H schemas, and the plan's output column count equals the header width of
  `bench/refs/sf1/qNN.csv`), `no_cross_join_left_in_q5_q7_q8_q9` (comma joins become hash joins),
  `filters_pushed_to_scans_in_q3`.
- [ ] **Step 2: Run**: fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run**: passes.
- [ ] **Step 5: Commit** `feat: optimize and plan tpch queries`

---

## After M1 (next plan)

`morsel-cli` (`morsel query --data DIR --threads T --sql-file F --out CSV`), end-to-end checks of all 22 queries at
SF1 and SF10, performance work, then the benchmark runs and the profile figure.
