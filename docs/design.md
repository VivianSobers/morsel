# morsel: design

morsel is an in-memory analytical SQL engine in Rust. It loads tables into RAM as columns, plans SQL into a
logical plan, and executes it with vectorized operators scheduled as small units of work ("morsels") that any idle
thread can take. The v1 finish line is all 22 TPC-H queries at scale factors 1 and 10, with every answer checked
against DuckDB's and every timing compared with DuckDB, Polars and DataFusion on the same machine.

## Goals

- Correct answers for TPC-H Q1-Q22, checked automatically against reference answers.
- Use every core: work is split into morsels of rows that threads pull from a shared queue, so fast and slow cores
  (the target machines are Intel i9-13900s, 8 performance + 16 efficiency cores) finish together.
- Reproducible benchmarks: every reported time comes from a recorded run with its full configuration (machine,
  thread count, scale factor, engine versions, commit) written next to it.

## Non-goals (v1)

- Writes, transactions, indexes, persistence. Data is loaded once from Parquet into memory.
- Full SQL. The dialect is the subset TPC-H needs (below).
- Beating DuckDB. The comparison is reported as measured, whatever it shows.

## Data

- TPC-H data is produced by DuckDB's `tpch` extension (`CALL dbgen(sf=N)`) and exported as one Parquet file per
  table: `<data_dir>/<table>.parquet` for region, nation, supplier, customer, part, partsupp, orders, lineitem.
- Reference answers come from DuckDB running the 22 standard queries (`tpch_queries()`), written as CSV. At SF1 the
  references are also checked against DuckDB's shipped official answers (`tpch_answers()`).
- Parquet is decoded with the `parquet`/`arrow` crates into morsel's own columns at load time. Arrow is used for
  I/O only; no Arrow compute kernels are used anywhere in the engine.

## Architecture

Cargo workspace, one crate per responsibility:

| Crate | Responsibility |
|:--|:--|
| `morsel-core` | Data types, scalars, columns, batches, schemas. No dependencies on other morsel crates. |
| `morsel-plan` | The logical plan and expression IR, plus `SchemaProvider`. Depends on `morsel-core`. |
| `morsel-exec` | Catalog, Parquet loader, expression evaluation, physical operators, morsel scheduler, `execute`. |
| `morsel-sql` | SQL text to `LogicalPlan`: parse (sqlparser crate), bind names and types, optimize. |
| `morsel-cli` | `morsel query` command: load data, plan SQL, execute, print CSV, report timings. |
| `bench/` | Python tooling: data generation, reference answers, answer checker, baseline runners. |

### Columns and batches

- Types: `Bool, Int32, Int64, Float64, Decimal{precision, scale}, Date32, Utf8`. Decimals are stored as `i128`
  scaled integers; `Date32` is days since 1970-01-01.
- A `Column` is typed data plus an optional validity vector (`None` means no nulls).
- A `Batch` is a set of equal-length columns with a schema. Operators exchange batches of at most
  `BATCH_ROWS = 2048` rows. A morsel is `MORSEL_ROWS = 122_880` rows of a base table.

### Logical plan

`Scan`, `Filter`, `Project`, `Aggregate`, `Join` (inner, left, semi, anti; equi keys plus optional residual
filter), `CrossJoin`, `Sort`, `Limit`. Expressions refer to input columns by index. A join's output is the left
columns followed by the right columns (semi and anti output only the left columns); the residual filter is
evaluated against left-then-right columns.

### Execution

- Plans are split into pipelines at pipeline breakers (hash-join build, aggregate, sort).
- A pipeline's source is cut into morsels; worker threads repeatedly take the next morsel, push it through the
  pipeline's operators batch by batch, and write into thread-local sinks that are merged when the pipeline ends.
- Hash join: the build side is materialized into one shared hash table, then probed in parallel by the morsels of
  the probe side.
- Aggregate: thread-local hash tables, merged at the end (partitioned merge once group counts are large).
- Sort: thread-local sorted runs, then a k-way merge; `Sort` followed by `Limit` becomes a top-k.

### SQL subset (what TPC-H needs)

SELECT with expressions and aliases; FROM with comma joins and explicit `JOIN ... ON`, including `LEFT OUTER JOIN`;
WHERE, GROUP BY, HAVING, ORDER BY (ASC/DESC), LIMIT; `CASE WHEN`, `LIKE`/`NOT LIKE`, `IN (list)`, `BETWEEN`,
`EXTRACT(YEAR FROM ...)`, `SUBSTRING(x FROM a FOR b)`, date literals and `date +/- interval 'n' day|month|year`
(constant-folded); aggregates `sum, avg, min, max, count, count(*), count(distinct)`; subqueries in FROM, scalar
subqueries, `EXISTS`, `NOT EXISTS`, `IN (subquery)`, `NOT IN (subquery)`, correlated or not; views are rewritten
as subqueries (Q15). Correlated subqueries are decorrelated into joins by the optimizer.

### Optimizer (rule-based)

Constant folding, predicate pushdown (including into joins), projection pruning, subquery decorrelation, join
ordering by estimated cardinality (greedy, smallest intermediate first; base-table row counts are known exactly).

## Verification

- Unit tests per expression and operator, written before the code.
- `bench/check.py` compares an engine CSV with a reference CSV: same columns, same rows; numbers equal within
  `abs(a - b) <= max(0.01, 1e-9 * abs(b))`; row order must match the reference wherever the reference's ORDER BY
  keys differ between consecutive rows.
- A query counts as passing only when its check passes at both SF1 and SF10.

## Benchmarks

- Engines: morsel, DuckDB, Polars (SQL context), DataFusion, each in-memory, same machine, same thread count.
- Timing excludes data loading. Each query: 1 warm-up run, then 5 timed runs; report the median and min.
- Every result row records: engine and version, morsel commit, scale factor, threads, query, run times, machine
  (CPU model, RAM), date. Results are appended to JSON Lines files; figures are drawn only from those files.

## Later (after v1)

- Two-machine execution with an exchange operator over the network, and recovery when one machine drops out
  mid-query by re-running its fragments on the survivor.
