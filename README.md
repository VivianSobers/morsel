# morsel

Morsel is an in-memory, vectorized analytical SQL engine written in Rust. It loads tables into memory as columnar data, compiles SQL queries into logical execution plans via a rule-based optimizer, and executes them with vectorized operators scheduled as fine-grained morsels across worker threads to achieve high core utilization and low query latency.

---

## Workspace Crates

| Crate | Description |
|:---|:---|
| `crates/morsel-core` | Foundational columnar data types (`Column`, `Batch`, `Schema`, `DataType`, `Scalar`) and compute kernels. |
| `crates/morsel-plan` | Logical plan intermediate representation (`LogicalPlan`, `Expr`), operator definitions, and schema resolution. |
| `crates/morsel-exec` | In-memory table catalog, Parquet loader, vectorized expression evaluator, parallel morsel scheduler, and physical operators. |
| `crates/morsel-sql` | SQL text frontend: parsing via `sqlparser`, name binding, type coercion, subquery decorrelation, and rule-based query optimization. |
| `crates/morsel-fuzz` | Differential query plan generator, random plan execution, and drop-minimizer for fuzzing against DuckDB. |
| `crates/morsel-cli` | Command-line interface (`morsel query`) executing SQL queries on Parquet data directories with CSV and timing JSON outputs. |

---

## Current Status

All 22/22 TPC-H queries execute successfully and match reference answer sets at Scale Factor 1 (SF1) and Scale Factor 10 (SF10) (both scale factors verified at commit `f7300f8`; SF1 again at `f6010a4`).

---

## Build and Run

### Build

Compile the workspace in release mode:

```bash
cargo build --release
```

The resulting binaries are located in `target/release/` (`morsel` and `morsel-fuzz`).

### Generating Data

Generate TPC-H Parquet tables and reference CSV answer sets using `bench/gen_tpch.py` (requires DuckDB and Python dependencies from `bench/requirements.txt`):

```bash
python bench/gen_tpch.py --sf 1 --data-dir /path/to/data/sf1 --refs-dir bench/refs/sf1
```

### Running Queries

Execute a SQL query against a directory of Parquet tables using the `morsel` CLI:

```bash
morsel query --data /path/to/data/sf1 --sql-file queries/tpch/q01.sql
```

Additional options:
- `--threads T`: Number of worker threads (default: available system threads).
- `--out FILE.csv`: Write query results to a CSV file.
- `--timing-json FILE.json`: Record query execution phases and durations in JSON format.

### Explaining Plans

To inspect the optimized logical query plan without executing:

```bash
morsel query --data /path/to/data/sf1 --sql-file queries/tpch/q01.sql --explain
```

### End-to-End Verification

Run all 22 TPC-H queries and check answers against reference files using `bench/e2e.py`:

```bash
python bench/e2e.py --data /path/to/data/sf1 --refs bench/refs/sf1 --threads 16
```

---

## Documentation

- [Design Overview](docs/design.md): System architecture, execution model, and engine trade-offs.
- [Execution Engine](docs/engine.md): Columnar batch layout, morsel scheduler, operator implementations, and memory management.
- [SQL Frontend and Optimizer](docs/sql.md): Query parsing, name binding, subquery decorrelation, and rule-based plan optimizations.
- [Testing Record](docs/testing.md): Workspace test suite, reference checking rules, differential fuzzing runs, and mutation testing results.

---

## Benchmarks

All four engines ran the 22 TPC-H queries in memory on one machine (i9-13900, 125 GB RAM, no other jobs), 16
threads each, at commit `ac0cdc9`. Times exclude data loading; each value is the median of 5 timed runs after 1
warm-up, and every answer passed the reference check (176 of 176).

| Engine | SF1 total | SF1 geomean | SF10 total | SF10 geomean |
|:---|---:|---:|---:|---:|
| DuckDB 1.4.5 | 1.70 s | 64.5 ms | 10.19 s | 354.8 ms |
| DataFusion 55.0.0 | 3.40 s | 141.7 ms | 23.81 s | 876.9 ms |
| Polars 1.34.0 | 16.11 s | 334.6 ms | 172.55 s | 3.34 s |
| morsel | 21.13 s | 752.8 ms | 182.09 s | 5.62 s |

morsel is about 12x slower than DuckDB at SF1 and 18x at SF10 on total time. Even Q6, a single scan, filter and
sum, is 24x slower at SF1 (316 ms against 13 ms), which points at expression evaluation: it still goes row by row
through a generic scalar type. That is the next thing to fix; no profile has confirmed it yet. Per-query numbers, the raw JSON Lines files and
the exact command are in [bench/results/final](bench/results/final/README.md).
