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

All 22/22 TPC-H queries execute successfully and match reference answer sets at Scale Factor 1 (SF1) and Scale Factor 10 (SF10) (verified at commit `b4532bd` and verified at commit `118f9f3`).

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

Benchmark results against DuckDB, Polars and DataFusion will be added from recorded runs.
