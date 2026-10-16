# morsel

morsel is an in-memory analytical SQL engine written in Rust. It loads tables into memory as columnar data, compiles SQL queries into logical execution plans, and executes them with vectorized operators scheduled as fine-grained morsels that can be processed by any available worker thread. The primary goal for v1 is running all 22 TPC-H queries at scale factors 1 and 10, verifying every query result against DuckDB reference answers, and comparing execution times against DuckDB, Polars, and DataFusion on identical hardware.

## Status

The core data structures (`morsel-core`), logical plan representation (`morsel-plan`), and vectorized morsel execution engine (`morsel-exec`) are implemented. Hand-built logical plans for TPC-H queries Q1 and Q6 are verified against reference data and pass in `crates/morsel-exec/tests/tpch_hand.rs`. The SQL parser and binder (`morsel-sql`) and support for the remaining TPC-H queries are currently in progress.

## Crate Layout

| Crate / Directory | Description |
|:--|:--|
| `crates/morsel-core` | Core data types (`DataType`, `Scalar`, `Column`, `Batch`, `Schema`). Independent of other engine crates. |
| `crates/morsel-plan` | Logical plan IR (`LogicalPlan`), expression representations (`Expr`), and the `SchemaProvider` trait. |
| `crates/morsel-exec` | In-memory table catalog, Parquet loader, vectorized expression evaluation, parallel morsel scheduler, and physical operators (`execute`). |
| `crates/morsel-sql` | SQL text frontend: parsing via `sqlparser`, name binding, type inference, and rule-based query optimization. |
| `crates/morsel-cli` | Command-line interface for executing queries, loading Parquet datasets, and outputting CSV results with timing metrics. |
| `bench/` | Python utilities for TPC-H data generation (`gen_tpch.py`), answer verification (`check.py`), and baseline benchmarking (`baselines.py`). |

## Building and Testing

### Prerequisites

- Rust 1.99 or later (stable toolchain)
- Python 3.10 or later (for benchmark tooling and verification tests)

### Build

Compile the workspace:

```bash
cargo build
```

### Formatting and Linting

Check code formatting and run clippy without warnings:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

### Running Tests

Run all unit and integration tests:

```bash
cargo test
python -m unittest bench/test_check.py
```

To run the hand-built TPC-H integration tests against generated Parquet data:

```bash
MORSEL_TPCH_DIR=/path/to/tpch/parquet cargo test -p morsel-exec --test tpch_hand
```

## Generating Data and Reference Answers

TPC-H dataset generation uses DuckDB via `bench/gen_tpch.py`.

Install Python requirements:

```bash
pip install -r bench/requirements.txt
```

Generate tables and reference answers at a specified scale factor (for example, SF 1):

```bash
python bench/gen_tpch.py --sf 1 --data-dir data/sf1 --refs-dir bench/refs/sf1
```

This command:
1. Generates 8 TPC-H tables (`customer`, `lineitem`, `nation`, `orders`, `part`, `partsupp`, `region`, `supplier`) and exports each as a Parquet file to the target data directory.
2. Extracts standard SQL query files into `queries/tpch/` (`q01.sql` through `q22.sql`).
3. Executes each query against DuckDB and writes the expected answer as CSV to the references directory.
4. For SF 1, validates each generated answer against DuckDB's internal `tpch_answers()`.
5. Writes a `MANIFEST.json` containing table row counts and SHA-256 checksums for Parquet files and reference CSV files.

## Answer Checker

Query output verification is performed by `bench/check.py`:

```bash
python bench/check.py <actual.csv> <expected.csv> --query <query_number>
```

The checker operates under the following rules:
- Header validation: Verifies column count and case-insensitive header names.
- Row count validation: Ensures the output has the exact expected number of rows.
- Data comparison:
  - Empty strings and `NULL` values are treated as distinct.
  - Floating-point and decimal values are compared using absolute and relative tolerance: `abs(actual - expected) <= max(0.01, 1e-9 * abs(expected))`.
  - String values must match exactly.
- Ordering validation:
  - Uses the query's leading `ORDER BY` key count (`ORDER_KEY_COUNTS`).
  - Consecutive rows in the reference output that share the same order keys form a tie group.
  - Rows across different order keys must follow the strict sorted sequence.
  - Rows within the same tie group may appear in any order (multiset equality).
- Exit status: Exits with code `0` on match, or `1` on mismatch with the row index and column detail.

## Benchmarks

Benchmark measurements will be published only from recorded runs. No preliminary performance numbers are reported.

Every benchmark execution records:
- Engine name and version (morsel, DuckDB, Polars, DataFusion)
- Git commit hash
- Scale factor (SF1, SF10)
- Worker thread count
- Query identifier
- Individual run times across one warm-up run and five timed runs (recording min and median)
- System hardware specification (CPU model, RAM, operating system, hostname, timestamp)

All published tables and figures will be generated strictly from recorded JSON Lines log files.
