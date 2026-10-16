# Morsel Execution Engine

Morsel is an in-memory, vectorized, morsel-driven analytical query execution engine written in Rust. It executes relational query plans over columnar data structures, utilizing thread parallelism to balance workloads across heterogeneous CPU cores.

---

## 1. Column and Batch Layout

### Columns
Columns in Morsel are strongly typed, contiguous, in-memory arrays represented by the `Column` struct in `morsel-core`. Each column consists of:
- `dtype: DataType`: The logical type of values stored in the column.
- `data: ColumnData`: An enum holding the physical typed buffer:
  - `Bool`: A dense `Vec<bool>`.
  - `Int32`: A contiguous `Vec<i32>`.
  - `Int64`: A contiguous `Vec<i64>`.
  - `Float64`: A contiguous `Vec<f64>`.
  - `Decimal`: A contiguous `Vec<i128>`.
  - `Date32`: A contiguous `Vec<i32>`.
  - `Utf8`: A single contiguous byte buffer (`bytes: Vec<u8>`) accompanied by an offset array (`offsets: Vec<u32>`) where slice `[offsets[i]..offsets[i+1]]` contains string $i$.
- `validity: Option<Vec<bool>>`: An optional boolean validity mask indicating non-null rows. `None` indicates the absence of null values, avoiding null-check overhead.

### Batches
Data flows between operators in chunks called `Batch`es:
- A `Batch` contains an `Arc<Schema>`, a `Vec<Arc<Column>>` of equal length, and a row count `len: usize`.
- Operators process batches of up to `BATCH_ROWS = 2048` rows. This vector size balances instruction cache locality, register reuse, and amortized function call overhead.

---

## 2. Decimals, Dates, and NULLs

- **Decimal (`Decimal { precision, scale }`)**: Decimals are represented as unscaled 128-bit signed integers (`i128`) scaled by $10^{\text{scale}}$ with up to 38 digits of precision. Arithmetic operations (addition, subtraction) align scales dynamically, and multiplications sum scales with checked arithmetic. Comparisons rescale values using exact integer arithmetic without loss of precision.
- **Date32**: Stored as a 32-bit signed integer representing the count of days since Unix epoch (`1970-01-01`). Date additions and subtractions operate natively on integer day counts.
- **NULL representation**: Values follow SQL three-valued logic (`TRUE`, `FALSE`, `UNKNOWN`/`NULL`). Comparisons or arithmetic operations involving NULL operands yield NULL. In joins, NULL keys never match other NULL keys.

---

## 3. Morsels and the Scheduler

Parallel execution in Morsel is governed by morsel-driven scheduling:
- Base tables and intermediate data are partitioned into work units called **morsels** (`Morsel`), each encompassing up to `MORSEL_ROWS = 100_000` rows.
- A lock-free shared work queue coordinates worker threads using atomic counters (`AtomicUsize`). Idle worker threads fetch the next available morsel index dynamically (`fetch_add`).
- This fine-grained dynamic scheduling prevents stragglers caused by uneven worker progress or heterogeneous CPU architectures (e.g. mixed Performance and Efficiency cores).

---

## 4. Query Flow: LogicalPlan to Batches

Queries progress through three distinct phases:
1. **Planning and Optimization (`morsel-sql`)**: SQL query text is parsed into an abstract syntax tree, bound into a typed `LogicalPlan`, and optimized via rule-based transformations (filter pushdown, projection pruning, join reordering, and decorrelation).
2. **Execution (`morsel-exec::execute`)**:
   - Pipelined streaming operators (`Scan`, `Filter`, `Project`) fuse together inside `execute_pipeline`, consuming input batches and streaming resulting output batches directly.
   - Materialization barriers (`Join` build, `Aggregate`, `Sort`) block execution until their respective input pipelines complete, then transition to probe, merge, or output generation.
3. **Result Collection**: The terminal operator emits `Vec<Batch>`, which the CLI or client formats into CSV or processes downstream.

---

## 5. Operators

### Scan
Reads requested column indices directly from catalog tables in memory. Scans partition table batches into morsels and stream rows into the execution pipeline without copying underlying data buffers.

### Filter
Evaluates boolean expressions on input batches, producing a selection vector (`Vec<usize>`). Downstream operators use this selection vector to retain qualifying rows.

### Project
Evaluates projection expressions (arithmetic, case-when, cast, string manipulation) against input batches, materializing new `Column` vectors adhering to the projected schema.

### Hash Join with Build-Side Choice
- **Build-Side Choice**: For inner joins, the engine compares total rows on the left and right inputs. If `left_rows < right_rows`, the hash table builds on the left side and probes with the right, avoiding building hash tables on large tables (e.g. 60M rows of `lineitem`).
- **Parallel Partitioned Build**:
  - **Phase 1 (Partitioning)**: Multiple worker threads evaluate join keys on build morsels in parallel, hashing keys into thread-local partition buffers across 64 partitions.
  - **Phase 2 (Table Construction)**: Worker threads build partition hash tables in parallel. Each partition stores rows in a flat `RowLoc` array and a `next` chain index, eliminating per-key vector allocations.
- **Morsel-Parallel Probe**: Worker threads concurrently probe read-only partition tables, yielding matching rows.
- **Semantics Supported**: Inner, Left Outer, Semi, and Anti joins with equi-join keys and optional residual non-equi filters.

### Hash Aggregate with Parallel Merge
- Worker threads maintain thread-local aggregation hash maps (`AHashMap<GroupKey, Vec<AggState>>`), consuming morsels independently.
- Thread-local maps are merged using a parallel pairwise tree-reduction (`std::thread::scope`), reducing merge time from sequential $O(N \cdot T)$ to parallel $O(\log_2 T)$.
- Aggregate states support `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `COUNT(DISTINCT)`.

### Sort and Top-K
- Multi-column lexicographical sort supporting ascending/descending order and nulls first/last positioning.
- When `Sort` is immediately followed by `Limit(n)`, the operator switches to a bounded binary heap (Top-K) keeping only the top $n$ elements, running in $O(M \log n)$ time without sorting the entire dataset.

### Limit
Tracks emitted row count across batches and truncates early, halting upstream morsel consumption when the threshold $n$ is reached.

---

## 6. What Is Not Supported

- **Data Modifications (DML / DDL)**: `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`. Data is loaded immutably from Parquet.
- **Transactions & Storage**: No transaction isolation, locking, write-ahead logging, indexes, or persistence.
- **Out-of-Core Processing**: No disk spilling; intermediate joins and aggregations must fit within available memory.
- **Full Outer Join & Non-Equi Joins**: Joins require at least one equality condition between sides. Full outer joins and window functions are unsupported in v1.

---

## 7. Observed Performance Characteristics

Measurements from recorded development runs (16 threads, TPC-H SF10). The machine was shared with another job
during these runs, so treat them as before/after comparisons on the same machine, not as benchmark results.
- **Parquet loading**: loading all SF10 tables takes 7,730.54 ms with parallel row-group decoding, down from
  66,017.10 ms when each file was decoded on one thread.
- **Inner join build on the smaller side**: Q9 execute time went from a timeout at 300 s to 15,818.68 ms; Q17 from
  117,023.56 ms to 27,212.44 ms.
- **Parallel partitioned hash join build** (median of 3 runs each):
  - Q3: 2,450.14 ms to 1,287.94 ms
  - Q5: 6,853.07 ms to 2,665.25 ms
  - Q9: 20,649.86 ms to 11,115.58 ms
  - Q18: 50,067.23 ms to 26,880.62 ms
  - Q21: 40,817.94 ms to 11,088.56 ms
- **End-to-end suite**: all 22 TPC-H queries pass the reference check at SF1 (26.4 s total wall time, including
  loading the data for each query) and SF10 (227.4 s).
