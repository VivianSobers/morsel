# Discovered Engine Bugs

| Seed | Operator / Bug Summary | Count | Minimized SQL | Morsel Result | DuckDB Result |
|:---|:---|:---|:---|:---|:---|
| `9061` | Panic: user-provided comparison function does not correctly implement a total or | 1 | `WITH step_0 AS (     SELECT ps_partkey AS c0, ps_suppkey AS c1, ps_availqty AS c2, ps_supplycost ...` | Panic: user-provided comparison function does not correctly implement a total... | Query executed successfully |
