# Benchmark Results

> **Note:** final: worker-2, no other jobs, threads 16, commit ac0cdc9

## Environment

- **CPU:** 13th Gen Intel(R) Core(TM) i9-13900
- **RAM:** 125.5 GB
- **Threads:** 16
- **Engine Versions:**
  - `morsel`: ac0cdc9
  - `duckdb`: 1.4.5
  - `polars`: 1.34.0
  - `datafusion`: 55.0.0

## Per-Query Median Execution Times (5 timed runs, 1 warm-up)

| Query | morsel (SF1) | duckdb (SF1) | polars (SF1) | datafusion (SF1) | morsel (SF10) | duckdb (SF10) | polars (SF10) | datafusion (SF10) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Q01** | 726.9 ms | 78.9 ms | 376.1 ms | 154.0 ms | 7.28 s | 671.2 ms | 3.52 s | 1.24 s |
| **Q02** | 701.5 ms | 41.5 ms | 438.0 ms | 109.5 ms | 14.38 s | 127.9 ms | 4.21 s | 425.3 ms |
| **Q03** | 344.8 ms | 54.9 ms | 473.2 ms | 136.7 ms | 2.58 s | 308.1 ms | 5.89 s | 775.6 ms |
| **Q04** | 516.5 ms | 63.6 ms | 162.0 ms | 75.8 ms | 4.85 s | 395.4 ms | 1.75 s | 425.2 ms |
| **Q05** | 483.9 ms | 52.9 ms | 1.45 s | 166.0 ms | 4.80 s | 324.9 ms | 16.05 s | 1.19 s |
| **Q06** | 316.3 ms | 13.2 ms | 75.7 ms | 63.7 ms | 2.58 s | 82.7 ms | 614.4 ms | 416.6 ms |
| **Q07** | 1.49 s | 57.6 ms | 1.66 s | 191.7 ms | 17.09 s | 330.3 ms | 18.88 s | 1.69 s |
| **Q08** | 1.05 s | 57.6 ms | 1.78 s | 159.2 ms | 4.08 s | 449.4 ms | 21.15 s | 1.21 s |
| **Q09** | 1.75 s | 152.6 ms | 276.1 ms | 193.0 ms | 18.60 s | 1.17 s | 2.87 s | 1.99 s |
| **Q10** | 1.30 s | 115.2 ms | 956.9 ms | 221.4 ms | 6.21 s | 663.7 ms | 11.09 s | 1.13 s |
| **Q11** | 357.7 ms | 29.3 ms | 50.3 ms | 78.9 ms | 1.93 s | 91.3 ms | 434.2 ms | 332.8 ms |
| **Q12** | 395.0 ms | 56.2 ms | 81.2 ms | 110.9 ms | 3.04 s | 383.1 ms | 637.6 ms | 689.5 ms |
| **Q13** | 1.84 s | 131.0 ms | 204.2 ms | 120.7 ms | 7.34 s | 941.7 ms | 1.98 s | 754.7 ms |
| **Q14** | 268.8 ms | 45.9 ms | 70.8 ms | 86.8 ms | 1.16 s | 167.0 ms | 586.5 ms | 415.6 ms |
| **Q15** | 342.3 ms | 26.7 ms | 53.8 ms | 112.6 ms | 2.75 s | 156.4 ms | 457.1 ms | 852.0 ms |
| **Q16** | 419.2 ms | 109.7 ms | 239.5 ms | 275.6 ms | 1.60 s | 330.8 ms | 1.84 s | 1.15 s |
| **Q17** | 1.26 s | 49.1 ms | 335.6 ms | 251.8 ms | 16.87 s | 350.1 ms | 3.91 s | 1.77 s |
| **Q18** | 2.26 s | 138.3 ms | 302.0 ms | 259.1 ms | 29.38 s | 860.9 ms | 3.52 s | 2.69 s |
| **Q19** | 1.05 s | 111.2 ms | 3.08 s | 116.0 ms | 4.61 s | 682.7 ms | 32.03 s | 712.5 ms |
| **Q20** | 1.27 s | 50.6 ms | 132.0 ms | 137.9 ms | 12.44 s | 227.6 ms | 1.50 s | 858.4 ms |
| **Q21** | 2.60 s | 199.0 ms | 3.63 s | 260.4 ms | 16.19 s | 1.24 s | 37.08 s | 2.85 s |
| **Q22** | 397.8 ms | 62.5 ms | 280.5 ms | 114.4 ms | 2.34 s | 234.8 ms | 2.54 s | 238.6 ms |
| **Total** | **21.13 s** | **1.70 s** | **16.11 s** | **3.40 s** | **182.09 s** | **10.19 s** | **172.55 s** | **23.81 s** |
| **Geomean** | **752.8 ms** | **64.5 ms** | **334.6 ms** | **141.7 ms** | **5.62 s** | **354.8 ms** | **3.34 s** | **876.9 ms** |

> Commit hashes: the history was rewritten on 17 Oct 2026 to remove a machine-specific path string from a few
> benchmark scripts. Hashes in these files were updated to the rewritten commits; the Rust sources at each commit
> are byte-identical to what was built and run (`ac0cdc9` was `e56ad31` when the benchmark ran).
