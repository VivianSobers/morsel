WITH step_0 AS (
    SELECT ps_partkey AS c0, ps_suppkey AS c1, ps_availqty AS c2, ps_supplycost AS c3, ps_comment AS c4 FROM partsupp
),
step_1 AS (
    SELECT s_suppkey AS c0, s_name AS c1, s_address AS c2, s_nationkey AS c3, s_phone AS c4, s_acctbal AS c5 FROM supplier
),
step_2 AS (
    SELECT step_0.c0 AS c0, step_0.c1 AS c1, step_0.c2 AS c2, step_0.c3 AS c3, step_0.c4 AS c4, step_1.c0 AS c5, step_1.c1 AS c6, step_1.c2 AS c7, step_1.c3 AS c8, step_1.c4 AS c9, step_1.c5 AS c10 FROM step_0 LEFT OUTER JOIN step_1 ON step_0.c1 = step_1.c0 AND (step_0.c0 <= step_1.c0)
),
step_3 AS (
    SELECT c8 AS c0, COUNT(DISTINCT c8) AS c1 FROM step_2 GROUP BY c8
)
SELECT c0 AS "s_nationkey", c1 AS "agg_0" FROM step_3
ORDER BY c0 ASC NULLS LAST, c1 ASC NULLS LAST
LIMIT 40