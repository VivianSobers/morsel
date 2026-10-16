WITH step_0 AS (
    SELECT c_custkey AS c0, c_name AS c1, c_nationkey AS c2, c_phone AS c3 FROM customer
),
step_1 AS (
    SELECT o_custkey AS c0, o_orderdate AS c1, o_orderpriority AS c2, o_comment AS c3 FROM orders
),
step_2 AS (
    SELECT step_0.c0 AS c0, step_0.c1 AS c1, step_0.c2 AS c2, step_0.c3 AS c3, step_1.c0 AS c4, step_1.c1 AS c5, step_1.c2 AS c6, step_1.c3 AS c7 FROM step_0 LEFT OUTER JOIN step_1 ON step_0.c0 = step_1.c0
),
step_3 AS (
    SELECT c2 AS c0, c6 AS c1, MAX(c5) AS c2, MAX(c1) AS c3, MAX(c3) AS c4 FROM step_2 GROUP BY c2, c6
)
SELECT c0 AS "c_nationkey", c1 AS "o_orderpriority", c2 AS "agg_0", c3 AS "agg_1", c4 AS "agg_2" FROM step_3
ORDER BY c0 ASC NULLS LAST, c1 ASC NULLS LAST, c2 ASC NULLS LAST, c3 ASC NULLS LAST, c4 ASC NULLS LAST
LIMIT 16