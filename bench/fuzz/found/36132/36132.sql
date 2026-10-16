WITH step_0 AS (
    SELECT c_custkey AS c0, c_phone AS c1, c_mktsegment AS c2 FROM customer
),
step_1 AS (
    SELECT o_custkey AS c0, o_totalprice AS c1, o_orderdate AS c2 FROM orders
),
step_2 AS (
    SELECT step_0.c0 AS c0, step_0.c1 AS c1, step_0.c2 AS c2, step_1.c0 AS c3, step_1.c1 AS c4, step_1.c2 AS c5 FROM step_0 LEFT OUTER JOIN step_1 ON step_0.c0 = step_1.c0
),
step_3 AS (
    SELECT c0 AS c0, c1 AS c1, c2 AS c2, (c3 + c3) AS c3, EXTRACT(YEAR FROM c5) AS c4 FROM step_2
),
step_4 AS (
    SELECT c2 AS c0, c4 AS c1, MIN(c2) AS c2 FROM step_3 GROUP BY c2, c4
)
SELECT c0 AS "c_mktsegment", c1 AS "year_1", c2 AS "agg_0" FROM step_4
ORDER BY c0 ASC NULLS LAST, c1 ASC NULLS LAST, c2 ASC NULLS LAST
LIMIT 20