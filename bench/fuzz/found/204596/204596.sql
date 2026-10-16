WITH step_0 AS (
    SELECT l_extendedprice AS c0, l_shipdate AS c1, l_shipinstruct AS c2, l_comment AS c3 FROM lineitem
),
step_1 AS (
    SELECT c0 AS c0, c1 AS c1, c2 AS c2, SUBSTRING(c2, 1, 5) AS c3 FROM step_0
),
step_2 AS (
    SELECT c0 AS c0, AVG(c0) AS c1, MAX(c2) AS c2, AVG(c0) AS c3 FROM step_1 GROUP BY c0
)
SELECT c0 AS "l_extendedprice", c1 AS "agg_0", c2 AS "agg_1", c3 AS "agg_2" FROM step_2
ORDER BY c0 ASC NULLS LAST, c1 ASC NULLS LAST, c2 ASC NULLS LAST, c3 ASC NULLS LAST