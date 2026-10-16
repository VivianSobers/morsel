WITH step_0 AS (
    SELECT l_extendedprice AS c0, l_tax AS c1, l_shipinstruct AS c2, l_shipmode AS c3 FROM lineitem
),
step_1 AS (
    SELECT c0, c1, c2, c3 FROM step_0 WHERE ((c1 >= CAST('0.00' AS DECIMAL(38, 2))) AND (c1 <= CAST('0.02' AS DECIMAL(38, 2))))
),
step_2 AS (
    SELECT c0 AS c0, AVG(c0) AS c1 FROM step_1 GROUP BY c0
)
SELECT c0 AS "l_extendedprice", c1 AS "agg_0" FROM step_2
ORDER BY c0 ASC NULLS LAST, c1 ASC NULLS LAST