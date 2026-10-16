WITH step_0 AS (
    SELECT l_extendedprice AS c0, l_tax AS c1, l_shipdate AS c2, l_receiptdate AS c3, l_shipinstruct AS c4 FROM lineitem
),
step_1 AS (
    SELECT c0, c1, c2, c3, c4 FROM step_0 WHERE (c4 IN ('COLLECT COD', 'TAKE BACK RETURN', 'DELIVER IN PERSON'))
),
step_2 AS (
    SELECT c0 AS c0, SUM(c1) AS c1, SUM(c1) AS c2, AVG(c0) AS c3 FROM step_1 GROUP BY c0
)
SELECT c0 AS "l_extendedprice", c1 AS "agg_0", c2 AS "agg_1", c3 AS "agg_2" FROM step_2
ORDER BY c0 ASC NULLS LAST, c1 ASC NULLS LAST, c2 ASC NULLS LAST, c3 ASC NULLS LAST