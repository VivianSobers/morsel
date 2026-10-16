WITH forest_parts AS (
    SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
),
lineitem_sum AS (
    SELECT
        l_partkey,
        l_suppkey,
        0.5 * sum(CAST(l_quantity AS DOUBLE)) AS threshold_qty
    FROM
        lineitem
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
    GROUP BY
        l_partkey,
        l_suppkey
),
valid_partsupp AS (
    SELECT DISTINCT
        ps_suppkey
    FROM
        partsupp
    JOIN
        forest_parts ON partsupp.ps_partkey = forest_parts.p_partkey
    JOIN
        lineitem_sum ON partsupp.ps_partkey = lineitem_sum.l_partkey
    WHERE
        partsupp.ps_suppkey = lineitem_sum.l_suppkey
        AND CAST(partsupp.ps_availqty AS DOUBLE) > lineitem_sum.threshold_qty
)
SELECT
    s_name,
    s_address
FROM
    supplier
JOIN
    nation ON supplier.s_nationkey = nation.n_nationkey
JOIN
    valid_partsupp ON supplier.s_suppkey = valid_partsupp.ps_suppkey
WHERE
    n_name = 'CANADA'
ORDER BY
    s_name;
