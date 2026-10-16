WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE))) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= CAST('1996-01-01' AS date)
      AND l_shipdate < CAST('1996-04-01' AS date)
    GROUP BY
        l_suppkey
),
max_revenue AS (
    SELECT max(total_revenue) AS max_rev FROM revenue
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier
JOIN
    revenue ON supplier.s_suppkey = revenue.supplier_no
JOIN
    max_revenue ON revenue.total_revenue = max_revenue.max_rev
ORDER BY
    s_suppkey;
