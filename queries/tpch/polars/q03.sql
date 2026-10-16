SELECT
    l_orderkey,
    sum(CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE))) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer
JOIN
    orders ON customer.c_custkey = orders.o_custkey
JOIN
    lineitem ON lineitem.l_orderkey = orders.o_orderkey
WHERE
    c_mktsegment = 'BUILDING'
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
