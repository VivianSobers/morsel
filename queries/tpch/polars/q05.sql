SELECT
    n_name,
    sum(CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE))) AS revenue
FROM
    customer
JOIN
    orders ON customer.c_custkey = orders.o_custkey
JOIN
    lineitem ON lineitem.l_orderkey = orders.o_orderkey
JOIN
    supplier ON lineitem.l_suppkey = supplier.s_suppkey
JOIN
    nation ON supplier.s_nationkey = nation.n_nationkey
JOIN
    region ON nation.n_regionkey = region.r_regionkey
WHERE
    customer.c_nationkey = supplier.s_nationkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;
