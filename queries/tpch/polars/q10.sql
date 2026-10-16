SELECT
    c_custkey,
    c_name,
    sum(CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE))) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer
JOIN
    orders ON customer.c_custkey = orders.o_custkey
JOIN
    lineitem ON lineitem.l_orderkey = orders.o_orderkey
JOIN
    nation ON customer.c_nationkey = nation.n_nationkey
WHERE
    o_orderdate >= CAST('1993-10-01' AS date)
    AND o_orderdate < CAST('1994-01-01' AS date)
    AND l_returnflag = 'R'
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
