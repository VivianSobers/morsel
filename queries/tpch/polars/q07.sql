SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE)) AS volume
    FROM
        supplier
    JOIN
        lineitem ON supplier.s_suppkey = lineitem.l_suppkey
    JOIN
        orders ON lineitem.l_orderkey = orders.o_orderkey
    JOIN
        customer ON orders.o_custkey = customer.c_custkey
    JOIN
        nation n1 ON supplier.s_nationkey = n1.n_nationkey
    JOIN
        nation n2 ON customer.c_nationkey = n2.n_nationkey
    WHERE
        ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
         OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;
