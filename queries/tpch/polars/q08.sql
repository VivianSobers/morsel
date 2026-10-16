SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        extract(year FROM o_orderdate) AS o_year,
        CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE)) AS volume,
        n2.n_name AS nation
    FROM
        part
    JOIN
        lineitem ON part.p_partkey = lineitem.l_partkey
    JOIN
        supplier ON supplier.s_suppkey = lineitem.l_suppkey
    JOIN
        orders ON lineitem.l_orderkey = orders.o_orderkey
    JOIN
        customer ON orders.o_custkey = customer.c_custkey
    JOIN
        nation n1 ON customer.c_nationkey = n1.n_nationkey
    JOIN
        region ON n1.n_regionkey = region.r_regionkey
    JOIN
        nation n2 ON supplier.s_nationkey = n2.n_nationkey
    WHERE
        r_name = 'AMERICA'
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
