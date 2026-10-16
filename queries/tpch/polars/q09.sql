SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        CAST(l_extendedprice AS DOUBLE) * (1.0 - CAST(l_discount AS DOUBLE)) - CAST(ps_supplycost AS DOUBLE) * CAST(l_quantity AS DOUBLE) AS amount
    FROM
        lineitem
    JOIN
        part ON lineitem.l_partkey = part.p_partkey
    JOIN
        supplier ON lineitem.l_suppkey = supplier.s_suppkey
    JOIN
        partsupp ON lineitem.l_partkey = partsupp.ps_partkey
    JOIN
        orders ON lineitem.l_orderkey = orders.o_orderkey
    JOIN
        nation ON supplier.s_nationkey = nation.n_nationkey
    WHERE
        partsupp.ps_suppkey = lineitem.l_suppkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
