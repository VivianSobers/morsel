WITH big_orders AS (
    SELECT l_orderkey AS big_orderkey, sum(l_quantity) AS qty
    FROM lineitem
    GROUP BY l_orderkey
    HAVING qty > 300
)
SELECT
    c_name,
    c_custkey,
    orders.o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(lineitem.l_quantity)
FROM
    orders
JOIN
    big_orders ON orders.o_orderkey = big_orders.big_orderkey
JOIN
    customer ON customer.c_custkey = orders.o_custkey
JOIN
    lineitem ON lineitem.l_orderkey = orders.o_orderkey
GROUP BY
    c_name,
    c_custkey,
    orders.o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
