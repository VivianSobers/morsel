SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey) AS c_count
    FROM
        customer
    LEFT OUTER JOIN (
        SELECT * FROM orders WHERE o_comment NOT LIKE '%special%requests%'
    ) AS filtered_orders ON customer.c_custkey = filtered_orders.o_custkey
    GROUP BY
        c_custkey
) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
