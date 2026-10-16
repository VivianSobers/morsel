WITH order_supp_stats AS (
    SELECT
        l_orderkey,
        count(distinct l_suppkey) as total_supps,
        count(distinct case when l_receiptdate > l_commitdate then l_suppkey end) as late_supps
    FROM
        lineitem
    GROUP BY
        l_orderkey
)
SELECT
    s_name,
    count(*) AS numwait
FROM
    supplier
JOIN
    lineitem l1 ON supplier.s_suppkey = l1.l_suppkey
JOIN
    orders ON orders.o_orderkey = l1.l_orderkey
JOIN
    nation ON supplier.s_nationkey = nation.n_nationkey
JOIN
    order_supp_stats ON l1.l_orderkey = order_supp_stats.l_orderkey
WHERE
    o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND order_supp_stats.total_supps > 1
    AND order_supp_stats.late_supps = 1
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;
