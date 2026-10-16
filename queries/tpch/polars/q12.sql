SELECT
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) AS low_line_count
FROM
    orders
JOIN
    lineitem ON orders.o_orderkey = lineitem.l_orderkey
WHERE
    l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= cast('1994-01-01' as date)
    and l_receiptdate < cast('1995-01-01' as date)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
