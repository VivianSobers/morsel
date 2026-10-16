WITH part_avg AS (
    SELECT
        l_partkey AS avg_partkey,
        0.2 * avg(CAST(l_quantity AS DOUBLE)) AS threshold_qty
    FROM
        lineitem
    GROUP BY
        l_partkey
)
SELECT
    sum(CAST(l_extendedprice AS DOUBLE)) / 7.0 AS avg_yearly
FROM
    lineitem
JOIN
    part ON lineitem.l_partkey = part.p_partkey
JOIN
    part_avg ON lineitem.l_partkey = part_avg.avg_partkey
WHERE
    p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND CAST(lineitem.l_quantity AS DOUBLE) < part_avg.threshold_qty;
