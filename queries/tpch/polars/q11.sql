WITH threshold AS (
    SELECT sum(CAST(ps_supplycost AS DOUBLE) * CAST(ps_availqty AS DOUBLE)) * 0.0001000000 AS thresh
    FROM partsupp
    JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey
    JOIN nation ON supplier.s_nationkey = nation.n_nationkey
    WHERE n_name = 'GERMANY'
),
part_values AS (
    SELECT
        ps_partkey,
        sum(CAST(ps_supplycost AS DOUBLE) * CAST(ps_availqty AS DOUBLE)) AS value
    FROM
        partsupp
    JOIN
        supplier ON partsupp.ps_suppkey = supplier.s_suppkey
    JOIN
        nation ON supplier.s_nationkey = nation.n_nationkey
    WHERE
        n_name = 'GERMANY'
    GROUP BY
        ps_partkey
)
SELECT
    ps_partkey,
    value
FROM
    part_values
CROSS JOIN
    threshold
WHERE
    value > thresh
ORDER BY
    value DESC;
