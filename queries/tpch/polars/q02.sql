WITH europe_suppliers AS (
    SELECT s_suppkey
    FROM supplier
    JOIN nation ON supplier.s_nationkey = nation.n_nationkey
    JOIN region ON nation.n_regionkey = region.r_regionkey
    WHERE r_name = 'EUROPE'
),
min_costs AS (
    SELECT
        ps_partkey AS min_partkey,
        min(ps_supplycost) AS min_cost
    FROM
        partsupp
    JOIN
        europe_suppliers ON partsupp.ps_suppkey = europe_suppliers.s_suppkey
    GROUP BY
        ps_partkey
)
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part
JOIN
    partsupp ON part.p_partkey = partsupp.ps_partkey
JOIN
    supplier ON supplier.s_suppkey = partsupp.ps_suppkey
JOIN
    nation ON supplier.s_nationkey = nation.n_nationkey
JOIN
    region ON nation.n_regionkey = region.r_regionkey
JOIN
    min_costs ON part.p_partkey = min_costs.min_partkey
WHERE
    partsupp.ps_supplycost = min_costs.min_cost
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND r_name = 'EUROPE'
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
