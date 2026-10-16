WITH complaints AS (
    SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%'
)
SELECT
    p_brand,
    p_type,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp
JOIN
    part ON partsupp.ps_partkey = part.p_partkey
LEFT JOIN
    complaints ON partsupp.ps_suppkey = complaints.s_suppkey
WHERE
    complaints.s_suppkey IS NULL
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;
