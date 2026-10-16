WITH avg_bal AS (
    SELECT
        avg(c_acctbal) AS avg_b
    FROM
        customer
    WHERE
        c_acctbal > 0.00
        AND substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
)
SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM (
    SELECT
        substr(c_phone, 1, 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    CROSS JOIN
        avg_bal
    LEFT JOIN
        orders ON customer.c_custkey = orders.o_custkey
    WHERE
        orders.o_custkey IS NULL
        AND substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > avg_bal.avg_b
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
