-- B2  operator axis: + LOW-CARDINALITY GROUP BY (4 groups)
-- Same filter and aggregate as 3.sql; adds a hash-aggregate whose table fits
-- in cache, so the added cost is per-row hashing, not memory traffic.
SELECT
    l_returnflag, l_linestatus,
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_shipdate BETWEEN '1992-01-01' AND '1995-06-30'
GROUP BY
    l_returnflag, l_linestatus;
