-- B3  operator axis: + HIGH-CARDINALITY GROUP BY (~500K groups at SF50)
-- Same filter and aggregate as 3.sql. The hash table no longer fits in cache,
-- so this is the random-access-bound counterpart of 5.sql.
SELECT
    l_suppkey,
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_shipdate BETWEEN '1992-01-01' AND '1995-06-30'
GROUP BY
    l_suppkey;
