-- B4  operator axis: + HASH JOIN against orders
-- Same lineitem filter and aggregate as 3.sql, plus a probe-side join.
-- orders is filtered to ~20% so the build side stays smaller than the probe.
SELECT
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem JOIN orders
      ON l_orderkey = o_orderkey
WHERE
    l_shipdate BETWEEN '1992-01-01' AND '1995-06-30'
    AND o_orderpriority = '1-URGENT';
