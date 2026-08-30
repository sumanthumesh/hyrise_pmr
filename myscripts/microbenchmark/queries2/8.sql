-- B5  operator axis: + SORT (top-N)
-- Same filter as 3.sql; replaces the aggregate with an ordered top-N so the
-- transient sort buffers, not the base columns, dominate the footprint.
SELECT
    l_orderkey, l_extendedprice
FROM
    lineitem
WHERE
    l_shipdate BETWEEN '1992-01-01' AND '1995-06-30'
ORDER BY
    l_extendedprice DESC
LIMIT 100;
