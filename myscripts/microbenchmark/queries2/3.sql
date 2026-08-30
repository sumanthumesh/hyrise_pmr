-- A3 / B1  type axis: DATE predicate column (l_shipdate, 1.062 GB)
-- Also the anchor for the operator axis: filter + scalar aggregate, no
-- grouping, no join, no sort. Every B query adds exactly one operator to this.
SELECT
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_shipdate BETWEEN '1992-01-01' AND '1995-06-30';
