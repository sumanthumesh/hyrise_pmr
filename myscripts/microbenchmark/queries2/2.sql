-- A2  type axis: DECIMAL predicate column (l_tax, 0.300 GB)
-- Fixed spine: ~56% selectivity, identical 2-column aggregate.
SELECT
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_tax BETWEEN 0.00 AND 0.04;
