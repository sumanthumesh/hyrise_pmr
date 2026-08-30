-- A1  type axis: INTEGER predicate column (l_partkey, 1.796 GB)
-- Fixed spine: ~50% selectivity, identical 2-column aggregate.
SELECT
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_partkey BETWEEN 1 AND 5000000;
