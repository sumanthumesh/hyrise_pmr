-- A4  type axis: STRING predicate column (l_shipinstruct, 0.302 GB)
-- Fixed spine: 2 of 4 categories = ~50% selectivity, identical aggregate.
SELECT
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_shipinstruct IN ('DELIVER IN PERSON', 'COLLECT COD');
