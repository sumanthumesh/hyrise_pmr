-- Q1: TableScan over an integer column (l_partkey).
-- Isolates: int-column scan cost. Near-zero temp; count-only aggregate.
SELECT COUNT(*)
FROM lineitem
WHERE l_partkey BETWEEN 1000 AND 100000;
