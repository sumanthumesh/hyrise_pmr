-- Q5: AggregateHash with grouping on strings, aggregating three double columns.
-- Tiny output cardinality (2 x 2 groups) but heavy input materialization.
-- Isolates: aggregate temp footprint dominated by input-column reads.
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice), AVG(l_discount)
FROM lineitem
GROUP BY l_returnflag, l_linestatus;
