-- Q6: Sort on a double column with LIMIT, materializing two columns of very
-- different widths.
-- Isolates: sort temp footprint (scales with input, not output).
SELECT l_orderkey, l_extendedprice
FROM lineitem
ORDER BY l_extendedprice DESC
LIMIT 100;
