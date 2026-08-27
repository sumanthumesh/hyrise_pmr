-- Q4: JoinHash on integer keys (l_orderkey = o_orderkey).
-- Build side is filtered orders; probe side is full lineitem.
-- Isolates: hash-table build/probe cost with a small string-column filter.
SELECT COUNT(*)
FROM lineitem JOIN orders
  ON l_orderkey = o_orderkey
WHERE o_orderpriority = '1-URGENT';
