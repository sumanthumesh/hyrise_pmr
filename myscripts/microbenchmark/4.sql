SELECT COUNT(*)
FROM lineitem JOIN orders
  ON l_orderkey = o_orderkey
WHERE o_orderpriority = '1-URGENT';
