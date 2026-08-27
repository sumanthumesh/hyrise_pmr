SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice), AVG(l_discount)
FROM lineitem
GROUP BY l_returnflag, l_linestatus;
