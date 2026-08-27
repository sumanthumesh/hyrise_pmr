-- Q3: TableScan over a dictionary-encoded string column (l_shipmode).
-- Isolates: string dictionary lookup cost.
SELECT COUNT(*)
FROM lineitem
WHERE l_shipmode = 'AIR';
