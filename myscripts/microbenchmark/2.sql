-- Q2: TableScan over a date column (l_shipdate).
-- Isolates: date-column scan cost under Hyrise's date encoding.
SELECT COUNT(*)
FROM lineitem
WHERE l_shipdate BETWEEN '1996-01-01' AND '1996-06-30';
