-- C1  column-size axis: the largest column in the schema
-- l_comment is 19.776 GB at SF50 -- 62.7% of all lineitem columns, and 11x
-- the widest column used anywhere else in this set (l_partkey, 1.796 GB).
--
-- Purpose is the COST side, not the benefit side. A LIKE scan reads the column
-- once, so migration has nothing to amortise against; the useful output is
-- migration_duration_ns, which measures the CRAM->DAM rate for a large
-- variable-length string column. Expect speedup < 1.
--
-- The aggregate is the same one used by 1.sql-4.sql, so this query sits on the
-- same axis as the type block and differs only in predicate column size.
--
-- CALIBRATE THE PATTERN FIRST (see calibrate_9.sql): pick one matching ~50% of
-- rows, to hold selectivity level with 1.sql-4.sql. '%the%' is a starting guess.
SELECT
    COUNT(*), SUM(l_quantity), SUM(l_extendedprice)
FROM
    lineitem
WHERE
    l_comment LIKE '%the%';
