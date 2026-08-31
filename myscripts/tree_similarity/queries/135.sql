-- 135.sql  derived from TPC-H Q13
-- Aggregate counts a different column (o_orderstatus for o_orderkey).
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderstatus)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;
