setting print off
setting binary_caching on
setting workers 16
generate_tpch 50
script /data1/sumanthu/hyrise_pmr/myscripts/move_all_cols_to_remote.sql
setting label d10737418240_tf1073741824_q10
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q16
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q19
setting print_migration_stats off
move2cxl part p_size 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q1
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q21
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q2
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_size 1
move2cxl region r_name 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
move2cxl supplier s_name 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_size 0
move2cxl region r_name 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
move2cxl supplier s_name 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q3
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_mktsegment 1
move2cxl orders o_shippriority 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_mktsegment 0
move2cxl orders o_shippriority 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q5
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_regionkey 1
move2cxl nation n_regionkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_regionkey 0
move2cxl nation n_regionkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q7
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q8
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl part p_type 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl part p_type 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf1073741824_q9
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q10
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl part p_brand 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl part p_brand 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl part p_container 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl part p_container 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q19
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q20
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
move2cxl part p_name 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
move2cxl part p_name 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q21
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q2
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_supplycost 1
move2cxl region r_name 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_supplycost 0
move2cxl region r_name 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q3
setting print_migration_stats off
move2cxl orders o_shippriority 1
move2cxl customer c_mktsegment 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_shippriority 0
move2cxl customer c_mktsegment 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q4
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q7
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q8
setting print_migration_stats off
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf2147483648_q9
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q10
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q12
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q14
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q19
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl part p_size 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl part p_size 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q20
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q21
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q22
setting print_migration_stats off
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl supplier s_acctbal 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl region r_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl supplier s_acctbal 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl region r_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q3
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_shippriority 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_shippriority 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q5
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q7
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q8
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl nation n_regionkey 1
move2cxl orders o_custkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl part p_type 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl nation n_regionkey 0
move2cxl orders o_custkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl part p_type 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf3221225472_q9
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl lineitem l_discount 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_partkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl lineitem l_discount 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_partkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q10
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl part p_type 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl part p_type 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl orders o_totalprice 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl orders o_totalprice 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
move2cxl part p_size 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipmode 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
move2cxl part p_size 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipmode 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q20
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl part p_name 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl part p_name 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q21
setting print_migration_stats off
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_size 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_size 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q3
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl customer c_mktsegment 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_shippriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl customer c_mktsegment 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_shippriority 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q5
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl region r_name 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl nation n_regionkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl region r_name 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl nation n_regionkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q7
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q8
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl part p_type 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl part p_type 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf4294967296_q9
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q10
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q12
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl part p_size 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl part p_size 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q17
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q18
setting print_migration_stats off
move2cxl orders o_totalprice 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_totalprice 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q19
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_size 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_size 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q20
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_quantity 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl part p_name 1
move2cxl supplier s_name 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_quantity 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl part p_name 0
move2cxl supplier s_name 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl nation n_name 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl nation n_name 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl region r_name 1
move2cxl supplier s_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl region r_name 0
move2cxl supplier s_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_mktsegment 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_mktsegment 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl nation n_regionkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl nation n_regionkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q7
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q8
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf5368709120_q9
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q10
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q11
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q19
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q1
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q21
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_orderstatus 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_orderstatus 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q2
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl supplier s_acctbal 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl supplier s_name 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl supplier s_acctbal 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl supplier s_name 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl customer c_mktsegment 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl customer c_mktsegment 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q5
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q8
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf1342177280_q9
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q10
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q11
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q18
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q19
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q1
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q20
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_quantity 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl part p_name 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_quantity 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl part p_name 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q21
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q2
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_name 1
move2cxl supplier s_acctbal 1
move2cxl nation n_regionkey 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_name 0
move2cxl supplier s_acctbal 0
move2cxl nation n_regionkey 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q3
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_mktsegment 1
move2cxl orders o_custkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_mktsegment 0
move2cxl orders o_custkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q4
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q7
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q8
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf2684354560_q9
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q14
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q16
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl supplier s_comment 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl supplier s_comment 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q18
setting print_migration_stats off
move2cxl orders o_totalprice 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_totalprice 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q20
setting print_migration_stats off
move2cxl part p_name 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_name 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q21
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl part p_type 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl part p_type 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q3
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q8
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf4026531840_q9
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q11
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q16
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q18
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_custkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_custkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q19
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl part p_size 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl part p_size 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q20
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_name 1
move2cxl part p_name 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_name 0
move2cxl part p_name 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q21
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q2
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q3
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q5
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q7
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q8
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl orders o_custkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl part p_partkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl orders o_custkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl part p_partkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf5368709120_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q12
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q14
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q18
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl orders o_totalprice 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl orders o_totalprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q19
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl lineitem l_discount 1
move2cxl part p_container 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl lineitem l_discount 0
move2cxl part p_container 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q1
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q20
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_availqty 1
move2cxl part p_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_availqty 0
move2cxl part p_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q21
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q2
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl nation n_regionkey 1
move2cxl part p_size 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_acctbal 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl nation n_regionkey 0
move2cxl part p_size 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_acctbal 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderdate 1
move2cxl customer c_mktsegment 1
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderdate 0
move2cxl customer c_mktsegment 0
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q4
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q5
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_extendedprice 1
move2cxl region r_regionkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl region r_name 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_extendedprice 0
move2cxl region r_regionkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl region r_name 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q7
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q8
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl part p_partkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl part p_partkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf6710886400_q9
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q10
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q11
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q16
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q19
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl part p_container 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl part p_container 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q1
setting print_migration_stats off
setting print_migration_stats on
hsh queue start
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q20
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q21
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q3
setting print_migration_stats off
move2cxl customer c_mktsegment 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_mktsegment 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q6
setting print_migration_stats off
setting print_migration_stats on
hsh queue start
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q7
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q8
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf268435456_q9
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q10
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q11
setting print_migration_stats off
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_type 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_type 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q19
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q20
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q21
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q2
setting print_migration_stats off
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q3
setting print_migration_stats off
move2cxl customer c_mktsegment 1
move2cxl orders o_orderdate 1
move2cxl orders o_shippriority 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_mktsegment 0
move2cxl orders o_orderdate 0
move2cxl orders o_shippriority 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q5
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q7
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q8
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl region r_name 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl orders o_orderdate 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl region r_name 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl orders o_orderdate 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf536870912_q9
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q10
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q12
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q19
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
move2cxl part p_partkey 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
move2cxl part p_partkey 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q20
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q21
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_name 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_name 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q22
setting print_migration_stats off
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q2
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_name 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_name 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q3
setting print_migration_stats off
move2cxl customer c_mktsegment 1
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_mktsegment 0
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q8
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl customer c_custkey 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl customer c_custkey 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf805306368_q9
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q10
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q19
setting print_migration_stats off
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q20
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q21
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q2
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_name 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_name 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_mktsegment 1
move2cxl orders o_shippriority 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_mktsegment 0
move2cxl orders o_shippriority 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q5
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q7
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q8
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl part p_type 1
move2cxl orders o_orderkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl part p_type 0
move2cxl orders o_orderkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1073741824_q9
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q10
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q19
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_discount 1
move2cxl part p_size 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_discount 0
move2cxl part p_size 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q21
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl orders o_orderstatus 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl orders o_orderstatus 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q2
setting print_migration_stats off
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_shippriority 1
move2cxl customer c_mktsegment 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_shippriority 0
move2cxl customer c_mktsegment 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q5
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q7
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q8
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl customer c_nationkey 1
move2cxl part p_type 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl customer c_nationkey 0
move2cxl part p_type 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1342177280_q9
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q10
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q16
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q20
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q21
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q2
setting print_migration_stats off
move2cxl region r_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_acctbal 1
move2cxl region r_regionkey 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_acctbal 0
move2cxl region r_regionkey 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q3
setting print_migration_stats off
move2cxl customer c_mktsegment 1
move2cxl orders o_shippriority 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_mktsegment 0
move2cxl orders o_shippriority 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q5
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q7
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q8
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl region r_name 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl region r_name 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf536870912_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q10
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q19
setting print_migration_stats off
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q20
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q21
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q2
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_acctbal 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl region r_regionkey 1
move2cxl part p_size 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_acctbal 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl region r_regionkey 0
move2cxl part p_size 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q3
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_shippriority 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_shippriority 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q4
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q5
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q7
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q8
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1073741824_q9
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q10
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q11
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q16
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q1
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_tax 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_tax 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q20
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_availqty 1
move2cxl part p_partkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_availqty 0
move2cxl part p_partkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q21
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q22
setting print_migration_stats off
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q2
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl supplier s_acctbal 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl supplier s_acctbal 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q3
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q4
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl customer c_custkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl customer c_custkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q7
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q8
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf1610612736_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q10
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q18
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q19
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_discount 1
move2cxl part p_size 1
move2cxl part p_container 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_discount 0
move2cxl part p_size 0
move2cxl part p_container 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q1
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q20
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl part p_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl part p_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl region r_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl supplier s_acctbal 1
move2cxl part p_size 1
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl region r_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl supplier s_acctbal 0
move2cxl part p_size 0
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl customer c_mktsegment 1
move2cxl orders o_shippriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl customer c_mktsegment 0
move2cxl orders o_shippriority 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q5
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q7
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q8
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl customer c_custkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl region r_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl customer c_custkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl region r_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2147483648_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q10
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q19
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q1
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q20
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl part p_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl part p_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q21
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q2
setting print_migration_stats off
move2cxl supplier s_acctbal 1
move2cxl part p_type 1
move2cxl region r_regionkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_size 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl part p_partkey 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_acctbal 0
move2cxl part p_type 0
move2cxl region r_regionkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_size 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl part p_partkey 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q3
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q5
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q7
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q8
setting print_migration_stats off
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_orderdate 1
move2cxl part p_partkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_orderdate 0
move2cxl part p_partkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf2684354560_q9
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q10
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q16
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q19
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q20
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_name 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_name 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q21
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q2
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl part p_size 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl part p_size 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q3
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl customer c_mktsegment 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl customer c_mktsegment 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q5
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q7
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q8
setting print_migration_stats off
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
move2cxl orders o_custkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl customer c_nationkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
move2cxl orders o_custkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl customer c_nationkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf805306368_q9
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q10
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl supplier s_comment 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl supplier s_comment 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q19
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipmode 1
move2cxl part p_container 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipmode 0
move2cxl part p_container 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q21
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q2
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl supplier s_acctbal 1
move2cxl part p_type 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl supplier s_acctbal 0
move2cxl part p_type 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q3
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q5
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl region r_regionkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl region r_regionkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q8
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf1610612736_q9
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q10
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q12
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q19
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q1
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_name 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_name 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q21
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q2
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q3
setting print_migration_stats off
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q7
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q8
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl customer c_custkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl part p_type 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl customer c_custkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl part p_type 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf2415919104_q9
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q10
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
move2cxl orders o_totalprice 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
move2cxl orders o_totalprice 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_size 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl part p_brand 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_size 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl part p_brand 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q20
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q21
setting print_migration_stats off
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q2
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl part p_size 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_regionkey 1
move2cxl part p_partkey 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl part p_size 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_regionkey 0
move2cxl part p_partkey 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q3
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q4
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q5
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q7
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q8
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl orders o_orderkey 1
move2cxl region r_name 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl orders o_orderdate 1
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl orders o_orderkey 0
move2cxl region r_name 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl orders o_orderdate 0
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf3221225472_q9
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q11
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q16
setting print_migration_stats off
move2cxl supplier s_comment 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_comment 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_partkey 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_partkey 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q18
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_totalprice 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_totalprice 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q19
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q20
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_name 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_name 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q21
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q2
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl supplier s_acctbal 1
move2cxl region r_regionkey 1
move2cxl part p_size 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl region r_name 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl supplier s_acctbal 0
move2cxl region r_regionkey 0
move2cxl part p_size 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl region r_name 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_discount 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_discount 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q7
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q8
setting print_migration_stats off
move2cxl region r_name 1
move2cxl orders o_orderkey 1
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl orders o_orderkey 0
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4026531840_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
quit
