setting print off
setting binary_caching on
setting workers 16
generate_tpch 50
script /home/umeshsum/hyrise_pmr/myscripts/move_all_cols_to_remote.sql
setting label d10737418240_tf6442450944_q10
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q12
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q13
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
setting label d10737418240_tf6442450944_q14
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl part p_brand 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl supplier s_comment 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl part p_brand 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl supplier s_comment 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q18
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q1
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q20
setting print_migration_stats off
move2cxl part p_name 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_name 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q21
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q2
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_type 1
move2cxl supplier s_acctbal 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_regionkey 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_type 0
move2cxl supplier s_acctbal 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_regionkey 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q3
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl customer c_custkey 1
move2cxl lineitem l_extendedprice 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_regionkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl customer c_custkey 0
move2cxl lineitem l_extendedprice 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_regionkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q7
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q8
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl part p_partkey 1
move2cxl orders o_orderdate 1
move2cxl part p_type 1
move2cxl region r_name 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl part p_partkey 0
move2cxl orders o_orderdate 0
move2cxl part p_type 0
move2cxl region r_name 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf6442450944_q9
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_orderkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_orderkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q10
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_comment 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_comment 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q19
setting print_migration_stats off
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q1
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q20
setting print_migration_stats off
move2cxl part p_name 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_name 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl part p_name 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_name 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q21
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q22
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
setting label d10737418240_tf7516192768_q2
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_shippriority 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_shippriority 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q7
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q8
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl customer c_custkey 1
move2cxl region r_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl region r_regionkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl customer c_custkey 0
move2cxl region r_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl region r_regionkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf7516192768_q9
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl orders o_orderdate 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl part p_name 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl orders o_orderdate 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl part p_name 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q10
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q11
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q12
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl orders o_comment 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl orders o_comment 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_brand 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_brand 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q17
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q18
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q19
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl part p_size 1
move2cxl lineitem l_shipinstruct 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl part p_size 0
move2cxl lineitem l_shipinstruct 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q20
setting print_migration_stats off
move2cxl part p_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q21
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl supplier s_name 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl supplier s_name 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q22
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
setting label d10737418240_tf8589934592_q2
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl partsupp ps_supplycost 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl partsupp ps_supplycost 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q3
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_mktsegment 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_mktsegment 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl lineitem l_discount 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl lineitem l_discount 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q6
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
setting label d10737418240_tf8589934592_q7
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q8
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl region r_regionkey 1
move2cxl orders o_orderdate 1
move2cxl part p_partkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_partkey 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl region r_regionkey 0
move2cxl orders o_orderdate 0
move2cxl part p_partkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_partkey 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf8589934592_q9
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q10
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q13
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_comment 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_comment 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q16
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q20
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl part p_name 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl part p_name 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q21
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q22
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
setting label d10737418240_tf9663676416_q2
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl supplier s_acctbal 1
move2cxl nation n_name 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl supplier s_acctbal 0
move2cxl nation n_name 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_shippriority 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_shippriority 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q5
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_custkey 1
move2cxl region r_name 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_custkey 0
move2cxl region r_name 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q7
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_extendedprice 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_extendedprice 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q8
setting print_migration_stats off
move2cxl region r_name 1
move2cxl part p_partkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_discount 1
move2cxl region r_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl part p_partkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_discount 0
move2cxl region r_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d10737418240_tf9663676416_q9
setting print_migration_stats off
move2cxl part p_name 1
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_orderkey 1
move2cxl partsupp ps_supplycost 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_name 0
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_orderkey 0
move2cxl partsupp ps_supplycost 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q10
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_custkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_custkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q13
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_comment 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_comment 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q16
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q17
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q18
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_totalprice 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_totalprice 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q19
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
move2cxl part p_size 1
move2cxl part p_container 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_brand 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
move2cxl part p_size 0
move2cxl part p_container 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_brand 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q1
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q20
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl part p_name 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl part p_name 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q21
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q22
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
setting label d13421772800_tf8053063680_q2
setting print_migration_stats off
move2cxl part p_size 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q3
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderdate 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderdate 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q5
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q6
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
setting label d13421772800_tf8053063680_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q8
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_name 1
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_custkey 1
move2cxl part p_type 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_custkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_name 0
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_custkey 0
move2cxl part p_type 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_custkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf8053063680_q9
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_comment 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_comment 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_totalprice 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_totalprice 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q19
setting print_migration_stats off
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipmode 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipmode 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_name 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_name 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q2
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl part p_type 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl part p_type 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q3
setting print_migration_stats off
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q5
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q7
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q8
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl part p_type 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl part p_type 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf9395240960_q9
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl part p_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl part p_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q10
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q11
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q13
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_comment 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_comment 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q16
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q17
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q18
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_totalprice 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_totalprice 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q19
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipinstruct 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipinstruct 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q20
setting print_migration_stats off
move2cxl partsupp ps_availqty 1
move2cxl part p_name 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_availqty 0
move2cxl part p_name 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q21
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_commitdate 1
move2cxl supplier s_name 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_commitdate 0
move2cxl supplier s_name 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q22
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
setting label d13421772800_tf10737418240_q2
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl region r_name 1
move2cxl supplier s_acctbal 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl region r_name 0
move2cxl supplier s_acctbal 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q3
setting print_migration_stats off
move2cxl customer c_mktsegment 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_shippriority 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_mktsegment 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_shippriority 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q5
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q6
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q7
setting print_migration_stats off
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_extendedprice 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_extendedprice 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q8
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl region r_name 1
move2cxl part p_type 1
move2cxl orders o_custkey 1
move2cxl part p_partkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_extendedprice 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl region r_name 0
move2cxl part p_type 0
move2cxl orders o_custkey 0
move2cxl part p_partkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_extendedprice 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf10737418240_q9
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl part p_name 1
move2cxl nation n_name 1
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_partkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl part p_name 0
move2cxl nation n_name 0
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_partkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q10
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q12
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q13
setting print_migration_stats off
move2cxl orders o_comment 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_comment 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_size 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_size 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_totalprice 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_totalprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q19
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q20
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q2
setting print_migration_stats off
move2cxl supplier s_acctbal 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_name 1
move2cxl part p_type 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_acctbal 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_name 0
move2cxl part p_type 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q3
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_shippriority 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_shippriority 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderdate 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderdate 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q7
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q8
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d13421772800_tf12079595520_q9
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl part p_name 1
move2cxl lineitem l_extendedprice 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl part p_name 0
move2cxl lineitem l_extendedprice 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q10
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q13
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
setting label d2684354560_tf1610612736_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl part p_type 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl part p_type 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q16
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q17
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q18
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q20
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q22
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q2
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl part p_size 1
move2cxl supplier s_name 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl part p_size 0
move2cxl supplier s_name 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q4
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q5
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q6
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q8
setting print_migration_stats off
move2cxl region r_name 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl part p_partkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl part p_partkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1610612736_q9
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q11
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q13
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
setting label d2684354560_tf1879048192_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q17
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q18
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q20
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q21
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q2
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl part p_partkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_size 1
move2cxl supplier s_acctbal 1
move2cxl region r_name 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl part p_partkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_size 0
move2cxl supplier s_acctbal 0
move2cxl region r_name 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q3
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_shippriority 1
move2cxl customer c_mktsegment 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_shippriority 0
move2cxl customer c_mktsegment 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q5
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl region r_regionkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl region r_regionkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q6
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
setting label d2684354560_tf1879048192_q7
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q8
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl customer c_nationkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl customer c_nationkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf1879048192_q9
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q10
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q13
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
setting label d2684354560_tf2147483648_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q18
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q19
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_container 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_container 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q1
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_linestatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_linestatus 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q20
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl part p_name 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl part p_name 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q21
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl lineitem l_commitdate 1
move2cxl supplier s_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderstatus 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl lineitem l_commitdate 0
move2cxl supplier s_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderstatus 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q22
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
setting label d2684354560_tf2147483648_q2
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_acctbal 1
move2cxl supplier s_name 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_acctbal 0
move2cxl supplier s_name 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_custkey 1
move2cxl customer c_mktsegment 1
move2cxl orders o_shippriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_custkey 0
move2cxl customer c_mktsegment 0
move2cxl orders o_shippriority 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q4
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q6
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
setting label d2684354560_tf2147483648_q7
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q8
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2147483648_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q10
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q11
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q13
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
setting label d2684354560_tf2415919104_q14
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
setting label d2684354560_tf2415919104_q16
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q17
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
setting label d2684354560_tf2415919104_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q19
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q20
setting print_migration_stats off
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl part p_name 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl part p_name 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q21
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderstatus 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderstatus 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q22
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
setting label d2684354560_tf2415919104_q2
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl part p_size 1
move2cxl supplier s_acctbal 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl part p_size 0
move2cxl supplier s_acctbal 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl customer c_mktsegment 1
move2cxl customer c_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl customer c_mktsegment 0
move2cxl customer c_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q5
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl orders o_orderkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl orders o_orderkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q6
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
setting label d2684354560_tf2415919104_q7
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q8
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d2684354560_tf2415919104_q9
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q10
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q12
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q13
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
setting label d5368709120_tf3221225472_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl part p_brand 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl part p_brand 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q17
setting print_migration_stats off
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl orders o_totalprice 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl orders o_totalprice 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q19
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipmode 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_partkey 1
move2cxl part p_size 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipmode 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_partkey 0
move2cxl part p_size 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_tax 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_tax 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q20
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q21
setting print_migration_stats off
move2cxl lineitem l_suppkey 1
move2cxl supplier s_name 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_suppkey 0
move2cxl supplier s_name 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q22
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
setting label d5368709120_tf3221225472_q2
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl nation n_regionkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_size 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl part p_partkey 1
move2cxl region r_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl nation n_regionkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_size 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl part p_partkey 0
move2cxl region r_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q3
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q4
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q5
setting print_migration_stats off
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q6
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
setting label d5368709120_tf3221225472_q7
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q8
setting print_migration_stats off
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl part p_type 1
move2cxl orders o_orderdate 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl part p_type 0
move2cxl orders o_orderdate 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3221225472_q9
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl lineitem l_discount 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl orders o_orderdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl lineitem l_discount 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl orders o_orderdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q10
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q12
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q13
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
setting label d5368709120_tf3758096384_q14
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_type 1
move2cxl part p_brand 1
move2cxl supplier s_suppkey 1
move2cxl part p_size 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_type 0
move2cxl part p_brand 0
move2cxl supplier s_suppkey 0
move2cxl part p_size 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q18
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q19
setting print_migration_stats off
move2cxl part p_size 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl part p_container 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl part p_container 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q20
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_partkey 1
move2cxl part p_name 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_partkey 0
move2cxl part p_name 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q21
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q2
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_size 1
move2cxl part p_type 1
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_size 0
move2cxl part p_type 0
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q3
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q5
setting print_migration_stats off
move2cxl nation n_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q6
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
setting label d5368709120_tf3758096384_q7
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q8
setting print_migration_stats off
move2cxl customer c_nationkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl region r_regionkey 1
move2cxl nation n_nationkey 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_orderkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_nationkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl region r_regionkey 0
move2cxl nation n_nationkey 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_orderkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf3758096384_q9
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q10
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q12
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q13
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
setting label d5368709120_tf4294967296_q14
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_partkey 1
move2cxl part p_type 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_partkey 0
move2cxl part p_type 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl part p_brand 1
move2cxl supplier s_comment 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_size 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl part p_brand 0
move2cxl supplier s_comment 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_size 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q18
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl orders o_totalprice 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl orders o_totalprice 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q19
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_quantity 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_quantity 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_name 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_name 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q21
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl orders o_orderstatus 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl orders o_orderstatus 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q2
setting print_migration_stats off
move2cxl supplier s_name 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl part p_size 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_name 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl part p_size 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q3
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl customer c_mktsegment 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl customer c_mktsegment 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q4
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q5
setting print_migration_stats off
move2cxl region r_regionkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl region r_regionkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q8
setting print_migration_stats off
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl region r_regionkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_nationkey 1
move2cxl part p_type 1
move2cxl nation n_regionkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl region r_name 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl region r_regionkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_nationkey 0
move2cxl part p_type 0
move2cxl nation n_regionkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl region r_name 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4294967296_q9
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_orderkey 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_orderkey 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_supplycost 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_supplycost 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q13
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
setting label d5368709120_tf4831838208_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl part p_container 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl part p_container 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q18
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl orders o_totalprice 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl orders o_totalprice 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q19
setting print_migration_stats off
move2cxl part p_container 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_quantity 1
move2cxl part p_size 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_container 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_quantity 0
move2cxl part p_size 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q1
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q20
setting print_migration_stats off
move2cxl part p_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q21
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderstatus 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderstatus 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q22
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
setting label d5368709120_tf4831838208_q2
setting print_migration_stats off
move2cxl part p_size 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl supplier s_acctbal 1
move2cxl partsupp ps_partkey 1
move2cxl part p_type 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl supplier s_acctbal 0
move2cxl partsupp ps_partkey 0
move2cxl part p_type 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q3
setting print_migration_stats off
move2cxl orders o_shippriority 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_shippriority 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q4
setting print_migration_stats off
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderpriority 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderpriority 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q5
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl nation n_regionkey 1
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl nation n_regionkey 0
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q7
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_suppkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_suppkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q8
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d5368709120_tf4831838208_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_discount 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_discount 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q10
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_returnflag 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_returnflag 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q11
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q12
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q13
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
setting label d8053063680_tf4831838208_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q16
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl part p_type 1
move2cxl part p_brand 1
move2cxl part p_size 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl part p_type 0
move2cxl part p_brand 0
move2cxl part p_size 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q17
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q18
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_totalprice 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_totalprice 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q19
setting print_migration_stats off
move2cxl part p_size 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl part p_brand 1
move2cxl lineitem l_shipmode 1
move2cxl part p_partkey 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl part p_brand 0
move2cxl lineitem l_shipmode 0
move2cxl part p_partkey 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q1
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q20
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_availqty 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl lineitem l_shipdate 1
move2cxl supplier s_name 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_name 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_availqty 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl lineitem l_shipdate 0
move2cxl supplier s_name 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_name 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q22
setting print_migration_stats off
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q2
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl region r_regionkey 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_name 1
move2cxl partsupp ps_partkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_size 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl region r_regionkey 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_name 0
move2cxl partsupp ps_partkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_size 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q3
setting print_migration_stats off
move2cxl orders o_shippriority 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_mktsegment 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_shippriority 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_mktsegment 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q4
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q5
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl supplier s_nationkey 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl supplier s_nationkey 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q7
setting print_migration_stats off
move2cxl supplier s_suppkey 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_suppkey 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q8
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_custkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl part p_type 1
move2cxl lineitem l_orderkey 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_custkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl part p_type 0
move2cxl lineitem l_orderkey 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf4831838208_q9
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q10
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_returnflag 1
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_returnflag 0
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q12
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q13
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
setting label d8053063680_tf5637144576_q14
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl part p_type 1
move2cxl lineitem l_partkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl part p_type 0
move2cxl lineitem l_partkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q16
setting print_migration_stats off
move2cxl part p_size 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_type 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_size 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_type 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q17
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q18
setting print_migration_stats off
move2cxl orders o_totalprice 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_totalprice 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q19
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q1
setting print_migration_stats off
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q20
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl part p_name 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl part p_partkey 1
move2cxl lineitem l_quantity 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl part p_name 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl part p_partkey 0
move2cxl lineitem l_quantity 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q21
setting print_migration_stats off
move2cxl orders o_orderstatus 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_commitdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderstatus 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_commitdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q22
setting print_migration_stats off
move2cxl customer c_phone 1
move2cxl customer c_acctbal 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_phone 0
move2cxl customer c_acctbal 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q2
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl nation n_name 1
move2cxl region r_regionkey 1
move2cxl part p_type 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_name 1
move2cxl part p_size 1
move2cxl region r_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_acctbal 1
move2cxl part p_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl nation n_name 0
move2cxl region r_regionkey 0
move2cxl part p_type 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_name 0
move2cxl part p_size 0
move2cxl region r_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_acctbal 0
move2cxl part p_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q3
setting print_migration_stats off
move2cxl customer c_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl customer c_mktsegment 1
move2cxl orders o_orderdate 1
move2cxl orders o_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl customer c_mktsegment 0
move2cxl orders o_orderdate 0
move2cxl orders o_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q5
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
move2cxl region r_name 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl nation n_regionkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_discount 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
move2cxl region r_regionkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
move2cxl region r_name 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl nation n_regionkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_discount 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
move2cxl region r_regionkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q7
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_name 1
move2cxl orders o_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_name 0
move2cxl orders o_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q8
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl region r_regionkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl lineitem l_partkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl region r_regionkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl lineitem l_partkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf5637144576_q9
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_suppkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_nationkey 1
move2cxl partsupp ps_supplycost 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_suppkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_nationkey 0
move2cxl partsupp ps_supplycost 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q11
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_availqty 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_availqty 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q12
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q13
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
setting label d8053063680_tf6442450944_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q16
setting print_migration_stats off
move2cxl partsupp ps_suppkey 1
move2cxl supplier s_suppkey 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_comment 1
move2cxl part p_size 1
move2cxl part p_type 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_suppkey 0
move2cxl supplier s_suppkey 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_comment 0
move2cxl part p_size 0
move2cxl part p_type 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q17
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl part p_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl part p_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q18
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl orders o_custkey 1
move2cxl orders o_totalprice 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl orders o_custkey 0
move2cxl orders o_totalprice 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q19
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipinstruct 1
move2cxl part p_size 1
move2cxl part p_container 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipinstruct 0
move2cxl part p_size 0
move2cxl part p_container 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q1
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_tax 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_returnflag 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_tax 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_returnflag 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q20
setting print_migration_stats off
move2cxl lineitem l_partkey 1
move2cxl supplier s_name 1
move2cxl part p_partkey 1
move2cxl part p_name 1
move2cxl lineitem l_shipdate 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_suppkey 1
move2cxl supplier s_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
move2cxl supplier s_name 0
move2cxl part p_partkey 0
move2cxl part p_name 0
move2cxl lineitem l_shipdate 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_suppkey 0
move2cxl supplier s_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q21
setting print_migration_stats off
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderstatus 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_receiptdate 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderstatus 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_receiptdate 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q22
setting print_migration_stats off
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
move2cxl customer c_custkey 1
move2cxl customer c_phone 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
move2cxl customer c_custkey 0
move2cxl customer c_phone 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q2
setting print_migration_stats off
move2cxl supplier s_nationkey 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl region r_name 1
move2cxl supplier s_acctbal 1
move2cxl nation n_nationkey 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_partkey 1
move2cxl partsupp ps_supplycost 1
move2cxl supplier s_name 1
move2cxl part p_size 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl region r_regionkey 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_nationkey 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl region r_name 0
move2cxl supplier s_acctbal 0
move2cxl nation n_nationkey 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_partkey 0
move2cxl partsupp ps_supplycost 0
move2cxl supplier s_name 0
move2cxl part p_size 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl region r_regionkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q3
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl orders o_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl orders o_shippriority 1
move2cxl lineitem l_shipdate 1
move2cxl customer c_mktsegment 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl orders o_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl orders o_shippriority 0
move2cxl lineitem l_shipdate 0
move2cxl customer c_mktsegment 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q4
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_receiptdate 1
move2cxl orders o_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_receiptdate 0
move2cxl orders o_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q5
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderdate 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl nation n_regionkey 1
move2cxl customer c_nationkey 1
move2cxl region r_name 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_nationkey 1
move2cxl region r_regionkey 1
move2cxl lineitem l_discount 1
move2cxl orders o_custkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderdate 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl nation n_regionkey 0
move2cxl customer c_nationkey 0
move2cxl region r_name 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_nationkey 0
move2cxl region r_regionkey 0
move2cxl lineitem l_discount 0
move2cxl orders o_custkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q7
setting print_migration_stats off
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_shipdate 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl supplier s_suppkey 1
move2cxl customer c_nationkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_discount 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_shipdate 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl supplier s_suppkey 0
move2cxl customer c_nationkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_discount 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q8
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl lineitem l_discount 1
move2cxl region r_regionkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl orders o_custkey 1
move2cxl nation n_regionkey 1
move2cxl orders o_orderdate 1
move2cxl region r_name 1
move2cxl part p_type 1
move2cxl supplier s_suppkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_nationkey 1
move2cxl customer c_custkey 1
move2cxl nation n_name 1
move2cxl lineitem l_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl lineitem l_discount 0
move2cxl region r_regionkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl orders o_custkey 0
move2cxl nation n_regionkey 0
move2cxl orders o_orderdate 0
move2cxl region r_name 0
move2cxl part p_type 0
move2cxl supplier s_suppkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_nationkey 0
move2cxl customer c_custkey 0
move2cxl nation n_name 0
move2cxl lineitem l_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf6442450944_q9
setting print_migration_stats off
move2cxl partsupp ps_supplycost 1
move2cxl lineitem l_quantity 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_partkey 1
move2cxl nation n_name 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_supplycost 0
move2cxl lineitem l_quantity 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_partkey 0
move2cxl nation n_name 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q10
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_discount 1
move2cxl customer c_custkey 1
move2cxl lineitem l_returnflag 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_extendedprice 1
move2cxl customer c_nationkey 1
move2cxl nation n_nationkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_discount 0
move2cxl customer c_custkey 0
move2cxl lineitem l_returnflag 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_extendedprice 0
move2cxl customer c_nationkey 0
move2cxl nation n_nationkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q11
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_availqty 1
move2cxl supplier s_nationkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl partsupp ps_suppkey 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_availqty 0
move2cxl supplier s_nationkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl partsupp ps_suppkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q12
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderpriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_receiptdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderpriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_receiptdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q13
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
setting label d8053063680_tf7247757312_q14
setting print_migration_stats off
move2cxl part p_type 1
move2cxl part p_partkey 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl part p_type 0
move2cxl part p_partkey 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q16
setting print_migration_stats off
move2cxl partsupp ps_partkey 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_type 1
move2cxl supplier s_comment 1
move2cxl supplier s_suppkey 1
move2cxl part p_size 1
move2cxl part p_brand 1
setting print_migration_stats on
hsh queue start
move2cxl partsupp ps_partkey 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_type 0
move2cxl supplier s_comment 0
move2cxl supplier s_suppkey 0
move2cxl part p_size 0
move2cxl part p_brand 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q17
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl part p_partkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_partkey 1
move2cxl part p_brand 1
move2cxl part p_container 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl part p_partkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_partkey 0
move2cxl part p_brand 0
move2cxl part p_container 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q18
setting print_migration_stats off
move2cxl orders o_totalprice 1
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl lineitem l_quantity 1
move2cxl customer c_custkey 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_totalprice 0
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl lineitem l_quantity 0
move2cxl customer c_custkey 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q19
setting print_migration_stats off
move2cxl lineitem l_discount 1
move2cxl part p_partkey 1
move2cxl part p_brand 1
move2cxl lineitem l_shipinstruct 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_shipmode 1
move2cxl lineitem l_extendedprice 1
move2cxl part p_container 1
move2cxl part p_size 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_discount 0
move2cxl part p_partkey 0
move2cxl part p_brand 0
move2cxl lineitem l_shipinstruct 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_shipmode 0
move2cxl lineitem l_extendedprice 0
move2cxl part p_container 0
move2cxl part p_size 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q1
setting print_migration_stats off
move2cxl lineitem l_tax 1
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_tax 0
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q20
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
move2cxl supplier s_nationkey 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl supplier s_name 1
move2cxl part p_partkey 1
move2cxl partsupp ps_suppkey 1
move2cxl part p_name 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_suppkey 1
move2cxl lineitem l_quantity 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_availqty 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
move2cxl supplier s_nationkey 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl supplier s_name 0
move2cxl part p_partkey 0
move2cxl partsupp ps_suppkey 0
move2cxl part p_name 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_suppkey 0
move2cxl lineitem l_quantity 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_availqty 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q21
setting print_migration_stats off
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderstatus 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_commitdate 1
move2cxl nation n_nationkey 1
move2cxl nation n_name 1
move2cxl orders o_orderkey 1
move2cxl supplier s_name 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderstatus 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_commitdate 0
move2cxl nation n_nationkey 0
move2cxl nation n_name 0
move2cxl orders o_orderkey 0
move2cxl supplier s_name 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q22
setting print_migration_stats off
move2cxl customer c_phone 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl customer c_acctbal 1
setting print_migration_stats on
hsh queue start
move2cxl customer c_phone 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl customer c_acctbal 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q2
setting print_migration_stats off
move2cxl supplier s_acctbal 1
move2cxl region r_name 1
move2cxl part p_type 1
move2cxl partsupp ps_supplycost 1
move2cxl region r_regionkey 1
move2cxl part p_size 1
move2cxl part p_partkey 1
move2cxl partsupp ps_partkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_regionkey 1
move2cxl partsupp ps_suppkey 1
move2cxl nation n_nationkey 1
move2cxl supplier s_name 1
move2cxl nation n_name 1
setting print_migration_stats on
hsh queue start
move2cxl supplier s_acctbal 0
move2cxl region r_name 0
move2cxl part p_type 0
move2cxl partsupp ps_supplycost 0
move2cxl region r_regionkey 0
move2cxl part p_size 0
move2cxl part p_partkey 0
move2cxl partsupp ps_partkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_regionkey 0
move2cxl partsupp ps_suppkey 0
move2cxl nation n_nationkey 0
move2cxl supplier s_name 0
move2cxl nation n_name 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q3
setting print_migration_stats off
move2cxl orders o_shippriority 1
move2cxl lineitem l_orderkey 1
move2cxl lineitem l_shipdate 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl customer c_mktsegment 1
move2cxl orders o_custkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_shippriority 0
move2cxl lineitem l_orderkey 0
move2cxl lineitem l_shipdate 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl customer c_mktsegment 0
move2cxl orders o_custkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q4
setting print_migration_stats off
move2cxl orders o_orderpriority 1
move2cxl lineitem l_receiptdate 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_commitdate 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderpriority 0
move2cxl lineitem l_receiptdate 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_commitdate 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q5
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl orders o_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl supplier s_nationkey 1
move2cxl nation n_regionkey 1
move2cxl customer c_nationkey 1
move2cxl nation n_name 1
move2cxl region r_name 1
move2cxl region r_regionkey 1
move2cxl supplier s_suppkey 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_orderkey 1
move2cxl customer c_custkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl orders o_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl supplier s_nationkey 0
move2cxl nation n_regionkey 0
move2cxl customer c_nationkey 0
move2cxl nation n_name 0
move2cxl region r_name 0
move2cxl region r_regionkey 0
move2cxl supplier s_suppkey 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_orderkey 0
move2cxl customer c_custkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q6
setting print_migration_stats off
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_shipdate 1
move2cxl lineitem l_extendedprice 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_shipdate 0
move2cxl lineitem l_extendedprice 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q7
setting print_migration_stats off
move2cxl nation n_nationkey 1
move2cxl customer c_custkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_custkey 1
move2cxl nation n_name 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl supplier s_nationkey 1
move2cxl orders o_orderkey 1
move2cxl customer c_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl supplier s_suppkey 1
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl nation n_nationkey 0
move2cxl customer c_custkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_custkey 0
move2cxl nation n_name 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl supplier s_nationkey 0
move2cxl orders o_orderkey 0
move2cxl customer c_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl supplier s_suppkey 0
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q8
setting print_migration_stats off
move2cxl lineitem l_orderkey 1
move2cxl customer c_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl region r_regionkey 1
move2cxl nation n_regionkey 1
move2cxl part p_type 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_suppkey 1
move2cxl customer c_custkey 1
move2cxl orders o_custkey 1
move2cxl orders o_orderdate 1
move2cxl orders o_orderkey 1
move2cxl lineitem l_discount 1
move2cxl region r_name 1
move2cxl nation n_name 1
move2cxl part p_partkey 1
move2cxl supplier s_nationkey 1
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_orderkey 0
move2cxl customer c_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl region r_regionkey 0
move2cxl nation n_regionkey 0
move2cxl part p_type 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_suppkey 0
move2cxl customer c_custkey 0
move2cxl orders o_custkey 0
move2cxl orders o_orderdate 0
move2cxl orders o_orderkey 0
move2cxl lineitem l_discount 0
move2cxl region r_name 0
move2cxl nation n_name 0
move2cxl part p_partkey 0
move2cxl supplier s_nationkey 0
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label d8053063680_tf7247757312_q9
setting print_migration_stats off
move2cxl part p_partkey 1
move2cxl lineitem l_partkey 1
move2cxl lineitem l_suppkey 1
move2cxl orders o_orderkey 1
move2cxl supplier s_nationkey 1
move2cxl supplier s_suppkey 1
move2cxl partsupp ps_supplycost 1
move2cxl nation n_nationkey 1
move2cxl lineitem l_orderkey 1
move2cxl nation n_name 1
move2cxl partsupp ps_partkey 1
move2cxl lineitem l_quantity 1
move2cxl lineitem l_discount 1
move2cxl part p_name 1
move2cxl partsupp ps_suppkey 1
move2cxl orders o_orderdate 1
setting print_migration_stats on
hsh queue start
move2cxl part p_partkey 0
move2cxl lineitem l_partkey 0
move2cxl lineitem l_suppkey 0
move2cxl orders o_orderkey 0
move2cxl supplier s_nationkey 0
move2cxl supplier s_suppkey 0
move2cxl partsupp ps_supplycost 0
move2cxl nation n_nationkey 0
move2cxl lineitem l_orderkey 0
move2cxl nation n_name 0
move2cxl partsupp ps_partkey 0
move2cxl lineitem l_quantity 0
move2cxl lineitem l_discount 0
move2cxl part p_name 0
move2cxl partsupp ps_suppkey 0
move2cxl orders o_orderdate 0
hsh queue end
setting print_migration_stats off
quit
