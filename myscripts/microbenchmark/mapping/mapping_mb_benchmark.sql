setting print off
setting binary_caching on
setting workers 16
generate_tpch 50
script /home/umeshsum/hyrise_pmr/myscripts/move_all_cols_to_remote.sql
setting label mb_1
setting print_migration_stats off
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_partkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_partkey 0
hsh queue end
setting print_migration_stats off
setting label mb_2
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipdate 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipdate 0
hsh queue end
setting print_migration_stats off
setting label mb_3
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_shipmode 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_shipmode 0
hsh queue end
setting print_migration_stats off
setting label mb_4
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl orders o_orderkey 1
move2cxl lineitem l_orderkey 1
move2cxl orders o_orderpriority 1
setting print_migration_stats on
hsh queue start
move2cxl orders o_orderkey 0
move2cxl lineitem l_orderkey 0
move2cxl orders o_orderpriority 0
hsh queue end
setting print_migration_stats off
setting label mb_5
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_linestatus 1
move2cxl lineitem l_returnflag 1
move2cxl lineitem l_discount 1
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_quantity 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_linestatus 0
move2cxl lineitem l_returnflag 0
move2cxl lineitem l_discount 0
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_quantity 0
hsh queue end
setting print_migration_stats off
setting label mb_6
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
setting print_migration_stats off
move2cxl lineitem l_extendedprice 1
move2cxl lineitem l_orderkey 1
setting print_migration_stats on
hsh queue start
move2cxl lineitem l_extendedprice 0
move2cxl lineitem l_orderkey 0
hsh queue end
setting print_migration_stats off
quit
