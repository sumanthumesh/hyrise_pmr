setting print off
setting binary_caching on
setting workers 16
generate_tpch 100
hsh mem_usage
script /home/umeshsum/hyrise_pmr/myscripts/all_cols_to_remote.sql
hsh mem_usage
hsh new_mem 107374182400 0 2
hsh new_mem 107374182400 1 3
hsh mem_usage
setting workers 8
setting mem_strategy Greedy
setting label d0_tf0_q1
hsh set_mem_capacity 0 1000000000000
script /home/umeshsum/hyrise_pmr/myscripts/microbenchmark/mapping/map_d0_f0_q1_migrate.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/1.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/1.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/1.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/1.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/1.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
setting label d0_tf0_q2
hsh set_mem_capacity 0 1000000000000
script /home/umeshsum/hyrise_pmr/myscripts/microbenchmark/mapping/map_d0_f0_q2_migrate.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/2.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/2.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/2.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/2.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/2.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
setting label d0_tf0_q3
hsh set_mem_capacity 0 1000000000000
script /home/umeshsum/hyrise_pmr/myscripts/microbenchmark/mapping/map_d0_f0_q3_migrate.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/3.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/3.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/3.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/3.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/3.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
setting label d0_tf0_q4
hsh set_mem_capacity 0 1000000000000
script /home/umeshsum/hyrise_pmr/myscripts/microbenchmark/mapping/map_d0_f0_q4_migrate.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/4.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/4.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/4.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/4.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/4.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
setting label d0_tf0_q5
hsh set_mem_capacity 0 1000000000000
script /home/umeshsum/hyrise_pmr/myscripts/microbenchmark/mapping/map_d0_f0_q5_migrate.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/5.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/5.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/5.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/5.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/5.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
setting label d0_tf0_q6
hsh set_mem_capacity 0 1000000000000
script /home/umeshsum/hyrise_pmr/myscripts/microbenchmark/mapping/map_d0_f0_q6_migrate.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/6.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/6.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/6.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/6.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /home/umeshsum/tpch_queries/6.sql
hsh mem_usage
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
hsh mem_usage
quit
