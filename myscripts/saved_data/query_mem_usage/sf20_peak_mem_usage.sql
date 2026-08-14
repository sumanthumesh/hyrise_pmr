setting print off
setting binary_caching on
setting workers 16
hsh mem_usage
generate_tpch 20
hsh mem_usage
hsh new_mem 64000000000 1 3
hsh mem_usage
setting workers 8
setting mem_strategy Remote
setting track_peak on
hsh mem_usage
script /data1/sumanthu/tpch_queries/1.sql
script /data1/sumanthu/tpch_queries/2.sql
script /data1/sumanthu/tpch_queries/3.sql
script /data1/sumanthu/tpch_queries/4.sql
script /data1/sumanthu/tpch_queries/5.sql
script /data1/sumanthu/tpch_queries/6.sql
script /data1/sumanthu/tpch_queries/7.sql
script /data1/sumanthu/tpch_queries/8.sql
script /data1/sumanthu/tpch_queries/9.sql
script /data1/sumanthu/tpch_queries/10.sql
script /data1/sumanthu/tpch_queries/11.sql
hsh clear_plan_caches
hsh clear_pipeline
hsh reset_exec_pools
script /data1/sumanthu/tpch_queries/12.sql
script /data1/sumanthu/tpch_queries/13.sql
script /data1/sumanthu/tpch_queries/14.sql
script /data1/sumanthu/tpch_queries/16.sql
script /data1/sumanthu/tpch_queries/17.sql
script /data1/sumanthu/tpch_queries/18.sql
script /data1/sumanthu/tpch_queries/19.sql
script /data1/sumanthu/tpch_queries/20.sql
script /data1/sumanthu/tpch_queries/21.sql
script /data1/sumanthu/tpch_queries/22.sql
hsh mem_usage
quit