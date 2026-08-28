import os
import sys
import argparse

HYRISE_ROOT = "/home/umeshsum/hyrise_pmr"
MAPPING_SCRIPT = "/home/umeshsum/hyrise_pmr/myscripts/mapping/mapping.py"
# HYRISE_ROOT = "/data1/sumanthu/hyrise_pmr"
# MAPPING_SCRIPT = "/data1/sumanthu/hyrise_pmr/myscripts/mapping/mapping.py"

#Parameters to sweep

SF=100
# DAM_sizes = [int(SF * f * 2**30) for f in [0.05, 0.10, 0.15, 0.20, 0.25]] #5% to 25% of SF in bytes
DAM_sizes = [int(SF * f * 2**30) for f in [0.25]] #5% to 25% of SF in bytes

table_fractions = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9] #10% to 50% of tables in DAM
# table_fractions = [0.6, 0.7, 0.8, 0.9] #10% to 50% of tables in DAM
query_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22] #Queries to run
# query_ids = [1,2,3] #Queries to run

for dam_size in DAM_sizes:
    for table_fraction in table_fractions:
        for query_id in query_ids:
            #Run the mapping script with the given parameters
            # cmd = f"python {MAPPING_SCRIPT} -p {HYRISE_ROOT}/myscripts/saved_data/sf1_hotness/access_ctr/hotness_{query_id}.json -s {HYRISE_ROOT}/myscripts/saved_data/column_sizes_sf50.dat -c {int(dam_size*table_fraction)},none -l 45,145 -o {os.path.abspath(f'scratch/sf50/p2/map_d{dam_size}_f{int(table_fraction*100)}_q{query_id}.json')} --sql-out"
            cmd = f"python {MAPPING_SCRIPT} -p {HYRISE_ROOT}/myscripts/saved_data/sf1_hotness/access_ctr/hotness_{query_id}.json -s {HYRISE_ROOT}/myscripts/saved_data/column_sizes_sf100.dat -c {int(dam_size*table_fraction)},none -l 45,145 -o {os.path.abspath(f'scratch/sf100/map_d{dam_size}_f{int(table_fraction*100)}_q{query_id}.json')} --sql-out"
            # cmd = f"python {MAPPING_SCRIPT} -p {HYRISE_ROOT}/myscripts/saved_data/sf50_hotness/lqp/hotness_{query_id}.json -s {HYRISE_ROOT}/myscripts/saved_data/column_sizes_sf50.dat -c {int(dam_size*table_fraction)},none -l 45,145 -o {os.path.abspath(f'scratch/sf50_lqp/map_d{dam_size}_f{int(table_fraction*100)}_q{query_id}.json')} --sql-out"
            # cmd = f"python {MAPPING_SCRIPT} -p {HYRISE_ROOT}/myscripts/saved_data/sf50_hotness/pqp/hotness_{query_id}.json -s {HYRISE_ROOT}/myscripts/saved_data/column_sizes_sf50.dat -c {int(dam_size*table_fraction)},none -l 45,145 -o {os.path.abspath(f'scratch/sf50_pqp/map_d{dam_size}_f{int(table_fraction*100)}_q{query_id}.json')} --sql-out"
            print(cmd)