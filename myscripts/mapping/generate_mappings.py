import os
import sys
import argparse

HYRISE_ROOT = "/data1/sumanthu/hyrise_pmr"
MAPPING_SCRIPT = "/data1/sumanthu/hyrise_pmr/myscripts/mapping/mapping.py"

#Parameters to sweep
# DAM_sizes = [i*100*2**20 for i in range(1, 11)] #100MB to 1GB
DAM_sizes = [i*1000*2**20 for i in range(1, 11)] #1GB to 10GB
table_fractions = [0.1, 0.2, 0.3, 0.4, 0.5] #10% to 50% of tables in DAM
query_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22] #Queries to run
# query_ids = [1,2,3] #Queries to run

for dam_size in DAM_sizes:
    for table_fraction in table_fractions:
        for query_id in query_ids:
            #Run the mapping script with the given parameters
            cmd = f"python {MAPPING_SCRIPT} -p {HYRISE_ROOT}/myscripts/saved_data/sf1_hotness/hotness_{query_id}.json -s {HYRISE_ROOT}/myscripts/saved_data/column_sizes_sf1.dat -c {int(dam_size*table_fraction)},none -l 45,145 -o {os.path.abspath(f'scratch/sf10/map_d{dam_size}_f{int(table_fraction*100)}_q{query_id}.json')} --sql-out"
            print(cmd)