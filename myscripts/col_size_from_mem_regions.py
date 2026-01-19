import csv
import os
import sys


if __name__ == '__main__':
    columns_file = sys.argv[1]
    regions_file = sys.argv[2]
    
    # Extract column names
    reader = csv.reader(open(columns_file), delimiter=',')
    col_name_vs_id = dict()
    col_id_vs_name = dict()
    for row in reader:
        col_id = int(row[0].split(':')[0])
        col_name = row[1]
        col_name_vs_id.update({col_name:col_id})
        col_id_vs_name.update({col_id:col_name})

    col_name_vs_size = dict()
    # Extract memory regions
    reader = csv.reader(open(regions_file), delimiter=',')
    for row in reader:
        col_id = int(row[0])
        start = int(row[1],16)
        end = int(row[2],16)
        size = end - start
        col_name = col_id_vs_name[col_id]
        if col_name not in col_name_vs_size.keys():
            col_name_vs_size.update({col_name:0})
        col_name_vs_size[col_name] += size

    for col_name, size in col_name_vs_size.items():
        print(f"{col_name},{size}")

