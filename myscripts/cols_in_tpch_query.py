import os
import sys
import argparse

from tpch_common import tpch_table_column_names, tpch_column_type

def cols_in_query(filename):
    tpch_cols = [s[1] for s in tpch_table_column_names]
    cols_in_query = set()
    for line in open(filename).readlines():
        for col in tpch_cols:
            if col in line:
                cols_in_query.add(col)
    return cols_in_query

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Extract column names used in a TPC-H query file")
    parser.add_argument("query_files", nargs="+", help="Paths to the TPC-H query files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    args = parser.parse_args()
    
    all_cols = set()
    
    for query_file in args.query_files:
        cols = cols_in_query(query_file)
        all_cols.update(cols)

        if args.verbose:
            print(f"Columns in {query_file}:")
            for c in cols:
                print(c)
                
    print(f"Columns used across {len(args.query_files)} queries:")
    table_name_from_column = {col: table for table, col in tpch_table_column_names}
    for c in all_cols:
        print(f"{c},{tpch_column_type[(table_name_from_column[c], c)]}")