import os
import sys

from tpch_common import tpch_table_column_names

def cols_in_query(filename):
    tpch_cols = [s[1] for s in tpch_table_column_names]
    cols_in_query = set()
    for line in open(filename).readlines():
        for col in tpch_cols:
            if col in line:
                cols_in_query.add(col)
    return cols_in_query

if __name__ == "__main__":

    query_file = sys.argv[1]
    cols = cols_in_query(query_file)
    for c in cols:
        print(c)