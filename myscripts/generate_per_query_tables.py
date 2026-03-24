import os
import sys
import argparse

from cols_in_tpch_query import cols_in_query
from tpch_common import tpch_table_column_names

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate per-query tables for TPCH queries.")
    parser.add_argument("query_file", help="Path to the TPCH query file.")
    parser.add_argument("-b","--bin", help="Path to the Hyrise binary.")
    parser.add_argument("-s","--scaling-factor", help="Scaling factor for the TPCH data.",type=float)
    parser.add_argument("-o","--out-dir", help="Directory to save the exported tables")

    args = parser.parse_args()

    query_file = os.path.abspath(args.query_file)
    bin_path = args.bin
    scaling_factor = args.scaling_factor
    out_dir = os.path.abspath(args.out_dir)

    query_name = os.path.basename(query_file).split('.')[0]

    #Create column to table mapping
    col_to_table = {}
    for table, col in tpch_table_column_names:
        col_to_table[col] = table

    #Get all columns used in query
    cols = cols_in_query(query_file)
    #Get unique tables from columns
    tables = set()
    for col in cols:
        tables.add(col_to_table[col])
    #Groups cols used by table
    cols_per_table = {}
    for col in cols:
        table = col_to_table[col]
        if table not in cols_per_table:
            cols_per_table[table] = []
        cols_per_table[table].append(col)

    #Create sql script
    sql_script = "setting workers 16\nsetting print off\nsetting mem_strategy Heap\n"
    sql_script += f"generate_tpch {scaling_factor}\n"

    for table in tables:
        sql_script += f"CREATE TABLE {table}_t as SELECT {','.join(cols_per_table[table])} FROM {table};\n"
        sql_script += f"DROP TABLE {table};\n"
        sql_script += f"CREATE TABLE {table} as SELECT * FROM {table}_t;\n"
        sql_script += f"export {table} {os.path.join(out_dir, f'{table}.bin')}\n"

    sql_script += "quit\n"

    #Write sql script to file
    sql_file = os.path.join(out_dir, f"export_{query_name}.sql")

    os.makedirs(out_dir, exist_ok=True)

    with open(sql_file, 'w') as f:
        f.write(sql_script)
    
    sql_file = os.path.join(out_dir, f"import_{query_name}.sql")

    with open(sql_file, 'w') as f:
        f.write("setting workers 16\nsetting print off\nsetting mem_strategy Heap\n")
        for table in tables:
            f.write(f"load {os.path.join(out_dir, f'{table}.bin')} {table} \n")