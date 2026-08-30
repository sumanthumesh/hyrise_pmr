import os
import sys
import argparse

# tpch_common lives in myscripts/, the parent of this directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tpch_common import tpch_table_column_names

column_names = [c for t,c in tpch_table_column_names]
table_name_from_column = {c:t for t,c in tpch_table_column_names}

def columns_in_query(queryfile):
    """Columns referenced by the SQL itself.

    Anything after a `--` is stripped first: query files carry comments that
    name other columns for documentation, and those must not be pinned.
    """
    columns = set()
    for line in open(queryfile).readlines():
        code = line.split("--", 1)[0]
        for column in column_names:
            if column in code:
                columns.add(column)
    return columns

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mapping scripts for microbenchmarks.")
    parser.add_argument("input_files", nargs="+", help="Query .sql files to process")
    parser.add_argument("-o","--output_dir", default=".", help="Directory to save the generated mapping scripts")
    args = parser.parse_args()


    for queryfile in args.input_files:
        columns = columns_in_query(queryfile)
        query_id = os.path.basename(queryfile).split(".")[0]
        mapping_script_filename = os.path.join(args.output_dir,f"map_d0_f0_q{query_id}_migrate.sql")
        with open(mapping_script_filename, "w") as mapping_script_file:
            for column in columns:
                table_name = table_name_from_column[column]
                mapping_script_file.write(f"move2cxl {table_name} {column} 0\n")


    
