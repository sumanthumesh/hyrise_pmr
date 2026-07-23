import os
import sys
import argparse
import json
from typing import Dict, List, Optional, Set, Tuple
from solver_core import solve_column_placement
from tpch_columns import table_from_column

def parse_hotness_file(hotness_file_path):
    with open(hotness_file_path, 'r') as f:
        hotness_raw = json.load(f)
    hotness:Dict[str,int] = {}
    for deltas in hotness_raw["column_access_deltas"]:
        hotness[deltas["column_name"]] = deltas["access_delta"]
    return hotness

def parse_size_file(size_file_path):
    col_sizes:Dict[str,int] = {}
    with open(size_file_path, 'r') as f:
        col_sizes_raw = json.load(f)
    return col_sizes_raw

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run the column placement solver"
    )

    parser.add_argument("-p","--hotness-file", required=True, help="Path to the hotness json file")
    parser.add_argument("-s","--size-file", required=True, help="Path to the size json file")
    parser.add_argument("-c","--mem-capacities", required=True, help="comma-separated list of memory capacities (use 'None' for unbounded)")
    parser.add_argument("-l","--mem-latencies", required=True, help="comma-separated list of memory latencies (must match the number of capacities)")
    parser.add_argument("-o","--output-file", required=True, help="Path to the output json file")
    parser.add_argument("--sql-out", action="store_true", help="Set to true if you want a sql output file that moves all columns according to the placement")

    args = parser.parse_args()

    #Parse the hotness and size files
    hotness = parse_hotness_file(args.hotness_file)
    col_sizes = parse_size_file(args.size_file)

    #Ensure that the hotness and size files have the same columns
    if set(hotness.keys()) != set(col_sizes.keys()):
        print("Error: The hotness and size files must have the same columns.")
        #Print the columns that are missing in each file
        missing_in_hotness = set(col_sizes.keys()) - set(hotness.keys())
        missing_in_size = set(hotness.keys()) - set(col_sizes.keys())
        if missing_in_hotness:
            print(f"Columns missing in hotness file: {missing_in_hotness}")
        if missing_in_size:
            print(f"Columns missing in size file: {missing_in_size}")
        sys.exit(1)

    # Combine into a single dictionary of columns with (hotness, size)
    columns:Dict[str,Tuple[int, int]] = {col:(hotness[col], col_sizes[col]) for col in hotness.keys()}

    #Parse the memory capacities and latencies
    mem_capacities = [None if cap.strip().lower() == "none" else int(cap.strip()) for cap in args.mem_capacities.split(",")]
    mem_latencies = [int(lat.strip()) for lat in args.mem_latencies.split(",")]

    if len(mem_capacities) != len(mem_latencies):
        print("Error: The number of memory capacities and latencies must match.")
        sys.exit(1)

    memories:List[Tuple[Optional[int], int]] = list(zip(mem_capacities, mem_latencies))

    #Solve the column placement problem
    placement = solve_column_placement(columns, memories)

    assert len(placement) == len(memories), "Placement result length does not match number of memories"

    total_sizes = [sum(col_sizes[col] for col in mem) for mem in placement]

    #Write the placement to the output file
    placment_output_file = args.output_file
    with open(placment_output_file, 'w') as f:
        json.dump({
            "placement": [list(mem) for mem in placement],
            "total_sizes": total_sizes,
        }, f, indent=4)

    #Based on the mapping, need to move all columns except those in the first memory to remote memory
    sql_output_file = args.output_file.replace(".json", "_migrate.sql")
    if args.sql_out:
        with open(sql_output_file, 'w') as f:
            for mem_idx, mem in enumerate(placement):
                for col in mem:
                    f.write(f"move2cxl {table_from_column[col]} {col} {mem_idx}\n")