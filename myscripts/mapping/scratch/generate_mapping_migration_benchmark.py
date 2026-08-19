import os
import sys
import argparse
import re

template = re.compile(r"map_d(\d+)_f(\d+)_q(\d+)_migrate.sql")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate mapping migration benchmark scripts")
    parser.add_argument("mapping_files", nargs="+", help="Mapping files to process")
    parser.add_argument("--num-trials","-n", type=int, default=5, help="Number of trials to run for each mapping")
    parser.add_argument("--output","-o", help="Output sql script file")

    args = parser.parse_args()

    output_file = args.output

    script_text = (
        f"setting print off\n"
        f"setting binary_caching on\n"
        f"setting workers 16\n"
        f"generate_tpch 1\n"
        f"script /data1/sumanthu/hyrise_pmr/myscripts/move_all_cols_to_remote.sql\n"
    )

    for file in args.mapping_files:
        match = template.search(os.path.basename(file))
        if not match:
            print(f"Failed Regex")
            sys.exit(1)
        dam_size = int(match.group(1))
        table_fraction = int(match.group(2))
        label = f"d{dam_size}_tf{int(dam_size*table_fraction/100)}_q{match.group(3)}"
        script_text += f"setting label {label}\n"
        columns_to_move = []
        for line in open(file).readlines():
            if "move2cxl" not in line:
                continue
            destination_node_id = int(line.split(" ")[-1].strip())
            assert destination_node_id in [0, 1], f"Unexpected destination node id: {destination_node_id}"
            table_name = line.split(" ")[1].strip()
            column_name = line.split(" ")[2].strip()
            if destination_node_id == 0:
                columns_to_move.append((table_name, column_name))
        for _ in range(args.num_trials):
            script_text += f"setting print_migration_stats off\n"
            for table_name, column_name in columns_to_move:
                script_text += f"move2cxl {table_name} {column_name} 1\n"
            script_text += f"setting print_migration_stats on\n"
            script_text += f"hsh queue start\n"
            for table_name, column_name in columns_to_move:
                script_text += f"move2cxl {table_name} {column_name} 0\n"
            script_text += f"hsh queue end\n"            
            script_text += f"setting print_migration_stats off\n"

    script_text += f"quit\n"

    with open(output_file, "w") as f:
        f.write(script_text)