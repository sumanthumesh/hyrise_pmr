import os
import sys
import argparse
import re


HYRISE_ROOT = "/data1/sumanthu/hyrise_pmr"

migration_file_regex = re.compile(r"map_d(\d+)_f(\d+)_q(\d+)_.*migrate\.sql")

if __name__ == "__main__":

    parser = argparse.ArgumentParser("Given a list of mapping/migration sql scripts, generate a single sql script to run all of them with a single generate_tpch command")
    parser.add_argument("migration_scripts", nargs="+", help="List of migration sql scripts to combine")
    parser.add_argument("-s","--scaling-factor", type=int, required=True, help="Scaling factor for the generate_tpch command")
    parser.add_argument("-n","--num-trials", required=True, help="Number of times to run each query (for averaging)")
    parser.add_argument("-o","--output-file", required=True, help="Path to the output sql file")

    args = parser.parse_args()

    input_files = [os.path.abspath(script) for script in args.migration_scripts]
    scaling_factor = args.scaling_factor
    num_trials = int(args.num_trials)
    output_file = os.path.abspath(args.output_file)

    final_script_text = (
        f"setting print off\n"
        f"setting binary_caching on\n"
        f"setting workers 16\n"
        f"hsh new_mem 64000000000 0 0\n"
        f"hsh new_mem 64000000000 0 1\n"
        f"hsh mem_usage\n"
        f"setting mem_strategy TableGen\n"
        f"generate_tpch {scaling_factor}\n"
        f"hsh mem_usage\n"
        f"script {HYRISE_ROOT}/myscripts/move_all_cols_to_remote.sql\n"
        f"hsh delete_mem 0\n"
        f"hsh mem_usage\n"
        f"hsh new_mem 50000000000 1 2\n"
        f"hsh new_mem 50000000000 1 3\n"
        f"hsh mem_usage\n"
        f"setting workers 8\n"
        f"setting mem_strategy Greedy\n"
    )

    for migration_script in input_files:
        filename = os.path.basename(migration_script)
        match = migration_file_regex.match(filename)
        if not match:
            print(f"Error: Migration script filename '{filename}' does not match expected pattern 'map_d{{DAM_size}}_f{{table_fraction}}_q{{query_id}}_.*migrate.sql'")
            sys.exit(1)
        match_dam_size, match_table_fraction, match_query_id = match.groups()
        dam_size = int(match_dam_size)
        table_fraction = float(match_table_fraction)
        final_script_text += f"setting label d{match_dam_size}_tf{int(dam_size*table_fraction/100)}_q{match_query_id}\n"
        final_script_text += f"hsh set_mem_capacity {dam_size} 1000000000000\n"
        final_script_text += f"hsh clear_plan_caches\n"
        final_script_text += f"hsh clear_pipeline\n"
        final_script_text += f"hsh reset_exec_pools\n"
        final_script_text += f"script {migration_script}\n"
        final_script_text += f"hsh mem_usage\n"
        for trial in range(num_trials):
            final_script_text += f"script /data1/sumanthu/tpch_queries/{match_query_id}.sql\n"
    final_script_text += "hsh mem_usage\n"
    final_script_text += "quit\n"

    with open(output_file, 'w') as f:
        f.write(final_script_text)
