import os
import sys
import argparse

HYRISE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Generate a benchmark script for migration")
    parser.add_argument("-s","--scaling-factor", type=int, required=True, help="Scaling factor for the benchmark")
    parser.add_argument("-n","--num-trials", type=int, required=True, help="Number of trials for the benchmark")
    parser.add_argument("-m","--mode",choices=["d2c","c2d"], required=True, help="Migration mode: d2c (DAM to CXL) or c2d (CXL to DAM)")
    parser.add_argument("-o","--output", type=str, required=True, help="Output file path for the generated benchmark script")
    args = parser.parse_args()

    if args.mode == "d2c":
        setup_script = os.path.join(HYRISE_ROOT,"myscripts/all_cols_to_local.sql")
        run_script = os.path.join(HYRISE_ROOT,"myscripts/all_cols_to_remote.sql")
    else:
        setup_script = os.path.join(HYRISE_ROOT,"myscripts/all_cols_to_remote.sql")
        run_script = os.path.join(HYRISE_ROOT,"myscripts/all_cols_to_local.sql")
    
    script_text = (
        f"setting print off\n"
        f"setting binary_caching on\n"
        f"setting workers 16\n"
        f"generate_tpch {args.scaling_factor}\n"
    )
    for _ in range(args.num_trials):
        script_text += (
        f"setting print_migration_stats off\n"
        f"script {setup_script}\n"
        f"setting print_migration_stats on\n"
        f"script {run_script}\n"
        )
    script_text += "quit\n"
    
    with open(args.output, 'w') as f:
        f.write(script_text)