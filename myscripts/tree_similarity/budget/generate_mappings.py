"""
Snap heuristic tableDAM budgets onto the measured DAM/10 grid.

The budget CSV (query_id, table_dam, regular_dam) holds the continuous split a
heuristic recommends. The sweeps, however, only ever measured table sizes at
multiples of DAM/10, so a recommendation has to be snapped to the nearest grid
point before it can be compared against a measurement or turned into a mapping.

For each row this prints the query id and the grid point closest to its
table_dam.

Usage:
    python3 generate_mappings.py <budget.csv> --dam 25 --scale-factor 100
    python3 generate_mappings.py lqp_heur_lqp_hot_budget.csv -d 25 -s 100
"""
import argparse
import csv
import os
import sys

HYRISE_ROOT = "/data1/sumanthu/hyrise_pmr"
MAPPING_SCRIPT = f"{HYRISE_ROOT}/myscripts/mapping/mapping.py"


def grid_points(dam_gb, divisions, include_zero):
    """The measured table sizes: multiples of dam/divisions.

    The sweeps ran f10..f90, i.e. one DAM/10 step up to 9 of them, so zero is
    not a measured point and is excluded unless asked for. It matters: a budget
    of 0.615 GB is closer to 0 than to 2.5, so including zero changes which
    grid point such a row lands on.
    """
    step = dam_gb / divisions
    start = 0 if include_zero else 1
    return [step * k for k in range(start, divisions + 1)]


def snap(value, points):
    return min(points, key=lambda p: (abs(p - value), p))


def fmt(value):
    """2.5 -> '2.5', 5.0 -> '5'."""
    return f"{value:g}"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("budget_csv", help="CSV of query_id,table_dam,regular_dam")
    ap.add_argument("--dam", "-d", type=float, required=True,
                    help="Total DAM in GB; the grid is multiples of DAM/divisions")
    ap.add_argument("--scale-factor", "-s", type=int, required=True,
                    help="TPC-H scale factor; selects column_sizes_sfN.dat "
                         "when --emit-commands is used")
    ap.add_argument("--divisions", type=int, default=10,
                    help="Number of grid steps in DAM (default 10)")
    ap.add_argument("--include-zero", action="store_true",
                    help="Treat 0 GB as a valid grid point")
    ap.add_argument("--row-count-file", default=None,
                    help="Row-count CSV for --emit-commands; without it mapping.py "
                         "optimises raw access counts rather than bytes/row")
    ap.add_argument("--hotness-dir", default=None,
                    help="Hotness directory for --emit-commands")
    ap.add_argument("--out-dir", default="mappings",
                    help="Mapping output directory for --emit-commands")
    ap.add_argument("--emit-commands", action="store_true",
                    help="Instead of the snapped sizes, print one mapping.py "
                         "invocation per row using the snapped table size")
    args = ap.parse_args()

    points = grid_points(args.dam, args.divisions, args.include_zero)

    with open(args.budget_csv, newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        sys.exit(f"{args.budget_csv} is empty")

    for row in rows:
        query_id = row["query_id"].strip()
        table_dam = float(row["table_dam"])
        table_size = snap(table_dam, points)

        if not args.emit_commands:
            print(f"{query_id},{fmt(table_size)}")
            continue

        # mapping.py takes capacities in BYTES (column_sizes_sfN.dat is in
        # bytes), not GB -- passing GB raises "invalid literal for int()".
        capacity_bytes = int(round(table_size * (1 << 30)))
        row_counts = args.row_count_file or (
            f"{HYRISE_ROOT}/myscripts/saved_data/sf{args.scale_factor}_row_count.csv")
        hotness_dir = args.hotness_dir or (
            f"{HYRISE_ROOT}/myscripts/saved_data/sf{args.scale_factor}_hotness/lqp")
        # Name the mapping in bytes too, matching the existing
        # map_d26843545600_* files rather than mixing units with the -c value.
        dam_bytes = int(round(args.dam * (1 << 30)))
        out = os.path.abspath(os.path.join(
            args.out_dir, f"map_d{dam_bytes}_f{capacity_bytes}_q{query_id}.json"))
        print(f"python {MAPPING_SCRIPT}"
              f" -p {hotness_dir}/hotness_{query_id}.json"
              f" -s {HYRISE_ROOT}/myscripts/saved_data/column_sizes_sf{args.scale_factor}.dat"
              f" -r {row_counts}"
              f" -c {capacity_bytes},none -l 45,145 -o {out} --sql-out")


if __name__ == "__main__":
    main()
