import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract query_id and duration from query_exec_info_*.json files into a CSV."
    )
    parser.add_argument(
        "directory",
        help="Directory containing query_exec_info_<N>.json files",
    )
    parser.add_argument(
        "--output", "-o",
        default="runtime_info.csv",
        help="Output CSV file (default: runtime_info.csv)",
    )
    args = parser.parse_args()

    pattern = re.compile(r"^query_exec_info_(\d+)\.json$")

    files = []
    for name in os.listdir(args.directory):
        m = pattern.match(name)
        if m:
            files.append((int(m.group(1)), name))

    if not files:
        print(f"No query_exec_info_*.json files found in {args.directory}", file=sys.stderr)
        sys.exit(1)

    files.sort(key=lambda x: x[0])

    # Collect durations per query_id in file order.
    durations_by_query: dict[int, list[int]] = defaultdict(list)
    for _, name in files:
        path = os.path.join(args.directory, name)
        with open(path) as f:
            data = json.load(f)
        query_id = int(os.path.splitext(os.path.basename(data["script_file"]))[0])
        durations_by_query[query_id].append(data["duration"])

    # Determine max number of repetitions to size the header.
    max_reps = max(len(v) for v in durations_by_query.values())
    header = ["query_id"] + [f"duration_{i}" for i in range(max_reps)]

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for query_id in sorted(durations_by_query):
            row = [query_id] + durations_by_query[query_id]
            writer.writerow(row)

    print(f"Written {args.output}  ({len(durations_by_query)} queries, {max_reps} repetitions each)")
