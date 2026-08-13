"""
Extract per-query peak memory usage from a set of query_exec_info_*.json files.

Usage:
    python3 extract_mem_usage.py path/*.json -o mem_usage.csv

Each JSON must contain:
  - "script_file": ".../<QUERY_ID>.sql"  (query id parsed from the filename stem)
  - "memory_deltas": [{"peak_mem_usage": ..., "total_allocated_bytes_delta": ...}, ...]

Output CSV columns:
  query_id, peak_mem_usage, total_allocated_bytes_delta
Values are summed across every pool entry in memory_deltas for a given file.
"""

import argparse
import csv
import json
import os
import sys


def parse_query_id(script_file: str):
    """/data1/sumanthu/tpch_queries/1.sql -> 1"""
    stem = os.path.splitext(os.path.basename(script_file))[0]
    try:
        return int(stem)
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", help="query_exec_info_*.json files")
    parser.add_argument("-o", "--output", default="mem_usage.csv", help="Output CSV path")
    args = parser.parse_args()

    rows = []
    for path in args.files:
        with open(path) as f:
            data = json.load(f)

        query_id = parse_query_id(data.get("script_file", ""))
        if query_id is None:
            print(f"warning: skipping {path} — could not parse query_id from script_file={data.get('script_file')!r}",
                  file=sys.stderr)
            continue

        memory_deltas = data.get("memory_deltas", [])
        peak_sum = sum(int(m.get("peak_mem_usage", 0)) for m in memory_deltas)
        total_sum = sum(int(m.get("total_allocated_bytes_delta", 0)) for m in memory_deltas)

        rows.append({
            "query_id": query_id,
            "peak_mem_usage": peak_sum,
            "total_allocated_bytes_delta": total_sum,
        })

    rows.sort(key=lambda r: r["query_id"])

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["query_id", "peak_mem_usage", "total_allocated_bytes_delta"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows)} row(s) to {args.output}")


if __name__ == "__main__":
    main()
