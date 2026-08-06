#!/usr/bin/env python3
import re
import csv
import sys

def parse_migration_log(input_file, output_file):
    pattern = re.compile(
        r"##Migration: column (\S+) of size (\d+)B to NUMA node \d+ in (\d+) ns"
    )

    rows = []
    with open(input_file) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                rows.append((m.group(1), int(m.group(2)), int(m.group(3))))

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["column_name", "column_size", "migration_time_ns"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input_log> <output_csv>")
        sys.exit(1)
    parse_migration_log(sys.argv[1], sys.argv[2])
