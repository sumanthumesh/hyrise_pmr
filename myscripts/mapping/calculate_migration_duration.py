#!/usr/bin/env python3
import csv
import json
import os
import re
import sys

def load_durations(dat_file):
    durations = {}
    with open(dat_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            col, dur = line.split(",", 1)
            durations[col.strip()] = float(dur.strip())
    return durations

def parse_filename(path):
    """Extract dam_size, table_fraction, query_id from map_<dam>_f<frac>_q<qid>.json"""
    name = os.path.basename(path)
    m = re.match(r"map_d(\d+)_f(\d+)_q(\d+)\.json", name)
    if not m:
        raise ValueError(f"Filename '{name}' does not match expected pattern map_<dam>_f<frac>_q<qid>.json")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))

def migration_duration(durations, map_file):
    with open(map_file) as f:
        mapping = json.load(f)

    first_list = mapping["placement"][0]
    total = 0.0
    missing = []
    for col in first_list:
        if col in durations:
            total += durations[col]
        else:
            missing.append(col)

    if missing:
        print(f"WARNING ({os.path.basename(map_file)}): no duration data for: {missing}", file=sys.stderr)

    return total

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <migration_duration.dat> <output.csv> <map.json> [map.json ...]")
        sys.exit(1)

    dat_file   = sys.argv[1]
    output_csv = sys.argv[2]
    map_files  = sys.argv[3:]

    durations = load_durations(dat_file)

    rows = []
    for map_file in map_files:
        dam_size, table_fraction, query_id = parse_filename(map_file)
        total_ns = migration_duration(durations, map_file)
        rows.append((dam_size, table_fraction, query_id, total_ns))

    rows.sort(key=lambda r: (r[0], r[1], r[2]))

    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["dam_size_gb", "local_capacity_gb", "query_id", "migration_duration_ns"])
        for dam_size, table_fraction, query_id, duration in rows:
            dam_gb = dam_size / (2 ** 30)
            local_gb = (table_fraction / 100) * dam_gb
            writer.writerow([dam_gb, local_gb, query_id, duration])

    print(f"Wrote {len(rows)} rows to {output_csv}")
