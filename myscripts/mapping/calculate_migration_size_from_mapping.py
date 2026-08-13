"""
Extract DRAM / table / resident sizes (in GB) from one or more mapping JSON files
produced by the placement solver.

File-name convention (parsed for dram_size + table_size):
    map_d<DRAM_BYTES>_f<TABLE_FRACTION_PCT>_q<QUERY>[...].json
        dram_size    = <DRAM_BYTES> bytes
        table_size   = dram_size * <TABLE_FRACTION_PCT> / 100

File content:
    total_sizes[0] is dram_resident_size in bytes.
    (total_sizes[1] is the far-memory / spillover size and is ignored here.)

Output CSV columns:
    filename, dram_size_gb, table_size_gb, dram_resident_size_gb
"""

import argparse
import csv
import json
import os
import re
import sys

GB = 2**30
NAME_RE = re.compile(r"map_d(\d+)_f(\d+)_q(\d+)")


def parse_name(path: str):
    """Return (dram_bytes, table_fraction_pct, query_id) or (None, None, None) if it doesn't match."""
    m = NAME_RE.search(os.path.basename(path))
    if not m:
        return None, None, None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def dram_resident_bytes(path: str) -> int:
    with open(path) as f:
        data = json.load(f)
    return int(data["total_sizes"][0])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mapping_files", nargs="+", help="Mapping JSON files to process")
    parser.add_argument(
        "-o", "--output", default="migration_size.csv", help="Output CSV path (default: migration_size.csv)"
    )
    args = parser.parse_args()

    rows = []
    for path in args.mapping_files:
        dram_bytes, table_pct, query_id = parse_name(path)
        if dram_bytes is None:
            print(f"warning: skipping {path} — name does not match map_d<bytes>_f<pct>_q<id> pattern", file=sys.stderr)
            continue

        table_bytes = dram_bytes * table_pct / 100.0
        resident_bytes = dram_resident_bytes(path)

        rows.append(
            {
                "filename": os.path.basename(path),
                "query_id": query_id,
                "dram_size_gb": dram_bytes / GB,
                "table_size_gb": table_bytes / GB,
                "dram_resident_size_gb": resident_bytes / GB,
            }
        )

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["filename", "query_id", "dram_size_gb", "table_size_gb", "dram_resident_size_gb"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows)} row(s) to {args.output}")


if __name__ == "__main__":
    main()
