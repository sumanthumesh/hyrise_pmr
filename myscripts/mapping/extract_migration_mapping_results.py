"""
Extract per-migration measurements from a Hyrise run log.

Reads lines of the form:
    ##ParallelMigration(d<DAM_BYTES>_tf<TABLE_BYTES>_q<QUERY_ID>): \
        <N> columns, <MIG_BYTES> bytes in <MIG_NS> ns (<X.XX> GB/s aggregate)

Emits a CSV with columns:
    query_id, dam_size_gb, table_size_gb, migration_size_gb, migration_duration_ns

Note: sizes are converted from bytes to GiB using 2**30 (not 1e9).

Usage:
    python3 extract_migration_mapping_results.py <run.log> -o migration_results.csv
"""

import argparse
import re
import sys

import pandas as pd

GB = 2**30

# Groups: dam_bytes, table_bytes, query_id, columns, migration_bytes, migration_ns.
LINE_RE = re.compile(
    r"##ParallelMigration\(d(\d+)_tf(\d+)_q(\d+)\)\s*:\s*"
    r"(\d+)\s+columns,\s*(\d+)\s+bytes\s+in\s+(\d+)\s+ns"
)


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("log", help="Path to a run log file")
    parser.add_argument("-o", "--output", default="migration_results.csv",
                        help="Output CSV path (default: migration_results.csv)")
    args = parser.parse_args()

    rows = []
    with open(args.log) as f:
        for line in f:
            m = LINE_RE.search(line)
            if not m:
                continue
            dam_bytes    = int(m.group(1))
            table_bytes  = int(m.group(2))
            query_id     = int(m.group(3))
            mig_bytes    = int(m.group(5))
            mig_ns       = int(m.group(6))
            rows.append({
                "query_id":             query_id,
                "dam_size_gb":          dam_bytes,
                "table_size_gb":        table_bytes  / GB,
                "migration_size_gb":    mig_bytes    / GB,
                "migration_duration_ns": mig_ns,
            })

    if not rows:
        print(f"no ##ParallelMigration lines matched in {args.log}", file=sys.stderr)

    # Collapse repeat runs of the same (query_id, dam_size_gb, table_size_gb) combo
    # into a single row by taking the mean of migration_size_gb and migration_duration_ns.
    df = pd.DataFrame(rows, columns=["query_id", "dam_size_gb", "table_size_gb",
                                      "migration_size_gb", "migration_duration_ns"])
    df = (
        df.groupby(["query_id", "dam_size_gb", "table_size_gb"], as_index=False)
          .agg({"migration_size_gb": "mean", "migration_duration_ns": "mean"})
          .sort_values(["query_id", "dam_size_gb", "table_size_gb"])
          .reset_index(drop=True)
    )
    df.to_csv(args.output, index=False)

    print(f"wrote {len(df)} row(s) to {args.output}  (from {len(rows)} raw entries)")


if __name__ == "__main__":
    main()
