"""
Extract per-column hotness from an LQP tree JSON emitted by LQPVisualizer.

Usage:
    python3 column_hotness_from_lqp.py lqp_q2.tree.json [lqp_q3.tree.json ...]

Model:
    Same as column_hotness_from_pqp.py, but cardinalities are optimizer *estimates*
    ("80088.4 row(s) est.") rather than measured row counts, so they are floats.
    For each node in the tree:
      cols in node["columns_left"]  += parse(LeftCard)
      cols in node["columns_right"] += parse(RightCard)

    Column names are validated against the real TPC-H schema (tpch_columns.py).
    Anything the LQP reports that is not an actual base column is dropped -- most
    notably "INVALID_COLUMN_ID", the sentinel Hyrise uses for COUNT(*), which reads
    no column data at all. Any real column mentioned in a node's `description` but
    missing from its column lists is folded in on the left side (use --lists-only to
    disable that fallback).

    LQPs are DAGs: a subplan with two parents is printed once in full and left as an
    empty `{}` child the second time -- but some dumps re-expand it instead, which
    double-counts it. Pass --dedup to count each distinct node (by name + description
    + cardinalities) only once per file.

    The denominator reported alongside the hotness is total *column-rows*: a node
    reading N columns of a K-row input touches N*K of them, so each side's cardinality
    is weighted by its column count rather than counted once. It tallies every name the
    node reported, including ones the whitelist drops, so hotness/column-rows reads as
    the share of column traffic that lands on real base columns -- 1.000 would mean
    every read was attributed.
Output:
    table.column -> hotness (estimated rows read across all ops), sorted descending,
    plus a per-table rollup.
"""

import argparse
import json
import re
import sys
from collections import defaultdict

from tpch_columns import table_from_column


_ROWS_RE = re.compile(r"^\s*([\d,]*\.?\d+)\s+row")
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def parse_card(card_str: str) -> float:
    """
    "80088.4 row(s) est." -> 80088.4
    ""                    -> 0.0
    """
    if not card_str:
        return 0.0
    m = _ROWS_RE.match(card_str)
    if not m:
        return 0.0
    return float(m.group(1).replace(",", ""))


def columns_in_description(description: str) -> set:
    """Real TPC-H columns named anywhere in a node description."""
    return {t for t in _IDENT_RE.findall(description or "") if t in table_from_column}


def node_signature(node: dict):
    return (
        node.get("name", ""),
        node.get("description", ""),
        node.get("LeftCard", ""),
        node.get("RightCard", ""),
    )


def walk(node: dict, hotness: dict, totals: dict, dropped: dict, seen, lists_only: bool):
    if not isinstance(node, dict) or not node:
        return
    if seen is not None:
        sig = node_signature(node)
        if sig in seen:
            return
        seen.add(sig)

    left_rows = parse_card(node.get("LeftCard", ""))
    right_rows = parse_card(node.get("RightCard", ""))

    reported_left = node.get("columns_left", [])
    reported_right = node.get("columns_right", [])
    if not reported_left and not reported_right:
        # Fallback for older JSON files that only carry the merged `columns` list.
        reported_left = node.get("columns", [])

    for name in reported_left + reported_right:
        if name not in table_from_column:
            dropped[name] += 1

    left = {c for c in reported_left if c in table_from_column}
    right = {c for c in reported_right if c in table_from_column}
    # Everything the node reported, base column or not (INVALID_COLUMN_ID included).
    # These size the column-rows denominator.
    all_left = set(reported_left)
    all_right = set(reported_right)

    if not lists_only:
        # Columns the description names but the lists missed -- attribute to the left
        # input, which is the only one a single-input node has.
        extra = columns_in_description(node.get("description", "")) - left - right
        totals["recovered"] += len(extra)
        left |= extra

    all_left |= left
    all_right |= right

    # A node reading N columns of a K-row input touches N*K column-rows, so weight each
    # side's cardinality by how many columns it read. Counting every reported name (not
    # just the base columns that survive the whitelist) keeps this an upper bound, which
    # makes hotness/column_rows the share of traffic attributable to real base columns.
    totals["column_rows"] += left_rows * len(all_left) + right_rows * len(all_right)

    for col in left:
        hotness[col] += left_rows
    for col in right:
        hotness[col] += right_rows

    if "Leftchildren" in node:
        walk(node["Leftchildren"], hotness, totals, dropped, seen, lists_only)
    if "Rightchildren" in node:
        walk(node["Rightchildren"], hotness, totals, dropped, seen, lists_only)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", help="LQP tree JSON files")
    parser.add_argument("-o", "--output", help="Write JSON output instead of printing")
    parser.add_argument(
        "--dedup",
        action="store_true",
        help="Count each distinct node once per file (LQP DAGs may re-print shared subplans)",
    )
    parser.add_argument(
        "--lists-only",
        action="store_true",
        help="Trust columns_left/columns_right only; do not recover columns from descriptions",
    )
    args = parser.parse_args()

    hotness = defaultdict(float)
    totals = {"column_rows": 0.0, "recovered": 0}
    dropped = defaultdict(int)
    for path in args.files:
        with open(path) as f:
            root = json.load(f)
        # Multi-plan files are wrapped in a top-level list.
        roots = root if isinstance(root, list) else [root]
        seen = set() if args.dedup else None
        for r in roots:
            walk(r, hotness, totals, dropped, seen, args.lists_only)

    ranked = sorted(hotness.items(), key=lambda kv: kv[1], reverse=True)

    by_table = defaultdict(float)
    for col, rows in ranked:
        by_table[table_from_column[col]] += rows
    ranked_tables = sorted(by_table.items(), key=lambda kv: kv[1], reverse=True)

    hotness_total = sum(rows for _, rows in ranked)
    column_rows_total = totals["column_rows"]
    ratio = (hotness_total / column_rows_total) if column_rows_total else float("nan")

    if args.output:
        with open(args.output, "w") as f:
            columns_with_non_zero_accesses = [col for col, rows in ranked]
            output_dict = {}
            output_dict["column_access_deltas"] = []
            for col,rows in ranked:
                output_dict["column_access_deltas"].append({
                    "access_delta": rows,
                    "column_name": f"{col}",
                    "table_name": f"{table_from_column[col]}"
                })
            for col in table_from_column.keys():
                if col not in columns_with_non_zero_accesses:
                    output_dict["column_access_deltas"].append({
                        "access_delta": 0,
                        "column_name": f"{col}",
                        "table_name": f"{table_from_column[col]}"
                    })
            json.dump(output_dict, f, indent=2)
    else:
        for col, rows in ranked:
            print(f"{table_from_column[col] + '.' + col:30s} {rows:>18,.1f}")
        print()
        print("per table")
        for table, rows in ranked_tables:
            print(f"{table:30s} {rows:>18,.1f}")
        print()
        print(f"{'total est. column-rows (LeftCard*#left + RightCard*#right)':58s} {column_rows_total:>18,.1f}")
        print(f"{'total hotness (sum over base columns)':58s} {hotness_total:>18,.1f}")
        print(f"{'ratio (hotness / column-rows)':58s} {ratio:>18.3f}")

    if dropped:
        summary = ", ".join(f"{name} x{count}" for name, count in
                            sorted(dropped.items(), key=lambda kv: -kv[1]))
        print(f"note: dropped non-schema names: {summary}", file=sys.stderr)
    if totals["recovered"]:
        print(f"note: recovered {totals['recovered']} column mention(s) from descriptions",
              file=sys.stderr)


if __name__ == "__main__":
    main()
