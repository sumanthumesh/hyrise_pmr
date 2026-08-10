"""
Extract per-column hotness from a PQP tree JSON emitted by PQPVisualizer.

Usage:
    python3 column_hotness_from_pqp.py pqp0.tree.json [pqp1.tree.json ...]

Model:
    For each node in the tree:
      cols in node["columns_left"]  += parse(LeftCard)
      cols in node["columns_right"] += parse(RightCard)
    Falls back to the combined `columns` field for older JSON files that predate the
    left/right split.

    Column names are validated against the real TPC-H schema (tpch_columns.py); any
    name that is not an actual base column is dropped. Any real column mentioned in a
    node's `description` but missing from its column lists is folded in (use
    --lists-only to disable). That fallback carries most of the weight here: dumps
    produced before performance_data->left_read_columns was populated have all three
    column fields empty, so the description is the only column signal they contain.

    For join operators the description reads "<left_col> = <right_col>", so each side
    of a comparison is charged to the matching input. Everything else recovered from a
    description is charged to the left input.

    Diamond-shaped PQPs are already deduped by PQPVisualizer (a shared subtree is
    emitted once, then as an empty {}), so no --dedup flag is needed here.

    The denominator reported alongside the hotness is total *column-rows*: a node
    reading N columns of a K-row input touches N*K of them, so each side's cardinality
    is weighted by its column count rather than counted once. It tallies every name the
    node reported, including derived ones ("SUM(l_quantity)") that the whitelist drops,
    so hotness/column-rows reads as the share of column traffic that lands on real base
    columns -- 1.000 would mean every read was attributed.
Output:
    table.column -> hotness (rows read across all ops), sorted descending, plus a
    per-table rollup.
"""

import argparse
import json
import re
import sys
from collections import defaultdict

from tpch_columns import table_from_column


_ROWS_RE = re.compile(r"^\s*([\d,]+)\s+row")
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_COMPARISON_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:<=|>=|!=|<>|=|<|>)\s*([A-Za-z_][A-Za-z0-9_]*)\b"
)


def parse_card(card_str: str) -> int:
    """
    "6001215 row(s)/6 chunk(s)" -> 6001215
    ""                          -> 0
    """
    if not card_str:
        return 0
    m = _ROWS_RE.match(card_str)
    if not m:
        return 0
    return int(m.group(1).replace(",", ""))


def columns_in_description(description: str) -> set:
    """Real TPC-H columns named anywhere in a node description."""
    return {t for t in _IDENT_RE.findall(description or "") if t in table_from_column}


def split_join_description(description: str):
    """
    "JoinHash (Inner) l_orderkey = o_orderkey" -> ({"l_orderkey"}, {"o_orderkey"})

    Hyrise prints join predicates with the left input's column first, so each side of
    a comparison can be charged to the input it actually came from.
    """
    left, right = set(), set()
    for a, b in _COMPARISON_RE.findall(description or ""):
        if a in table_from_column and b in table_from_column:
            left.add(a)
            right.add(b)
    return left, right


def is_join(node: dict) -> bool:
    return "Join" in node.get("name", "")


def walk(node: dict, hotness: dict, totals: dict, dropped: dict, lists_only: bool):
    if not isinstance(node, dict) or not node:
        return

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
    # Everything the node reported, base column or not (derived names like
    # "SUM(l_quantity)" included). These size the column-rows denominator.
    all_left = set(reported_left)
    all_right = set(reported_right)

    if not lists_only:
        description = node.get("description", "")
        if is_join(node):
            join_left, join_right = split_join_description(description)
            # Compare against the reported lists, not the sets being built, so a
            # self-join predicate ("l_orderkey = l_orderkey") still lands on both sides.
            reported_l, reported_r = set(left), set(right)
            left |= join_left - reported_r
            right |= join_right - reported_l
        # Whatever the description names that neither list nor the join predicate
        # placed: charge the left input, the only one a single-input node has.
        extra = columns_in_description(description) - left - right
        totals["recovered"] += len(extra)
        left |= extra

    all_left |= left
    all_right |= right

    # A node reading N columns of a K-row input touches N*K column-rows, so weight each
    # side's cardinality by how many columns it read. Counting every reported name (not
    # just the base columns that survive the whitelist) keeps this an upper bound, which
    # makes hotness/column_rows the share of traffic attributable to real base columns.
    totals["column_rows"] += left_rows * len(all_left) + right_rows * len(all_right)

    if right_rows and not right:
        totals["unattributed_right"] += 1

    for col in left:
        hotness[col] += left_rows
    for col in right:
        hotness[col] += right_rows

    if "Leftchildren" in node:
        walk(node["Leftchildren"], hotness, totals, dropped, lists_only)
    if "Rightchildren" in node:
        walk(node["Rightchildren"], hotness, totals, dropped, lists_only)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", help="PQP tree JSON files")
    parser.add_argument("-o", "--output", help="Write JSON output instead of printing")
    parser.add_argument(
        "--lists-only",
        action="store_true",
        help="Trust columns_left/columns_right only; do not recover columns from descriptions",
    )
    args = parser.parse_args()

    hotness = defaultdict(int)
    totals = {"column_rows": 0, "recovered": 0, "unattributed_right": 0}
    dropped = defaultdict(int)
    for path in args.files:
        with open(path) as f:
            root = json.load(f)
        # Multi-plan files are wrapped in a top-level list.
        roots = root if isinstance(root, list) else [root]
        for r in roots:
            walk(r, hotness, totals, dropped, args.lists_only)

    ranked = sorted(hotness.items(), key=lambda kv: kv[1], reverse=True)

    by_table = defaultdict(int)
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
            print(f"{table_from_column[col] + '.' + col:30s} {rows:>18,}")
        print()
        print("per table")
        for table, rows in ranked_tables:
            print(f"{table:30s} {rows:>18,}")
        print()
        print(f"{'total column-rows (LeftCard*#left + RightCard*#right)':52s} {column_rows_total:>18,}")
        print(f"{'total hotness (sum over base columns)':52s} {hotness_total:>18,}")
        print(f"{'ratio (hotness / column-rows)':52s} {ratio:>18.3f}")

    if dropped:
        summary = ", ".join(f"{name} x{count}" for name, count in
                            sorted(dropped.items(), key=lambda kv: -kv[1]))
        print(f"note: dropped non-schema names: {summary}", file=sys.stderr)
    if totals["recovered"]:
        print(f"note: recovered {totals['recovered']} column mention(s) from descriptions",
              file=sys.stderr)
    if totals["unattributed_right"]:
        print(f"note: {totals['unattributed_right']} node(s) had a right input whose columns "
              f"could not be identified; their RightCard counts toward cardinality but not hotness",
              file=sys.stderr)


if __name__ == "__main__":
    main()
