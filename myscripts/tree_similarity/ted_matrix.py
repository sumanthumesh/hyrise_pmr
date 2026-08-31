"""
TED matrix: each new query's LQP against every original TPC-H query's PQP.

Rows are the tree-similarity queries (091, 092, ... 215); columns are the
original TPC-H query ids. Cell (r, c) is the tree edit distance between the
pre-execution plan of query r and the cached post-execution plan of query c --
exactly the lookup the estimator performs when it searches the query cache for
a plan resembling the incoming query.

Because the two sides are different plan formats, columns are extracted in
'shared' mode (see tree_edit_distance.node_columns): only the carriers that
both formats populate.

Usage:
    python3 ted_matrix.py [-o ted_matrix.csv] [--normalized ted_matrix_norm.csv]
"""
import argparse
import csv
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tree_edit_distance import (indel_cost, parse_plan, postorder,
                                substitution_cost, tree_edit_distance)

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_NEW_LQP = os.path.join(HERE, "plans", "lqp")
DEFAULT_BASE_PQP = os.path.join(
    HERE, os.pardir, "saved_data", "sf100_plans", "pqp")


def collect(directory, prefix):
    """{query id -> path} for <prefix>_q<id>.tree.json in `directory`."""
    found = {}
    pattern = re.compile(rf"^{prefix}_q(\w+)\.tree\.json$")
    for name in os.listdir(directory):
        match = pattern.match(name)
        if match:
            found[match.group(1)] = os.path.join(directory, name)
    return found


def sort_key(query_id):
    return (0, int(query_id)) if query_id.isdigit() else (1, query_id)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new-lqp", default=DEFAULT_NEW_LQP)
    ap.add_argument("--base-pqp", default=DEFAULT_BASE_PQP)
    ap.add_argument("--out", "-o", default=os.path.join(HERE, "ted_matrix.csv"))
    ap.add_argument("--normalized", default=None,
                    help="Also write a matrix of TED divided by the cost of "
                         "deleting all of one tree and inserting all of the "
                         "other, which is what makes a single threshold "
                         "comparable across queries of different sizes.")
    args = ap.parse_args()

    new_plans = collect(args.new_lqp, "lqp")
    base_plans = collect(args.base_pqp, "pqp")
    if not new_plans:
        sys.exit(f"no lqp_q*.tree.json under {args.new_lqp}")
    if not base_plans:
        sys.exit(f"no pqp_q*.tree.json under {args.base_pqp}")

    rows = sorted(new_plans, key=sort_key)
    cols = sorted(base_plans, key=sort_key)

    def load(path):
        tree = parse_plan(json.load(open(path)), "shared", None)
        nodes, _ = postorder(tree)
        return tree, sum(indel_cost(n, False) for n in nodes)

    base_trees = {c: load(base_plans[c]) for c in cols}

    raw, normalized = {}, {}
    for r in rows:
        tree_r, weight_r = load(new_plans[r])
        raw[r], normalized[r] = {}, {}
        for c in cols:
            tree_c, weight_c = base_trees[c]
            distance = tree_edit_distance(
                tree_r, tree_c, lambda n: indel_cost(n, False), substitution_cost)
            raw[r][c] = distance
            normalized[r][c] = distance / (weight_r + weight_c)

    def write(path, table, fmt):
        with open(path, "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["query_id"] + cols)
            for r in rows:
                writer.writerow([r] + [fmt(table[r][c]) for c in cols])
        print(f"wrote {path}  ({len(rows)} rows x {len(cols)} columns)")

    write(args.out, raw, lambda v: v)
    if args.normalized:
        write(args.normalized, normalized, lambda v: f"{v:.4f}")

    print(f"\n{'query':>6} {'base':>5} {'nearest':>8} {'TED':>5} {'norm':>6}   runner-up")
    for r in rows:
        base = r[:-1].lstrip("0") or r[:-1]
        order = sorted(cols, key=lambda c: (raw[r][c], sort_key(c)))
        best, second = order[0], order[1]
        flag = "" if best == base else "   <-- not its own base"
        print(f"{r:>6} {base:>5} {best:>8} {raw[r][best]:>5} "
              f"{normalized[r][best]:>6.3f}   {second}:{raw[r][second]}{flag}")


if __name__ == "__main__":
    main()
