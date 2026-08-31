"""
Tree edit distance between two Hyrise query plans.

Reads the *.tree.json plans emitted for both planner stages -- pre-execution
(LQP, sf100_plans/lqp) and post-execution (PQP, sf100_plans/pqp) -- and
computes the TED between them. Either file may be either format, so an LQP can
be compared against a cached PQP.

Each node is encoded as a vector of N+1 elements, following the estimator's
definition: the first N are indicators over the schema's columns (1 where the
node makes non-zero accesses to that column) and the last is the operator type.
The substitution cost between two nodes is the Hamming distance between their
vectors; insertion and deletion cost the Hamming distance from the zero vector,
so that indel and substitution stay on the same scale. The distance itself is
computed with Zhang-Shasha.

Note that the choice of N does not affect the result: a column absent from both
nodes contributes 0 to every Hamming distance. The schema file is therefore
optional and only ever used to constrain which identifiers in a node's
description count as column references.

Usage:
    python3 tree_edit_distance.py <plan_a.json> <plan_b.json> [-v]
    python3 tree_edit_distance.py lqp/lqp_q6.tree.json pqp/pqp_q6.tree.json
"""
import argparse
import json
import os
import re
import sys

CHILD_KEYS = ("Leftchildren", "Rightchildren")

# PQP and LQP name the same logical operator differently (GetTable/StoredTable,
# TableScan/Predicate, JoinHash/Join, UnionAll+UnionPositions/Union). Matching
# on the raw name would make every node of a PQP differ from its LQP
# counterpart, so names are folded to a common vocabulary by prefix. Order
# matters only in that the first matching prefix wins.
OP_PREFIXES = (
    ("aggregate", "AGGREGATE"),
    ("gettable", "GET_TABLE"),
    ("storedtable", "GET_TABLE"),
    ("tablescan", "PREDICATE"),
    ("predicate", "PREDICATE"),
    ("join", "JOIN"),
    ("union", "UNION"),
    ("projection", "PROJECTION"),
    ("sort", "SORT"),
    ("alias", "ALIAS"),
    ("validate", "VALIDATE"),
    ("limit", "LIMIT"),
    ("difference", "DIFFERENCE"),
    ("import", "IMPORT"),
    ("export", "EXPORT"),
)

# TPC-H style identifiers: a one- or two-letter table prefix, then the name.
IDENT_RE = re.compile(r"\b[a-z]{1,2}_[a-z_]+\b")


def canonical_op(name):
    low = (name or "").lower()
    for prefix, canon in OP_PREFIXES:
        if low.startswith(prefix):
            return canon
    return low.upper() or "UNKNOWN"


class Node:
    __slots__ = ("op", "cols", "children", "raw_name")

    def __init__(self, op, cols, children, raw_name):
        self.op = op
        self.cols = cols            # frozenset of column names
        self.children = children
        self.raw_name = raw_name

    def __repr__(self):
        cols = ",".join(sorted(self.cols))
        return f"{self.op}({cols})"


def node_columns(node, mode, vocabulary):
    """Columns this plan node touches.

    `mode` controls which of the JSON's several column carriers are trusted:

      shared -- `used_columns` (on leaf table nodes) and `predicate.column`.
                These are the only carriers populated in BOTH formats, so this
                is the honest choice when comparing an LQP against a PQP.
      all    -- additionally `columns`/`columns_left`/`columns_right` and any
                schema identifier appearing in `description`. Richer, but PQP
                leaves `columns` empty and prints bare "Projection"/"Sort"
                descriptions where the LQP lists the expressions, so the two
                formats are no longer encoded symmetrically.
    """
    cols = set()
    for name in node.get("used_columns") or ():
        cols.add(name)
    predicate = node.get("predicate")
    if isinstance(predicate, dict) and predicate.get("column"):
        cols.add(predicate["column"])

    if mode == "all":
        for key in ("columns", "columns_left", "columns_right"):
            for name in node.get(key) or ():
                cols.add(name)
        description = node.get("description") or ""
        for candidate in IDENT_RE.findall(description):
            if vocabulary is None or candidate in vocabulary:
                cols.add(candidate)
    return frozenset(cols)


def parse_plan(node, mode, vocabulary):
    """Convert one plan JSON dict into a Node tree.

    Children hang off "Leftchildren"/"Rightchildren" and are kept in that order,
    since Zhang-Shasha operates on ordered trees. Nodes carrying
    `shared_ref: true` are stubs for a subtree that appears more than once in
    the plan DAG; they hold only an id and a name, and are treated as leaves.
    """
    children = [parse_plan(node[key], mode, vocabulary)
                for key in CHILD_KEYS if isinstance(node.get(key), dict)]
    return Node(canonical_op(node.get("name")),
                node_columns(node, mode, vocabulary),
                children,
                node.get("name"))


def detect_format(node):
    """'pqp' or 'lqp', inferred from markers only one of the two ever carries."""
    found = set()

    def walk(n):
        if "walltime_ns" in n or n.get("name") in ("GetTable", "TableScan", "AggregateHash"):
            found.add("pqp")
        if n.get("name") in ("StoredTable", "Predicate", "Aggregate"):
            found.add("lqp")
        for key in CHILD_KEYS:
            if isinstance(n.get(key), dict):
                walk(n[key])

    walk(node)
    if len(found) == 1:
        return found.pop()
    return "pqp" if "pqp" in found else "unknown"


# --- costs -------------------------------------------------------------------

def substitution_cost(a, b):
    """Hamming distance between the two N+1 vectors.

    Columns held by exactly one of the nodes each flip one indicator; the
    operator element contributes 1 when the types differ.
    """
    return len(a.cols ^ b.cols) + (0 if a.op == b.op else 1)


def indel_cost(node, unit):
    """Cost of inserting or deleting a node.

    By default this is the node's Hamming distance from the zero vector, which
    keeps indel on the same scale as substitution. With unit costs, deleting
    and re-inserting a node is cheaper than substituting it whenever the two
    differ in more than two elements, which collapses the measure.
    """
    return 1 if unit else len(node.cols) + 1


# --- Zhang-Shasha ------------------------------------------------------------

def postorder(root):
    """Nodes in postorder, plus each node's leftmost-leaf-descendant index."""
    nodes, lmd = [], []

    def visit(node):
        first = None
        for child in node.children:
            index = visit(child)
            if first is None:
                first = index
        nodes.append(node)
        position = len(nodes) - 1
        lmd.append(position if first is None else first)
        return lmd[position]

    visit(root)
    return nodes, lmd


def keyroots(lmd):
    """The rightmost node for each distinct leftmost-leaf descendant."""
    last = {}
    for index, leftmost in enumerate(lmd):
        last[leftmost] = index
    return sorted(last.values())


def tree_edit_distance(root_a, root_b, cost_indel, cost_sub):
    nodes_a, lmd_a = postorder(root_a)
    nodes_b, lmd_b = postorder(root_b)
    size_a, size_b = len(nodes_a), len(nodes_b)
    tree_dist = [[0] * size_b for _ in range(size_a)]

    for i in keyroots(lmd_a):
        for j in keyroots(lmd_b):
            base_i, base_j = lmd_a[i], lmd_b[j]
            rows, cols = i - base_i + 2, j - base_j + 2
            forest = [[0] * cols for _ in range(rows)]
            for x in range(1, rows):
                forest[x][0] = forest[x - 1][0] + cost_indel(nodes_a[base_i + x - 1])
            for y in range(1, cols):
                forest[0][y] = forest[0][y - 1] + cost_indel(nodes_b[base_j + y - 1])
            for x in range(1, rows):
                for y in range(1, cols):
                    ai, bj = base_i + x - 1, base_j + y - 1
                    delete = forest[x - 1][y] + cost_indel(nodes_a[ai])
                    insert = forest[x][y - 1] + cost_indel(nodes_b[bj])
                    if lmd_a[ai] == base_i and lmd_b[bj] == base_j:
                        # Both are whole subtrees: this cell is itself a tree
                        # distance, so record it for later keyroot pairs.
                        forest[x][y] = min(delete, insert,
                                           forest[x - 1][y - 1] + cost_sub(nodes_a[ai], nodes_b[bj]))
                        tree_dist[ai][bj] = forest[x][y]
                    else:
                        forest[x][y] = min(delete, insert,
                                           forest[lmd_a[ai] - base_i][lmd_b[bj] - base_j]
                                           + tree_dist[ai][bj])
    return tree_dist[size_a - 1][size_b - 1]


# --- driver ------------------------------------------------------------------

def load_vocabulary(path):
    if path is None:
        return None
    with open(path) as handle:
        return set(json.load(handle).keys())


def render(node, depth=0, lines=None):
    lines = [] if lines is None else lines
    lines.append(f"{'  ' * depth}{node.op:<10s} {node.raw_name or '':<16s} "
                 f"{{{', '.join(sorted(node.cols))}}}")
    for child in node.children:
        render(child, depth + 1, lines)
    return lines


def main():
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("plan_a")
    ap.add_argument("plan_b")
    ap.add_argument("--columns", choices=("auto", "shared", "all"), default="auto",
                    help="Which column carriers to trust. 'auto' (default) uses "
                         "'shared' when the two plans are different formats and "
                         "'all' when they match.")
    ap.add_argument("--schema", default=None,
                    help="JSON file whose keys are the schema's column names "
                         "(e.g. column_sizes_sf100.dat). Only constrains "
                         "description parsing under --columns all.")
    ap.add_argument("--unit-indel", action="store_true",
                    help="Cost insert/delete at 1 instead of the node's Hamming "
                         "weight. Not recommended; see indel_cost().")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Print the encoded trees.")
    args = ap.parse_args()

    raw_a = json.load(open(args.plan_a))
    raw_b = json.load(open(args.plan_b))
    fmt_a, fmt_b = detect_format(raw_a), detect_format(raw_b)

    mode = args.columns
    if mode == "auto":
        mode = "shared" if fmt_a != fmt_b else "all"

    vocabulary = load_vocabulary(args.schema)
    tree_a = parse_plan(raw_a, mode, vocabulary)
    tree_b = parse_plan(raw_b, mode, vocabulary)

    def cost_indel(node):
        return indel_cost(node, args.unit_indel)

    distance = tree_edit_distance(tree_a, tree_b, cost_indel, substitution_cost)

    nodes_a, _ = postorder(tree_a)
    nodes_b, _ = postorder(tree_b)
    # Deleting all of A and inserting all of B is always a valid edit script,
    # so this is an upper bound and the ratio lands in [0, 1].
    worst = sum(cost_indel(n) for n in nodes_a) + sum(cost_indel(n) for n in nodes_b)

    if args.verbose:
        print(f"--- {args.plan_a} ({fmt_a}, {len(nodes_a)} nodes)")
        print("\n".join(render(tree_a)))
        print(f"--- {args.plan_b} ({fmt_b}, {len(nodes_b)} nodes)")
        print("\n".join(render(tree_b)))
        print("---")

    print(f"plan A : {args.plan_a}  [{fmt_a}, {len(nodes_a)} nodes]")
    print(f"plan B : {args.plan_b}  [{fmt_b}, {len(nodes_b)} nodes]")
    print(f"columns: {mode}")
    print(f"TED    : {distance}")
    print(f"normalized: {distance / worst:.4f}  (TED / {worst})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
