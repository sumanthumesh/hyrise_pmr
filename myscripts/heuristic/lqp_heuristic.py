"""
LQP-driven tableDAM / regDAM split heuristic (F1 -- size-ratio).

Sibling of pqp_heuristic.py -- same F1 formula, same H2 column selection, but
consumes a LOGICAL query plan (lqp_qN.tree.json) emitted by Hyrise's LQPVisualizer.

Given
  * an LQP JSON, and
  * a per-column encoded-size map (column_sizes_sfN.dat, JSON of column -> bytes),
recommend how a fixed DAM budget should be split between:
  * tableDAM : fast memory holding a subset of base-table columns pinned before execution
  * regDAM   : fast memory left over for the query's short-lived temp allocations

Strategy (F1)
-------------
    table_bytes = sum of encoded sizes of every referenced base column
    temp_bytes  = path-max estimate of peak transient allocations
                  (peak(subtree) = own_temp(node) + max(peak(left), peak(right)))
    tableDAM    = DAM * table_bytes / (table_bytes + temp_bytes)
    regDAM      = DAM - tableDAM

If --cap is passed, tableDAM is additionally clipped to DAM/2.

Two differences from the PQP version:
  1. Card parser accepts LQP's float-with-suffix form ("297067150.2 row(s) est.").
     LQP cardinalities are estimator output, not actuals -- expect noisier estimates,
     especially on multi-join and large-group-by queries.
  2. Join has no build_side field (that's a physical-planner decision). The
     smaller-of-{left_card, right_card} is picked as build (Hyrise's convention),
     and Semi / AntiNullAsTrue / AntiNullAsFalse joins use a 0.6x multiplier on the
     probe-side term because semi/anti joins don't fully materialize the probe side
     into their output.

Column selection (Heuristic 2)
------------------------------
Identical to the PQP version:

    net_ns(col) = access_rows(col) * K_NS_PER_ACCESS
                  - size_gb(col)   * MIG_NS_PER_GB

Positive-net columns only, greedy-packed. Unused budget rolls back into regDAM.

Usage
-----
    python3 lqp_heuristic.py <lqp_plan.json> <column_sizes.json> --dam 12.5 [--cap] [--json] [--concise]
"""

import argparse
import json
import os
import re
import sys
from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GB = 2 ** 30

# Column-access weights: how much memory traffic each usage mode spends per row.
W_PROJECT   = 1     # column referenced by a StoredTable's used_columns
W_PREDICATE = 3     # scan predicate column -- walked over full input cardinality
W_JOIN_KEY  = 2     # join key -- read on both sides at post-filter cardinality

# Per-operator temp-memory constants (same values as the PQP fit).
BYTES_PER_TEMP_ROW    = 8      # RowIDPosList entry (scan matches)
JOIN_BUILD_ROW_BYTES  = 24     # hash-table entry on the build side
JOIN_PROBE_ROW_BYTES  = 16     # probe-side materialization buffer
AGG_ENTRY_BYTES       = 24     # hash-table entry per aggregate output group
AGG_INPUT_COLS_AVG    = 6      # avg columns materialized as aggregate inputs (empirical, TPC-H)
AGG_INPUT_ROW_BYTES   = AGG_INPUT_COLS_AVG * 8

# Semi / anti joins don't fully materialize the probe side; scale probe-side term down.
SEMI_PROBE_MULTIPLIER = 0.6
SEMI_ANTI_MODES       = {"Semi", "AntiNullAsTrue", "AntiNullAsFalse"}

# Safety cap on tableDAM budget when --cap is set.
MAX_TABLEDAM_FRAC   = 0.5

# Migration awareness (Heuristic 2).
MIG_NS_PER_GB       = 3.0e7   # median CRAM->DAM migration cost in ns per GB
K_NS_PER_ACCESS     = 0.1     # per-access latency saved by pinning a column


# ---------------------------------------------------------------------------
# Plan walking / feature extraction
# ---------------------------------------------------------------------------

# LQP LeftCard/RightCard look like "297067150.2 row(s) est." (float, may have decimal).
_CARD_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s+row")

# Only storage columns follow the "<table_prefix>_<name>" pattern.
_STORAGE_COL_RE = re.compile(r"^[a-z]+_")


def parse_card(card_str: Any) -> int:
    """Convert '297067150.2 row(s) est.' -> 297067150. Returns 0 if malformed."""
    if not isinstance(card_str, str):
        return 0
    m = _CARD_RE.search(card_str)
    return int(float(m.group(1).replace(",", ""))) if m else 0


def collect_operators(node: dict, out: list, output_card: int = 0) -> None:
    """
    Walk the LQP tree, appending one dict per node with the fields the heuristic uses.

    output_card = the cardinality this node PRODUCED (parent's Left/RightCard for this child).
    Threaded top-down because the JSON stores each edge's cardinality on the parent.
    """
    # Diamond back-references contain only {id, name, shared_ref: true}; skip.
    if not isinstance(node, dict) or node.get("shared_ref"):
        return

    left_card  = parse_card(node.get("LeftCard"))
    right_card = parse_card(node.get("RightCard"))

    out.append({
        "name":            node.get("name"),
        "left_card":       left_card,
        "right_card":      right_card,
        "output_card":     output_card,
        "used_columns":    node.get("used_columns", []),
        "predicate":       node.get("predicate"),
        "join_predicate":  node.get("join_predicate"),
        "join_mode":       node.get("join_mode"),
        "description":     node.get("description", ""),
    })

    left = node.get("Leftchildren")
    right = node.get("Rightchildren")
    if isinstance(left, dict):
        collect_operators(left, out, left_card)
    if isinstance(right, dict):
        collect_operators(right, out, right_card)


def compute_column_hotness(operators: list, col_sizes: dict) -> dict:
    """
    Return {column: {access_rows, size_bytes, hotness}} for every referenced storage column.

    hotness = access_rows / size_bytes  ("cost of not pinning" / "cost of pinning").
    Columns not present in col_sizes or with unknown sizes are dropped.
    """
    access: dict = {}
    for op in operators:
        name = op["name"]
        if name == "StoredTable":
            for column in op["used_columns"]:
                access[column] = access.get(column, 0) + op["output_card"] * W_PROJECT
        elif name == "Predicate":
            predicate = op["predicate"] or {}
            column = predicate.get("column")
            if column and _STORAGE_COL_RE.match(column):
                access[column] = access.get(column, 0) + op["left_card"] * W_PREDICATE
        elif name == "Join":
            jp = op["join_predicate"] or {}
            for c_key, card in (("left_column",  op["left_card"]),
                                ("right_column", op["right_card"])):
                c = jp.get(c_key)
                if c and _STORAGE_COL_RE.match(c):
                    access[c] = access.get(c, 0) + card * W_JOIN_KEY

    hot = {}
    for column, access_rows in access.items():
        size_bytes = col_sizes.get(column)
        if size_bytes is None or size_bytes == 0:
            continue
        hot[column] = {
            "access_rows": access_rows,
            "size_bytes":  size_bytes,
            "hotness":     access_rows / size_bytes,
        }
    return hot


def own_temp_bytes(name: str, left_card: int, right_card: int,
                   output_card: int, join_mode: str | None) -> int:
    """Per-operator temp footprint at its own execution peak, ignoring ancestors."""
    if name == "Join":
        # No build_side hint in LQP -- assume the smaller side is build (Hyrise convention).
        if left_card <= right_card:
            build, probe = left_card, right_card
        else:
            build, probe = right_card, left_card
        probe_mult = SEMI_PROBE_MULTIPLIER if join_mode in SEMI_ANTI_MODES else 1.0
        return build * JOIN_BUILD_ROW_BYTES + int(probe * JOIN_PROBE_ROW_BYTES * probe_mult)
    if name == "Predicate":
        return output_card * BYTES_PER_TEMP_ROW
    if name == "Aggregate":
        return output_card * AGG_ENTRY_BYTES + left_card * AGG_INPUT_ROW_BYTES
    if name in ("StoredTable", "Validate", "Alias", "Union"):
        return 0
    return output_card * BYTES_PER_TEMP_ROW


def estimate_peak_temp_bytes(node: dict, output_card: int = 0) -> int:
    """
    Recursive path-max walk (identical rule to the PQP version):
        peak(subtree) = own_temp(node) + max(peak(left), peak(right))
    """
    if not isinstance(node, dict) or node.get("shared_ref"):
        return 0

    name       = node.get("name")
    left_card  = parse_card(node.get("LeftCard"))
    right_card = parse_card(node.get("RightCard"))
    join_mode  = node.get("join_mode")

    own = own_temp_bytes(name, left_card, right_card, output_card, join_mode)

    left  = node.get("Leftchildren")
    right = node.get("Rightchildren")
    left_peak  = estimate_peak_temp_bytes(left,  left_card)  if isinstance(left,  dict) else 0
    right_peak = estimate_peak_temp_bytes(right, right_card) if isinstance(right, dict) else 0

    return own + max(left_peak, right_peak)


# ---------------------------------------------------------------------------
# Heuristic 1: F1 size-ratio DAM split
# ---------------------------------------------------------------------------
def split_dam(plan: dict, col_sizes: dict, dam_gb: float,
              cap: bool = False,
              measured_peak_bytes: int | None = None) -> dict:
    """
    F1 size-ratio DAM split.

        table_bytes = sum of sizes of every referenced base column
        temp_bytes  = path-max estimate of peak transient allocations
        tableDAM    = DAM * table_bytes / (table_bytes + temp_bytes)
        regDAM      = DAM - tableDAM

    If cap=True, tableDAM is additionally clipped to DAM * MAX_TABLEDAM_FRAC.
    """
    ops: list = []
    collect_operators(plan, ops)
    hot = compute_column_hotness(ops, col_sizes)
    table_bytes = sum(info["size_bytes"] for info in hot.values())

    if measured_peak_bytes is not None:
        temp_bytes = measured_peak_bytes
        temp_source = "measured"
    else:
        temp_bytes = estimate_peak_temp_bytes(plan)
        temp_source = "plan_estimate"

    denom = table_bytes + temp_bytes
    if denom == 0:
        return {
            "tabledam_budget_gb": 0.0,
            "regdam_initial_gb":  dam_gb,
            "temp_estimate_gb":   0.0,
            "temp_source":        temp_source,
            "table_bytes_gb":     0.0,
            "size_ratio":         0.0,
            "cap_active":         False,
        }

    ratio = table_bytes / denom
    raw_tab = dam_gb * ratio
    if cap:
        capped = min(raw_tab, MAX_TABLEDAM_FRAC * dam_gb)
        cap_active = capped < raw_tab
        tab_gb = capped
    else:
        cap_active = False
        tab_gb = raw_tab

    return {
        "tabledam_budget_gb": tab_gb,
        "regdam_initial_gb":  dam_gb - tab_gb,
        "temp_estimate_gb":   temp_bytes / GB,
        "temp_source":        temp_source,
        "table_bytes_gb":     table_bytes / GB,
        "size_ratio":         ratio,
        "cap_active":         cap_active,
    }


# ---------------------------------------------------------------------------
# Heuristic 2: migration-aware column selection
# ---------------------------------------------------------------------------
def select_columns(plan: dict, col_sizes: dict, tabledam_budget_gb: float,
                   mig_ns_per_gb: float = MIG_NS_PER_GB,
                   k_ns_per_access: float = K_NS_PER_ACCESS) -> dict:
    """
    Greedy pick columns whose predicted per-access speedup outweighs migration cost.

    net_ns(col) = access_rows(col) * k_ns_per_access - size_gb(col) * mig_ns_per_gb

    Only columns with net_ns > 0 are candidates; among candidates, packed greedily
    by descending net_ns until tabledam_budget_gb runs out.
    """
    if tabledam_budget_gb <= 0:
        return {"actual_tabledam_gb": 0.0, "columns_to_pin": [], "column_details": []}

    operators: list = []
    collect_operators(plan, operators)
    hot = compute_column_hotness(operators, col_sizes)

    scored = []
    for column, info in hot.items():
        sz_gb      = info["size_bytes"] / GB
        benefit_ns = info["access_rows"] * k_ns_per_access
        cost_ns    = sz_gb * mig_ns_per_gb
        scored.append((column, info, sz_gb, benefit_ns, cost_ns, benefit_ns - cost_ns))
    scored.sort(key=lambda t: -t[5])

    picked_cols = []
    picked_details = []
    total_gb = 0.0
    for column, info, sz_gb, benefit_ns, cost_ns, net_ns in scored:
        if net_ns <= 0:
            continue
        if total_gb + sz_gb > tabledam_budget_gb:
            continue
        picked_cols.append(column)
        picked_details.append({
            "column":     column,
            "size_gb":    sz_gb,
            "hotness":    info["hotness"],
            "benefit_ns": benefit_ns,
            "cost_ns":    cost_ns,
            "net_ns":     net_ns,
        })
        total_gb += sz_gb

    return {
        "actual_tabledam_gb":  total_gb,
        "columns_to_pin":      picked_cols,
        "column_details":      picked_details,
    }


def recommend_split(plan: dict, col_sizes: dict, dam_gb: float,
                    cap: bool = False,
                    measured_peak_bytes: int | None = None) -> dict:
    """
    Compose H1 (split_dam) + H2 (select_columns). Any budget H2 doesn't consume
    rolls back into regDAM.
    """
    split = split_dam(plan, col_sizes, dam_gb, cap=cap,
                      measured_peak_bytes=measured_peak_bytes)
    budget = split["tabledam_budget_gb"]
    pick = select_columns(plan, col_sizes, budget)
    unused = budget - pick["actual_tabledam_gb"]
    return {
        "tabledam_gb":        pick["actual_tabledam_gb"],
        "regdam_gb":          split["regdam_initial_gb"] + unused,
        "tabledam_budget_gb": budget,
        "temp_estimate_gb":   split["temp_estimate_gb"],
        "temp_source":        split["temp_source"],
        "table_bytes_gb":     split["table_bytes_gb"],
        "size_ratio":         split["size_ratio"],
        "cap_active":         split["cap_active"],
        "columns_to_pin":     pick["columns_to_pin"],
        "column_details":     pick["column_details"],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("plan",      help="Path to lqp_qN.tree.json")
    parser.add_argument("col_sizes", help="Path to column_sizes.json (map: column -> bytes)")
    parser.add_argument("--dam", type=float, required=True, help="Total DAM in GB")
    parser.add_argument("--cap", action="store_true",
                        help="Clip tableDAM to DAM/2 (default: uncapped size-ratio)")
    parser.add_argument("--measured-peak", type=int, default=None,
                        help="Override plan-derived temp estimate with this measured value (bytes)")
    parser.add_argument("--json", action="store_true",
                        help="Machine-readable JSON output only")
    parser.add_argument("--concise", "-c", action="store_true",
                        help="Print only 'tableDAM_gb,regDAM_gb' (nothing else)")
    args = parser.parse_args()

    with open(args.plan) as f:
        plan = json.load(f)
    with open(args.col_sizes) as f:
        col_sizes = {k: int(v) for k, v in json.load(f).items()}

    result = recommend_split(plan, col_sizes, args.dam,
                             cap=args.cap,
                             measured_peak_bytes=args.measured_peak)

    if args.json:
        print(json.dumps(result, indent=2))
        return

    if args.concise:
        print(f"{result['tabledam_gb']:.3f},{result['regdam_gb']:.3f}")
        return

    dam = args.dam
    tb = result["table_bytes_gb"]
    tp = result["temp_estimate_gb"]
    print(f"DAM budget        : {dam:.3f} GB")
    print(f"table_bytes       : {tb:.3f} GB")
    print(f"temp_bytes        : {tp:.3f} GB   (via {result['temp_source']})")
    print(f"size_ratio        : {result['size_ratio']:.3f}   "
          f"(cap={'on' if args.cap else 'off'}"
          f"{', clipped' if result['cap_active'] else ''})")
    print(f"H1 tableDAM budget: {result['tabledam_budget_gb']:.3f} GB")
    print(f"H2 tableDAM used  : {result['tabledam_gb']:.3f} GB "
          f"({result['tabledam_gb']/dam*100:.1f} % of DAM)   "
          f"unused rolled back: {result['tabledam_budget_gb']-result['tabledam_gb']:.3f} GB")
    print(f"regDAM final      : {result['regdam_gb']:.3f} GB")
    print(f"columns to pin    : {len(result['columns_to_pin'])}")
    if result["column_details"]:
        print(f"\n{'column':30s} {'size_gb':>10s}   {'hotness':>14s}")
        for d in result["column_details"]:
            print(f"{d['column']:30s} {d['size_gb']:>10.3f}   {d['hotness']:>14.2e}")


if __name__ == "__main__":
    main()
