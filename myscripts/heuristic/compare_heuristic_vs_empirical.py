"""
Compare the F1 (size-ratio) heuristic against actual experiment data.

For every (query_id, DAM) combo in the input CSV, computes four heuristic
predictions of (tableDAM, regDAM):

  * pqp_cap    -- PQP heuristic with the DAM/2 cap
  * pqp_nocap  -- PQP heuristic, unbounded size ratio
  * lqp_cap    -- LQP heuristic with the DAM/2 cap
  * lqp_nocap  -- LQP heuristic, unbounded size ratio

For each prediction, the closest empirical row in the group is looked up by
matching on dam_size_gb (exact) and dram_resident_size_gb (nearest); the
"heuristic speedup" for that combo is the speedup of that snapped row.

The actual best speedup in the group and its tableDAM/regDAM split are also
recorded. Baseline is the (dam_size_gb='local', table_size_gb='remote') row
for the query; speedup = baseline_total_duration / current_total_duration.

Handles both raw_result.csv schemas:
  * old: total_duration = duration + migration_duration_ns  (computed here)
  * new: total_duration column already present

Usage:
    python3 compare_heuristic_vs_empirical.py <raw_result.csv> [--out ...] [--pqp-plans ...] [--lqp-plans ...] [--col-sizes ...]
"""
import argparse
import csv
import json
import os
import sys
from collections import defaultdict

# Sibling imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pqp_heuristic as PQP
import lqp_heuristic as LQP


DEFAULT_PQP_PLANS = "/data1/sumanthu/hyrise_pmr/myscripts/saved_data/sf50_plans/pqp"
DEFAULT_LQP_PLANS = "/data1/sumanthu/hyrise_pmr/myscripts/saved_data/sf50_plans/lqp"
DEFAULT_COL_SIZES = "/data1/sumanthu/hyrise_pmr/myscripts/saved_data/column_sizes_sf50.dat"


def compute_total(row):
    """Handle both CSV schemas."""
    if "total_duration" in row and row["total_duration"] != "":
        return float(row["total_duration"])
    dur = row.get("query_execution_duration") or row.get("duration")
    return float(dur) + float(row["migration_duration_ns"])


def snap_by_dram_resident(group, tab_pred_gb):
    """Row in `group` whose dram_resident_size_gb is closest to tab_pred_gb."""
    return min(group, key=lambda r: abs(float(r["dram_resident_size_gb"]) - tab_pred_gb))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("raw_csv", help="Path to raw_result.csv")
    ap.add_argument("--out",         default=None,
                    help="Output CSV path (default: <raw_dir>/heuristic_comparison.csv)")
    ap.add_argument("--pqp-plans",   default=DEFAULT_PQP_PLANS)
    ap.add_argument("--lqp-plans",   default=DEFAULT_LQP_PLANS)
    ap.add_argument("--col-sizes",   default=DEFAULT_COL_SIZES)
    args = ap.parse_args()

    if args.out is None:
        args.out = os.path.join(os.path.dirname(os.path.abspath(args.raw_csv)),
                                "heuristic_comparison.csv")

    col_sizes = {k: int(v) for k, v in json.load(open(args.col_sizes)).items()}

    rows = list(csv.DictReader(open(args.raw_csv)))
    for r in rows:
        r["total"] = compute_total(r)

    baseline = {int(r["query_id"]): r["total"]
                for r in rows
                if r["dam_size_gb"] == "local" and r["table_size_gb"] == "remote"}

    groups = defaultdict(list)
    for r in rows:
        try:
            dam = float(r["dam_size_gb"])
        except ValueError:
            continue
        groups[(int(r["query_id"]), dam)].append(r)

    # Cache plan JSONs so we don't re-open the same file per DAM.
    plan_cache = {}
    def load_plan(planner, q):
        key = (planner, q)
        if key in plan_cache:
            return plan_cache[key]
        base_dir = args.pqp_plans if planner == "pqp" else args.lqp_plans
        path = os.path.join(base_dir, f"{planner}_q{q}.tree.json")
        plan = json.load(open(path)) if os.path.exists(path) else None
        plan_cache[key] = plan
        return plan

    HEURISTICS = [
        ("pqp_cap",   "pqp", True,  PQP),
        ("pqp_nocap", "pqp", False, PQP),
        ("lqp_cap",   "lqp", True,  LQP),
        ("lqp_nocap", "lqp", False, LQP),
    ]

    out_rows = []
    for (q, dam) in sorted(groups.keys()):
        grp = groups[(q, dam)]
        base = baseline.get(q)
        if base is None:
            continue

        row = {"query_id": q, "dam_gb": dam}

        for tag, planner, cap, mod in HEURISTICS:
            plan = load_plan(planner, q)
            if plan is None:
                row[f"{tag}_tab_gb"]           = None
                row[f"{tag}_reg_gb"]           = None
                row[f"{tag}_matched_dram_gb"]  = None
                row[f"{tag}_sp"]               = None
                continue
            rec = mod.recommend_split(plan, col_sizes, dam, cap=cap)
            tab_pred = rec["tabledam_gb"]
            reg_pred = rec["regdam_gb"]
            matched  = snap_by_dram_resident(grp, tab_pred)
            row[f"{tag}_tab_gb"]           = round(tab_pred, 4)
            row[f"{tag}_reg_gb"]           = round(reg_pred, 4)
            row[f"{tag}_matched_dram_gb"]  = round(float(matched["dram_resident_size_gb"]), 4)
            row[f"{tag}_sp"]               = round(base / matched["total"], 4)

        # Empirical best in the group
        best = min(grp, key=lambda r: r["total"])
        best_tab = float(best["dram_resident_size_gb"])
        row["best_tab_gb"] = round(best_tab, 4)
        row["best_reg_gb"] = round(dam - best_tab, 4)
        row["best_sp"]     = round(base / best["total"], 4)

        out_rows.append(row)

    if not out_rows:
        print("no rows produced -- check plan paths and CSV format", file=sys.stderr)
        return

    # Column ordering: (query, dam), then each heuristic's block, then best.
    fields = ["query_id", "dam_gb"]
    for tag, *_ in HEURISTICS:
        fields += [f"{tag}_tab_gb", f"{tag}_reg_gb", f"{tag}_matched_dram_gb", f"{tag}_sp"]
    fields += ["best_tab_gb", "best_reg_gb", "best_sp"]

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)

    print(f"wrote {len(out_rows)} rows to {args.out}")

    # Summary aggregates
    print()
    print(f"{'heuristic':12s}  {'mean_heur_sp':>13s}  {'mean_best_sp':>13s}  "
          f"{'h/best':>7s}  {'exact':>7s}  {'<=0.03':>7s}  {'TW_capture':>11s}")
    for tag, *_ in HEURISTICS:
        vals = [r for r in out_rows if r[f"{tag}_sp"] is not None]
        n = len(vals)
        if n == 0: continue
        h_mean = sum(r[f"{tag}_sp"] for r in vals) / n
        b_mean = sum(r["best_sp"]   for r in vals) / n
        exact = sum(1 for r in vals if abs(r[f"{tag}_sp"] - r["best_sp"]) < 1e-4)
        close = sum(1 for r in vals if abs(r[f"{tag}_sp"] - r["best_sp"]) < 0.03)
        # time-weighted capture
        ach = 0.0; real = 0.0
        for r in vals:
            base = baseline[r["query_id"]]
            heur_total = base / r[f"{tag}_sp"]
            best_total = base / r["best_sp"]
            ach  += max(base - best_total, 0)
            real += base - heur_total
        tw = real / ach * 100 if ach > 0 else 0
        print(f"{tag:12s}  {h_mean:>13.4f}  {b_mean:>13.4f}  {h_mean/b_mean:>7.4f}  "
              f"{exact:>4d}/{n}  {close:>4d}/{n}  {tw:>10.1f}%")


if __name__ == "__main__":
    main()
