import pandas as pd
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process consolidated data to find best dam/table combo per query")
    parser.add_argument("--input", "-i", help="Input CSV file (default: consolidated_data.csv)")
    parser.add_argument("--output", "-o", default="per_query_min_duration.csv", help="Output CSV file for per-query min duration (default: per_query_min_duration.csv)")
    parser.add_argument("--migration", "-m", default=None,
                        help="Migration CSV (columns: query_id, dam_size_gb, table_size_gb, "
                             "migration_size_gb, migration_duration_ns). When provided, migration "
                             "cost is added to execution duration before finding the best combo, "
                             "AND migration_size_gb is exposed as dram_resident_size_gb in the "
                             "best-combo and raw outputs.")
    args = parser.parse_args()

    INPUT_CSV  = args.input
    OUTPUT_CSV = args.output
    MIGRATION_CSV = args.migration

    # ---------------------------------------------------------------------------
    # Load and reduce to one row per (query_id, dam_size, table_size) by taking
    # the minimum duration across the 10 repetitions.
    # ---------------------------------------------------------------------------
    df = pd.read_csv(INPUT_CSV, dtype={"dam_size": str, "table_size": str})

    # Mean over repetitions of duration; also carry total_allocated_bytes_delta_pool3
    # (regDAM churn) through to raw_result.csv when present in the input.
    agg_cols = {"duration": "mean"}
    if "total_allocated_bytes_delta_pool3" in df.columns:
        agg_cols["total_allocated_bytes_delta_pool3"] = "mean"
    df_min = (
        df.groupby(["query_id", "dam_size", "table_size"], as_index=False)
        .agg(agg_cols)
    )

    # ---------------------------------------------------------------------------
    # Separate out baseline (local,remote) and ideal (local,local) rows before pivoting.
    # ---------------------------------------------------------------------------
    BASELINE_DAM  = "local"
    BASELINE_TF   = "remote"
    IDEAL_DAM     = "local"
    IDEAL_TF      = "local"

    df_baseline = df_min[(df_min["dam_size"] == BASELINE_DAM) & (df_min["table_size"] == BASELINE_TF)][["query_id", "duration"]].rename(columns={"duration": "baseline"})
    df_ideal    = df_min[(df_min["dam_size"] == IDEAL_DAM)    & (df_min["table_size"] == IDEAL_TF)   ][["query_id", "duration"]].rename(columns={"duration": "ideal"})

    df_rest = df_min[
        ~(
            ((df_min["dam_size"] == BASELINE_DAM) & (df_min["table_size"] == BASELINE_TF)) |
            ((df_min["dam_size"] == IDEAL_DAM)    & (df_min["table_size"] == IDEAL_TF))
        )
    ].copy()

    # Keep only rows where dam_size and table_size are numeric (exclude any other string combos)
    df_rest = df_rest[pd.to_numeric(df_rest["dam_size"], errors="coerce").notna()].copy()
    df_rest["dam_size"]   = df_rest["dam_size"].astype(int)
    df_rest["table_size"] = df_rest["table_size"].astype(int)

    # ---------------------------------------------------------------------------
    # Compute total_duration = query_execution_duration + migration_duration_ns.
    # `duration` from the input CSV is the raw query-execution duration (excluding
    # migration). We keep it un-mutated and add a new `total_duration` column that
    # includes migration cost. All downstream ranking (best combo, pivot) uses
    # total_duration; downstream reporting can expose either.
    # Migration CSV uses GB units; consolidated CSV uses bytes.
    # ---------------------------------------------------------------------------
    if MIGRATION_CSV:
        # Migration CSV columns: query_id, dam_size_gb, table_size_gb,
        # migration_size_gb, migration_duration_ns (produced by extract_migration_mapping_results.py).
        df_mig = pd.read_csv(MIGRATION_CSV)
        # Convert bytes -> GB to match migration CSV keys
        df_rest["dam_size_gb"]   = df_rest["dam_size"]   / (1 << 30)
        df_rest["table_size_gb"] = df_rest["table_size"] / (1 << 30)

        df_rest = df_rest.merge(
            df_mig[["dam_size_gb", "table_size_gb", "query_id", "migration_duration_ns"]],
            on=["dam_size_gb", "table_size_gb", "query_id"],
            how="left"
        )
        missing = df_rest["migration_duration_ns"].isna().sum()
        if missing:
            print(f"WARNING: {missing} (dam_size, table_size, query_id) combos had no matching migration duration — treated as 0.")
        df_rest["migration_duration_ns"] = df_rest["migration_duration_ns"].fillna(0)
        df_rest["total_duration"] = df_rest["duration"] + df_rest["migration_duration_ns"]
        df_rest = df_rest.drop(columns=["dam_size_gb", "table_size_gb"])
    else:
        df_rest["migration_duration_ns"] = 0
        df_rest["total_duration"] = df_rest["duration"]

    # ---------------------------------------------------------------------------
    # Pivot: one row per query_id, columns are (dam_size, table_size) combos
    # ordered by increasing table_size then dam_size.
    # ---------------------------------------------------------------------------
    combo_order = (
        df_rest[["dam_size", "table_size"]]
        .drop_duplicates()
        .sort_values(["table_size", "dam_size"])
    )
    def to_gb(b):
        return f"{int(b) / (1 << 30):g}"

    combo_labels = [f"d{to_gb(r.dam_size)}GB_tf{to_gb(r.table_size)}GB" for _, r in combo_order.iterrows()]

    df_rest["col"] = df_rest.apply(
        lambda r: f"d{to_gb(r.dam_size)}GB_tf{to_gb(r.table_size)}GB", axis=1
    )

    pivot = df_rest.pivot(index="query_id", columns="col", values="total_duration")[combo_labels]
    pivot = pivot.reset_index()

    # Attach baseline and ideal columns
    pivot = pivot.merge(df_baseline, on="query_id", how="left")
    pivot = pivot.merge(df_ideal,    on="query_id", how="left")

    pivot = pivot.sort_values("query_id").reset_index(drop=True)

    pivot.to_csv(OUTPUT_CSV, index=False)
    print(f"Written {OUTPUT_CSV}  ({len(pivot)} rows x {len(pivot.columns)} columns)")

    # ---------------------------------------------------------------------------
    # Second output: best (dam_size, table_size) per query_id, ranked by SPEEDUP
    # (baseline / total_duration). Emitting speedup alongside both duration
    # components lets downstream see how much each combo actually gained relative to
    # the tableDAM=0 baseline.
    # ---------------------------------------------------------------------------
    BEST_CSV = "best_combo_per_query.csv"

    # Attach baseline to every combo row so we can compute speedup, then pick argmax.
    df_rest = df_rest.merge(df_baseline, on="query_id", how="left")
    df_rest["speedup"] = df_rest["baseline"] / df_rest["total_duration"]

    best_idx = df_rest.groupby(["query_id", "dam_size"])["speedup"].idxmax()
    df_best = df_rest.loc[
        best_idx,
        ["query_id", "dam_size", "table_size",
         "duration", "total_duration", "migration_duration_ns", "speedup"],
    ].copy()
    df_best = df_best.rename(columns={"duration": "query_execution_duration"})

    df_best["dam_size_gb"]   = df_best["dam_size"].apply(to_gb)
    df_best["table_size_gb"] = df_best["table_size"].apply(to_gb)
    df_best = df_best[["query_id", "dam_size_gb", "table_size_gb",
                       "query_execution_duration", "migration_duration_ns",
                       "total_duration", "speedup"]]

    df_best = df_best.merge(df_baseline, on="query_id", how="left")
    df_best = df_best.merge(df_ideal,    on="query_id", how="left")

    # Optionally attach dram_resident_size_gb from the migration CSV's migration_size_gb
    # column (same quantity, different name from an earlier producer).
    output_columns = ["query_id", "dam_size_gb", "table_size_gb", "baseline", "ideal",
                      "query_execution_duration", "migration_duration_ns",
                      "total_duration", "speedup"]
    if MIGRATION_CSV:
        df_ms = pd.read_csv(MIGRATION_CSV)
        df_ms = df_ms.rename(columns={"migration_size_gb": "dram_resident_size_gb"})
        df_ms["dam_size_gb"]   = df_ms["dam_size_gb"].apply(lambda v: f"{float(v):g}")
        df_ms["table_size_gb"] = df_ms["table_size_gb"].apply(lambda v: f"{float(v):g}")
        df_best = df_best.merge(
            df_ms[["dam_size_gb", "table_size_gb", "query_id", "dram_resident_size_gb"]],
            on=["dam_size_gb", "table_size_gb", "query_id"],
            how="left",
        )
        missing = df_best["dram_resident_size_gb"].isna().sum()
        if missing:
            print(f"WARNING: {missing} (dam_size, table_size, query_id) combos had no matching dram_resident_size — left as NaN.")
        output_columns.append("dram_resident_size_gb")

    df_best = df_best[output_columns]
    df_best = df_best.sort_values("query_id").reset_index(drop=True)

    df_best.to_csv(BEST_CSV, index=False)
    print(f"Written {BEST_CSV}  ({len(df_best)} rows)")

    # ---------------------------------------------------------------------------
    # Third output: raw per-(query, dam, table) rows with raw execution duration
    # (i.e. duration WITHOUT migration added), plus migration_duration_ns and
    # dram_resident_size_gb as separate columns. Rebuilt from df_min so we don't
    # depend on df_rest's mutated `duration` column.
    # ---------------------------------------------------------------------------
    RAW_CSV = "raw_result.csv"

    # Keep ALL rows including baseline (dam=local, table=remote) and ideal (dam=local,
    # table=local). Numeric byte-values get converted to GB strings; string labels
    # ("local", "remote") pass through unchanged so they align with the join keys.
    df_raw = df_min.copy()
    def size_to_label(v):
        try:
            return to_gb(int(v))
        except (TypeError, ValueError):
            return str(v)
    df_raw["dam_size_gb"]   = df_raw["dam_size"].apply(size_to_label)
    df_raw["table_size_gb"] = df_raw["table_size"].apply(size_to_label)

    if MIGRATION_CSV:
        df_mig3 = pd.read_csv(MIGRATION_CSV)
        df_mig3["dam_size_gb"]   = df_mig3["dam_size_gb"].apply(lambda v: f"{float(v):g}")
        df_mig3["table_size_gb"] = df_mig3["table_size_gb"].apply(lambda v: f"{float(v):g}")
        df_raw = df_raw.merge(
            df_mig3[["dam_size_gb", "table_size_gb", "query_id", "migration_duration_ns"]],
            on=["dam_size_gb", "table_size_gb", "query_id"],
            how="left",
        )
        df_raw["migration_duration_ns"] = df_raw["migration_duration_ns"].fillna(0)
    else:
        df_raw["migration_duration_ns"] = 0

    if MIGRATION_CSV:
        # df_ms was normalized to (dam_size_gb, table_size_gb, query_id, dram_resident_size_gb)
        # in the earlier best-combo block. It carries the migration_size_gb column renamed.
        df_raw = df_raw.merge(
            df_ms[["dam_size_gb", "table_size_gb", "query_id", "dram_resident_size_gb"]],
            on=["dam_size_gb", "table_size_gb", "query_id"],
            how="left",
        )
    else:
        df_raw["dram_resident_size_gb"] = 0
    # For baseline (local, remote) and ideal (local, local) rows there is no
    # matching migration or resident-size entry — those are 0 by definition.
    df_raw["dram_resident_size_gb"] = df_raw["dram_resident_size_gb"].fillna(0)

    if "total_allocated_bytes_delta_pool3" not in df_raw.columns:
        df_raw["total_allocated_bytes_delta_pool3"] = 0

    df_raw["total_duration"] = df_raw["duration"] + df_raw["migration_duration_ns"]
    df_raw = df_raw.rename(columns={"duration": "query_execution_duration"})

    df_raw = df_raw[["query_id", "dam_size_gb", "table_size_gb",
                     "query_execution_duration", "migration_duration_ns", "total_duration",
                     "dram_resident_size_gb", "total_allocated_bytes_delta_pool3"]]
    df_raw = df_raw.sort_values(["query_id", "dam_size_gb", "table_size_gb"]).reset_index(drop=True)
    df_raw.to_csv(RAW_CSV, index=False)
    print(f"Written {RAW_CSV}  ({len(df_raw)} rows)")
