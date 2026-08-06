import pandas as pd

INPUT_CSV = "consolidated_data.csv"
OUTPUT_CSV = "per_query_min_duration.csv"

# ---------------------------------------------------------------------------
# Load and reduce to one row per (query_id, dam_size, table_size) by taking
# the minimum duration across the 10 repetitions.
# ---------------------------------------------------------------------------
df = pd.read_csv(INPUT_CSV, dtype={"dam_size": str, "table_size": str})

df_min = (
    df.groupby(["query_id", "dam_size", "table_size"], as_index=False)["duration"]
    .mean()
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

pivot = df_rest.pivot(index="query_id", columns="col", values="duration")[combo_labels]
pivot = pivot.reset_index()

# Attach baseline and ideal columns
pivot = pivot.merge(df_baseline, on="query_id", how="left")
pivot = pivot.merge(df_ideal,    on="query_id", how="left")

pivot = pivot.sort_values("query_id").reset_index(drop=True)

pivot.to_csv(OUTPUT_CSV, index=False)
print(f"Written {OUTPUT_CSV}  ({len(pivot)} rows x {len(pivot.columns)} columns)")

# ---------------------------------------------------------------------------
# Second output: best (dam_size, table_size) per query_id
# ---------------------------------------------------------------------------
BEST_CSV = "best_combo_per_query.csv"

best_idx = df_rest.groupby("query_id")["duration"].idxmin()
df_best = df_rest.loc[best_idx, ["query_id", "dam_size", "table_size", "duration"]].copy()
df_best["dam_size_gb"]   = df_best["dam_size"].apply(to_gb)
df_best["table_size_gb"] = df_best["table_size"].apply(to_gb)
df_best = df_best.rename(columns={"duration": "best_duration"})
df_best = df_best[["query_id", "dam_size_gb", "table_size_gb", "best_duration"]]

df_best = df_best.merge(df_baseline, on="query_id", how="left")
df_best = df_best.merge(df_ideal,    on="query_id", how="left")
df_best = df_best[["query_id", "dam_size_gb", "table_size_gb", "baseline", "ideal", "best_duration"]]
df_best = df_best.sort_values("query_id").reset_index(drop=True)

df_best.to_csv(BEST_CSV, index=False)
print(f"Written {BEST_CSV}  ({len(df_best)} rows)")
