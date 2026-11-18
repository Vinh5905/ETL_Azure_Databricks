# Databricks notebook source
# MAGIC %md
# MAGIC # Sinh biểu đồ kết quả Data Science
# MAGIC Notebook đọc kết quả thật trong `ml.experiment_runs` và `ml.holdout_metrics`.
# MAGIC Chạy sau khi E1–E7 và E6 đã được nạp; không tự tạo hay thay thế số đo còn thiếu.

# COMMAND ----------

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
RUNS = "instacart.ml.experiment_runs"
HOLDOUT = "instacart.ml.holdout_metrics"
OUTPUT = Path("/Volumes/instacart/ml/inbox/report")
OUTPUT.mkdir(parents=True, exist_ok=True)

runs = spark.table(RUNS).toPandas()
holdout = spark.table(HOLDOUT).toPandas()
if runs.empty:
    raise ValueError("ml.experiment_runs chưa có số đo để vẽ")

# COMMAND ----------


def save_figure(fig, name: str) -> None:
    """Lưu PNG và hiện biểu đồ trong notebook."""
    fig.tight_layout()
    fig.savefig(OUTPUT / name, dpi=160, bbox_inches="tight")
    display(fig)  # noqa: F821
    plt.close(fig)


def median_rows(frame: pd.DataFrame, keys: list[str], metric: str) -> pd.DataFrame:
    """Lấy trung vị các lần lặp thành một điểm theo cấu hình."""
    valid = frame.loc[(frame["status"] == "ok") & frame[metric].notna()]
    return valid.groupby(keys, dropna=False, as_index=False)[metric].median()


# COMMAND ----------

# 1. E2 — thời gian theo MinSupport, tách thuật toán và cấp item.
e2 = runs.loc[runs["experiment"] == "E2"]
summary = median_rows(e2, ["grain", "algo", "min_support"], "wall_s")
fig, axes = plt.subplots(1, 3, figsize=(15, 4), sharey=False)
for ax, grain in zip(axes, ("product", "aisle", "department"), strict=True):
    part = summary.loc[summary["grain"] == grain]
    for algo, series in part.groupby("algo"):
        series = series.sort_values("min_support")
        ax.plot(series["min_support"], series["wall_s"], marker="o", label=algo)
    ax.set_title(grain)
    ax.set_xlabel("MinSupport")
    ax.set_ylabel("Trung vị thời gian (giây)")
    ax.legend()
save_figure(fig, "01_e2_time_by_minsupport.png")

# COMMAND ----------

# 2. E2 — số tập phổ biến theo kích thước và MinSupport.
itemset_rows = []
for row in e2.loc[e2["status"] == "ok"].itertuples(index=False):
    try:
        by_k = json.loads(row.n_itemsets_by_k or "{}")
    except (TypeError, json.JSONDecodeError):
        continue
    for size, count in by_k.items():
        itemset_rows.append(
            {
                "grain": row.grain,
                "algo": row.algo,
                "min_support": row.min_support,
                "k": int(size),
                "n_itemsets": int(count),
            }
        )
itemsets = pd.DataFrame(itemset_rows)
fig, axes = plt.subplots(1, 3, figsize=(15, 4))
if not itemsets.empty:
    itemsets = itemsets.groupby(["grain", "algo", "min_support", "k"], as_index=False)["n_itemsets"].median()
    itemsets["series"] = itemsets["algo"] + " · k=" + itemsets["k"].astype(str)
    for ax, grain in zip(axes, ("product", "aisle", "department"), strict=True):
        part = itemsets.loc[itemsets["grain"] == grain]
        for label, series in part.groupby("series"):
            series = series.sort_values("min_support")
            ax.plot(series["min_support"], series["n_itemsets"], marker=".", label=label)
        ax.set_title(grain)
        ax.set_xlabel("MinSupport")
        ax.set_ylabel("Số tập phổ biến")
        ax.legend(fontsize="x-small")
save_figure(fig, "02_e2_itemsets_by_k.png")

# COMMAND ----------

# 3. E3 — thời gian theo quy mô dữ liệu.
e3 = runs.loc[runs["experiment"] == "E3"]
summary = median_rows(e3, ["algo", "scale_pct"], "wall_s")
fig, ax = plt.subplots(figsize=(7, 4))
for algo, series in summary.groupby("algo"):
    series = series.sort_values("scale_pct")
    ax.plot(series["scale_pct"], series["wall_s"], marker="o", label=algo)
ax.set_xlabel("Quy mô giao dịch (%)")
ax.set_ylabel("Trung vị thời gian (giây)")
ax.legend()
save_figure(fig, "03_e3_time_by_scale.png")

# COMMAND ----------

# 4. E4 — thời gian theo số item; null biểu thị toàn bộ danh mục xuất hiện trong prior.
e4 = runs.loc[runs["experiment"] == "E4"].copy()
e4["item_count"] = e4["top_n"].fillna(49677)
summary = median_rows(e4, ["algo", "item_count"], "wall_s")
fig, ax = plt.subplots(figsize=(7, 4))
for algo, series in summary.groupby("algo"):
    series = series.sort_values("item_count")
    ax.plot(series["item_count"], series["wall_s"], marker="o", label=algo)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Số item (trục log)")
ax.set_ylabel("Trung vị thời gian (giây, log)")
ax.legend()
save_figure(fig, "04_e4_time_by_item_count.png")

# COMMAND ----------

# 5. E2/E3 — bộ nhớ tăng thêm theo thuật toán, chỉ lấy phép đo có dữ liệu.
memory_runs = runs.loc[runs["experiment"].isin(["E2", "E3"]) & (runs["status"] == "ok")]
memory_runs = memory_runs.loc[memory_runs["peak_mem_mb"].notna()]
fig, ax = plt.subplots(figsize=(7, 4))
if not memory_runs.empty:
    labels = sorted(memory_runs["algo"].dropna().unique())
    groups = [memory_runs.loc[memory_runs["algo"] == algo, "peak_mem_mb"] for algo in labels]
    ax.boxplot(groups, labels=labels)
ax.set_ylabel("Bộ nhớ tăng thêm (MB)")
save_figure(fig, "05_memory_by_algorithm.png")

# COMMAND ----------

# 6. E5 — speedup và efficiency, tự tính từ median của 1 core cùng cấu hình.
e5 = runs.loc[(runs["experiment"] == "E5") & (runs["status"] == "ok")]
keys = ["platform", "algo", "grain", "scale_pct", "min_support", "cores"]
e5 = e5.groupby(keys, dropna=False, as_index=False)["wall_s"].median()
base = e5.loc[e5["cores"] == 1, ["platform", "algo", "grain", "scale_pct", "min_support", "wall_s"]]
base = base.rename(columns={"wall_s": "time_1_core"})
e5 = e5.merge(base, on=["platform", "algo", "grain", "scale_pct", "min_support"], how="left")
e5["speedup"] = e5["time_1_core"] / e5["wall_s"]
e5["efficiency"] = e5["speedup"] / e5["cores"]
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for algo, series in e5.groupby("algo"):
    series = series.sort_values("cores")
    axes[0].plot(series["cores"], series["speedup"], marker="o", label=algo)
    axes[1].plot(series["cores"], series["efficiency"], marker="o", label=algo)
axes[0].set_title("Speedup Sₙ = T₁/Tₙ")
axes[1].set_title("Efficiency Eₙ = Sₙ/n")
for ax in axes:
    ax.set_xlabel("Số core")
    ax.legend(fontsize="small")
save_figure(fig, "06_e5_speedup_efficiency.png")

# COMMAND ----------

# 7. E6 — thời gian và byte ghi trung gian trên cùng một máy local.
e6 = runs.loc[(runs["experiment"] == "E6") & (runs["status"] == "ok")]
e6 = e6.groupby(["algo", "scale_pct", "cores"], as_index=False).agg(
    wall_s=("wall_s", "median"), spill_mb=("spill_mb", "median"), n_result=("n_result", "max")
)
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for (algo, cores), series in e6.groupby(["algo", "cores"]):
    series = series.sort_values("scale_pct")
    label = f"{algo} · c{cores}"
    axes[0].plot(series["scale_pct"], series["wall_s"], marker="o", label=label)
    if series["spill_mb"].notna().any():
        axes[1].plot(series["scale_pct"], series["spill_mb"], marker="o", label=label)
axes[0].set_xlabel("Quy mô giao dịch (%)")
axes[0].set_ylabel("Thời gian (giây)")
axes[1].set_xlabel("Quy mô giao dịch (%)")
axes[1].set_ylabel("Spill (MB; Hadoop bytes cần chuẩn hóa ở báo cáo)")
for ax in axes:
    ax.legend(fontsize="small")
save_figure(fig, "07_e6_mapreduce_vs_spark.png")

# COMMAND ----------

# 8. E7 — chất lượng trên val; đường baseline phổ biến để so sánh.
e7 = holdout.loc[holdout["eval_part"] == "val"].copy()
e7 = e7.merge(runs[["run_key", "min_support"]].drop_duplicates(), on="run_key", how="left")
e7["hit_rate_at_10"] = e7.get("hit_rate_at_10", e7.get("hit_rate_at_k"))
e7["precision_at_10"] = e7.get("precision_at_10", e7.get("precision_at_k"))
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for min_support, series in e7.loc[e7["run_key"] != "popularity"].groupby("min_support"):
    series = series.sort_values("min_confidence")
    axes[0].plot(series["min_confidence"], series["hit_rate_at_10"], marker="o", label=f"MinSupp={min_support}")
    axes[1].plot(series["min_confidence"], series["precision_at_10"], marker="o", label=f"MinSupp={min_support}")
baseline = e7.loc[e7["run_key"] == "popularity"]
if not baseline.empty:
    axes[0].axhline(baseline["hit_rate_at_10"].iloc[0], color="black", linestyle="--", label="baseline")
    axes[1].axhline(baseline["precision_at_10"].iloc[0], color="black", linestyle="--", label="baseline")
axes[0].set_title("HitRate@10 trên val")
axes[1].set_title("Precision@10 trên val")
for ax in axes:
    ax.set_xlabel("MinConfidence")
    ax.legend(fontsize="x-small")
save_figure(fig, "08_e7_holdout_quality.png")

# COMMAND ----------

# Lưu bảng tổng hợp các lần chạy và holdout để gắn nguồn cho báo cáo.
runs.to_csv(OUTPUT / "experiment_runs.csv", index=False)
holdout.to_csv(OUTPUT / "holdout_metrics.csv", index=False)
print(f"Đã lưu 8 biểu đồ và bảng số liệu vào {OUTPUT}")
