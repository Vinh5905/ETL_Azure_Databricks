# Databricks notebook source
# MAGIC %md
# MAGIC # EDA dữ liệu thực nghiệm DS
# MAGIC Đọc snapshot `v1` do DE tạo; notebook chỉ đọc và không sửa dữ liệu nguồn.

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VERSION = "v1"
TX = "instacart.ds_data.transactions"
ITEMS = "instacart.ds_data.item_index"
HOLDOUT = "instacart.ds_data.holdout_eval"
MIN_SUPPORTS = [0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2]

spark = SparkSession.builder.getOrCreate()
transactions = spark.table(TX).where((F.col("data_version") == VERSION) & (F.col("split") == "prior"))
items = spark.table(ITEMS).where(F.col("data_version") == VERSION)

# COMMAND ----------

# Kích thước và độ dài giỏ theo cấp item.
basket_stats = (
    transactions.groupBy("grain")
    .agg(
        F.count("*").alias("n_tx"),
        F.round(F.avg("n_items"), 2).alias("avg_len"),
        F.percentile_approx("n_items", 0.50, 10000).alias("p50"),
        F.percentile_approx("n_items", 0.95, 10000).alias("p95"),
        F.max("n_items").alias("max_len"),
    )
    .orderBy("grain")
)
display(basket_stats)  # noqa: F821

# COMMAND ----------

# Đếm L1 tại từng ngưỡng; ước lượng số cặp ứng viên C2 theo n(L1)*(n(L1)-1)/2.
support_frame = spark.createDataFrame([(value,) for value in MIN_SUPPORTS], "min_support DOUBLE")
transactions_per_grain = transactions.groupBy("grain").agg(F.count("*").alias("n_tx"))
support_counts = (
    items.join(transactions_per_grain, "grain")
    .crossJoin(support_frame)
    .groupBy("grain", "min_support", "n_tx")
    .agg(F.sum((F.col("freq_prior") >= F.ceil(F.col("min_support") * F.col("n_tx"))).cast("long")).alias("n_l1"))
    .withColumn("n_c2", (F.col("n_l1") * (F.col("n_l1") - 1) / 2).cast("long"))
    .withColumn("broadcast_estimate_mb", F.round(F.col("n_c2") * 128 / (1024 * 1024), 1))
    .orderBy("grain", "min_support")
)
display(support_counts)  # noqa: F821

# COMMAND ----------

# Tần suất item theo idx để vẽ đuôi dài log-log; chỉ lấy cột cần thiết.
freq_pd = items.select("grain", "idx", "freq_prior").orderBy("grain", "idx").toPandas()
display(freq_pd.head(20))  # noqa: F821

fig, ax = plt.subplots(figsize=(8, 5))
for grain in ("product", "aisle", "department"):
    values = freq_pd.loc[freq_pd["grain"] == grain, "freq_prior"].to_numpy()
    values = values[values > 0]
    ax.plot(range(1, len(values) + 1), sorted(values, reverse=True), label=grain)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Thứ hạng item (log)")
ax.set_ylabel("Số đơn prior (log)")
ax.set_title("Phân phối tần suất item theo cấp")
ax.legend()
ax.grid(True, which="both", alpha=0.2)
display(fig)  # noqa: F821
plt.close(fig)

# COMMAND ----------

# Quy mô holdout val/test, chỉ gồm đơn đủ món để chia context và targets.
holdout_stats = (
    spark.table(HOLDOUT)  # noqa: F821
    .where(F.col("data_version") == VERSION)
    .groupBy("eval_part")
    .agg(
        F.count("*").alias("n_eval"),
        F.round(F.avg(F.size("context")), 2).alias("avg_context"),
        F.round(F.avg(F.size("targets")), 2).alias("avg_targets"),
    )
    .orderBy("eval_part")
)
display(holdout_stats)  # noqa: F821

# COMMAND ----------

# Kiểm tra giới hạn được ghi trong kế hoạch; không tự sửa lưới khi chưa xem số liệu thật.
product_limits = support_counts.where(F.col("grain") == "product").select(
    "min_support", "n_l1", "n_c2", "broadcast_estimate_mb"
)
display(product_limits)  # noqa: F821
