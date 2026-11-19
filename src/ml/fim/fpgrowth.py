"""Bọc FP-Growth trong Spark MLlib."""

from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.sql import DataFrame


def fpgrowth(
    df: DataFrame,
    min_support: float,
    min_conf: float = 0.0,
    num_partitions: int | None = None,
) -> FPGrowthModel:
    """Huấn luyện FP-Growth trên DataFrame có cột `items` kiểu array."""
    if not 0 < min_support <= 1 or not 0 <= min_conf <= 1:
        raise ValueError("min_support phải ở (0, 1] và min_conf nằm trong [0, 1]")
    estimator = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_conf)
    if num_partitions is not None:
        if num_partitions < 1:
            raise ValueError("num_partitions phải >= 1")
        estimator = estimator.setNumPartitions(num_partitions)
    return estimator.fit(df)
