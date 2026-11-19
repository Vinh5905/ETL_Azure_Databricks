"""Sinh luật kết hợp từ bảng tần suất tập mục."""

from itertools import combinations

from pyspark.sql import DataFrame, SparkSession

RULE_SCHEMA = "antecedent array<int>, consequent array<int>, support double, confidence double, lift double"


def gen_rules(
    spark: SparkSession,
    freq: dict[tuple[int, ...], int],
    n_tx: int,
    min_conf: float,
    single_consequent: bool = False,
) -> DataFrame:
    """Sinh luật X → Y, có thể giới hạn vế phải một item để so với MLlib."""
    if n_tx < 1 or not 0 <= min_conf <= 1:
        raise ValueError("n_tx phải > 0 và min_conf nằm trong [0, 1]")
    if any(count < 0 or count > n_tx for count in freq.values()):
        raise ValueError("Tần suất phải nằm trong [0, n_tx]")
    itemsets = [tuple(sorted(key)) for key in freq if len(key) >= 2]
    if not itemsets:
        return spark.createDataFrame([], RULE_SCHEMA)
    rows = []
    for itemset in itemsets:
        whole_count = freq[itemset]
        for size in range(1, len(itemset)):
            for left in combinations(itemset, size):
                right = tuple(item for item in itemset if item not in left)
                if single_consequent and len(right) != 1:
                    continue
                left_count = freq.get(left, 0)
                right_count = freq.get(right, 0)
                if not left_count or not right_count:
                    continue
                confidence = whole_count / left_count
                if confidence >= min_conf:
                    rows.append(
                        (list(left), list(right), whole_count / n_tx, confidence, confidence * n_tx / right_count)
                    )
    return spark.createDataFrame(rows, RULE_SCHEMA)
