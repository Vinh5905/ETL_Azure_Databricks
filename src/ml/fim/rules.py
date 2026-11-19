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
    normalized_freq = {tuple(sorted(key)): count for key, count in freq.items()}
    itemsets = [itemset for itemset in normalized_freq if len(itemset) >= 2]
    if not itemsets:
        return spark.createDataFrame([], RULE_SCHEMA)

    # Downward closure: chỉ cần mọi tập con bậc k-1 có mặt. Quy nạp xuống bậc 1 thì mọi
    # vế trái/phải đều có support, nên driver không phải duyệt lại 2^k cách tách.
    for itemset in itemsets:
        for subset in combinations(itemset, len(itemset) - 1):
            if subset not in normalized_freq:
                raise ValueError(f"Thiếu support của vế trái/phải khi sinh luật từ {itemset}")

    sc = spark.sparkContext
    freq_bc = sc.broadcast(normalized_freq)

    def rules_of(itemset: tuple[int, ...]):
        counts = freq_bc.value
        whole_count = counts[itemset]
        output = []
        for size in range(1, len(itemset)):
            for left in combinations(itemset, size):
                right = tuple(item for item in itemset if item not in left)
                if single_consequent and len(right) != 1:
                    continue
                left_count, right_count = counts[left], counts[right]
                confidence = whole_count / left_count
                if confidence >= min_conf:
                    output.append(
                        (list(left), list(right), whole_count / n_tx, confidence, confidence * n_tx / right_count)
                    )
        return output

    partitions = min(len(itemsets), max(sc.defaultParallelism, 1))
    rows = sc.parallelize(itemsets, partitions).flatMap(rules_of)
    return spark.createDataFrame(rows, RULE_SCHEMA)
