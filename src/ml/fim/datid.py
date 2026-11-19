"""DATID phân tán trên Spark RDD với dữ liệu giao dịch thu gọn dần."""

from pyspark import RDD, StorageLevel

from ml.fim._count import count_items, count_level
from ml.fim.apriori_gen import apriori_gen


def datid(tx: RDD, min_count: int, max_k: int | None = None) -> dict[tuple[int, ...], int]:
    """Loại item không phổ biến và thu gọn giao dịch sau từng bậc."""
    if min_count < 1 or (max_k is not None and max_k < 1):
        raise ValueError("min_count và max_k phải >= 1")
    sc = tx.context
    first_level = count_items(tx, min_count)
    freq = {(item,): count for item, count in first_level}
    if not freq or max_k == 1:
        return freq

    frequent_items = sc.broadcast(frozenset(item for item, _ in first_level))
    current = tx.map(lambda row: tuple(item for item in row if item in frequent_items.value)).filter(
        lambda row: len(row) >= 2
    )
    current.persist(StorageLevel.MEMORY_AND_DISK)
    current.count()
    level, k = sorted(freq), 1
    try:
        while level and (max_k is None or k < max_k):
            candidates = apriori_gen(level)
            if not candidates:
                break
            k += 1
            found = count_level(current, candidates, k, min_count)
            freq.update(found)
            level = sorted(itemset for itemset, _ in found)
            if level and (max_k is None or k < max_k):
                retained = sc.broadcast(frozenset(item for itemset in level for item in itemset))
                next_rdd = current.map(lambda row, bc=retained: tuple(item for item in row if item in bc.value)).filter(
                    lambda row, next_k=k + 1: len(row) >= next_k
                )
                next_rdd.persist(StorageLevel.MEMORY_AND_DISK)
                next_rdd.count()
                current.unpersist()
                current = next_rdd
                retained.unpersist(blocking=True)
        return freq
    finally:
        current.unpersist()
        frequent_items.unpersist(blocking=True)
