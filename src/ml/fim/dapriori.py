"""DApriori phân tán trên Spark RDD."""

from pyspark import RDD, StorageLevel

from ml.fim._count import count_items, count_level
from ml.fim.apriori_gen import apriori_gen


def dapriori(tx: RDD, min_count: int, max_k: int | None = None) -> dict[tuple[int, ...], int]:
    """Quét lại dữ liệu giao dịch gốc ở mỗi bậc để đếm tập mục phổ biến."""
    if min_count < 1 or (max_k is not None and max_k < 1):
        raise ValueError("min_count và max_k phải >= 1")
    owns_cache = not tx.is_cached
    cached = tx.persist(StorageLevel.MEMORY_AND_DISK) if owns_cache else tx
    try:
        freq = {(item,): count for item, count in count_items(cached, min_count)}
        level, k = sorted(freq), 1
        while level and (max_k is None or k < max_k):
            candidates = apriori_gen(level)
            if not candidates:
                break
            k += 1
            found = count_level(cached, candidates, k, min_count)
            freq.update(found)
            level = sorted(itemset for itemset, _ in found)
        return freq
    finally:
        if owns_cache:
            cached.unpersist()
