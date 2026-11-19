"""Các phép đếm RDD dùng chung cho DApriori và DATID."""

from itertools import combinations
from operator import add

from pyspark import RDD


def count_items(tx: RDD, min_count: int) -> list[tuple[int, int]]:
    """Đếm item và giữ item đạt ngưỡng đếm tối thiểu."""
    if min_count < 1:
        raise ValueError("min_count phải >= 1")
    return (
        tx.flatMap(lambda row: ((int(item), 1) for item in row))
        .reduceByKey(add)
        .filter(lambda pair: pair[1] >= min_count)
        .collect()
    )


def count_level(
    tx: RDD, candidates: list[tuple[int, ...]], k: int, min_count: int
) -> list[tuple[tuple[int, ...], int]]:
    """Đếm ứng viên một bậc Apriori bằng một lượt reduceByKey."""
    if not candidates:
        return []
    sc = tx.context
    candidate_set = sc.broadcast(frozenset(candidates))
    candidate_items = sc.broadcast(frozenset(item for candidate in candidates for item in candidate))

    def emit(row):
        items = tuple(sorted(item for item in row if item in candidate_items.value))
        if len(items) >= k:
            for candidate in combinations(items, k):
                if candidate in candidate_set.value:
                    yield candidate, 1

    try:
        return tx.flatMap(emit).reduceByKey(add).filter(lambda pair: pair[1] >= min_count).collect()
    finally:
        candidate_set.destroy()
        candidate_items.destroy()
