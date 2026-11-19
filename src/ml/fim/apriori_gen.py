"""Sinh ứng viên Apriori với phép tỉa downward closure."""

from itertools import combinations


def apriori_gen(level: list[tuple[int, ...]]) -> list[tuple[int, ...]]:
    """Sinh ứng viên bậc k+1 từ Lk đã sắp xếp tăng dần."""
    if not level:
        return []
    prev = sorted(set(tuple(x) for x in level))
    k = len(prev[0])
    if any(len(itemset) != k or tuple(sorted(set(itemset))) != itemset for itemset in prev):
        raise ValueError("Các tập trong một level phải có cùng độ dài và tăng dần, không trùng")

    seen = set(prev)
    out: list[tuple[int, ...]] = []
    for i, left in enumerate(prev):
        for right in prev[i + 1 :]:
            if left[:-1] != right[:-1]:
                if right[:-1] > left[:-1]:
                    break
                continue
            candidate = left + (right[-1],)
            if all(subset in seen for subset in combinations(candidate, k)):
                out.append(candidate)
    return out
