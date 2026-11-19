"""Logic gợi ý dùng chung giữa đánh giá local và model serving."""

from collections import defaultdict
from collections.abc import Iterable
from itertools import combinations


def rank_rule_indices(context: Iterable[int], rules: Iterable[dict], *, k: int = 10) -> list[tuple[int, float, float]]:
    """Trả các món theo confidence/lift cùng điểm số của luật tốt nhất."""
    if k < 1:
        raise ValueError("k phải >= 1")
    basket = {int(item) for item in context}
    ctx = sorted(basket)
    by_antecedent: dict[tuple[int, ...], list[dict]] = defaultdict(list)
    max_left = 0
    for rule in rules:
        antecedent = tuple(sorted(int(item) for item in rule["antecedent"]))
        if antecedent:
            by_antecedent[antecedent].append(rule)
            max_left = max(max_left, len(antecedent))
    best: dict[int, tuple[float, float]] = {}
    for size in range(1, min(max_left, len(ctx)) + 1):
        for antecedent in combinations(ctx, size):
            for rule in by_antecedent.get(antecedent, ()):
                for value in rule["consequent"]:
                    item = int(value)
                    score = (float(rule["confidence"]), float(rule.get("lift", 0.0)))
                    if item not in basket and score > best.get(item, (-1.0, -1.0)):
                        best[item] = score
    return [(item, *best[item]) for item in sorted(best, key=lambda item: (-best[item][0], -best[item][1], item))[:k]]


def recommend_indices(
    context: Iterable[int],
    rules: Iterable[dict],
    popular_items: Iterable[int] = (),
    *,
    k: int = 10,
    fill_popular: bool = True,
) -> list[int]:
    """Xếp hạng món theo confidence/lift, tùy chọn bù top popular tới đủ k."""
    return [
        item for item, _, _ in recommend_scored_indices(context, rules, popular_items, k=k, fill_popular=fill_popular)
    ]


def recommend_scored_indices(
    context: Iterable[int],
    rules: Iterable[dict],
    popular_items: Iterable[int] = (),
    *,
    k: int = 10,
    fill_popular: bool = True,
) -> list[tuple[int, float | None, float | None]]:
    """Xếp hạng luật và bổ sung món phổ biến, giữ confidence/lift rỗng cho fallback."""
    basket = {int(item) for item in context}
    ranked = rank_rule_indices(basket, rules, k=k)
    scores = {item: (confidence, lift) for item, confidence, lift in ranked}
    result = [*ranked]
    if fill_popular:
        for item in popular_items:
            item = int(item)
            if len(result) >= k:
                break
            if item not in basket and item not in scores and item not in {rec[0] for rec in result}:
                result.append((item, None, None))
    return result
