"""Kiểm thử chỉ số holdout và baseline phổ biến."""

from ml.evaluate_holdout import evaluate_holdout, evaluate_popularity_baseline


def test_evaluate_holdout_and_popularity_baseline():
    metrics = evaluate_holdout(
        [{"antecedent": [1], "consequent": [3], "confidence": 0.8, "lift": 1.4}],
        [{"context": [1, 2], "targets": [3, 4]}],
        [2, 5, 6],
        k=2,
        min_confidence=0.5,
    )
    assert metrics["hit_rate_at_k"] == 1.0
    assert metrics["precision_at_k"] == 0.5
    assert metrics["recall_at_k"] == 0.5
    assert metrics["order_coverage"] == 1.0
    baseline = evaluate_popularity_baseline(
        [{"context": [1, 2], "targets": [3, 4]}, {"context": [1, 2], "targets": [3]}],
        [2, 4, 5],
        k=2,
    )
    assert baseline["hit_rate_at_k"] == 0.5
