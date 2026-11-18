"""Kiểm tra bung lưới và các phép đo thuần Python của DS."""

from ml.champion import select_champion
from ml.evaluate_holdout import evaluate_holdout, evaluate_popularity_baseline
from ml.plan_grid import make_run_key, plan_grid


def test_plan_grid_expands_and_creates_distinct_keys():
    rows = plan_grid(experiments=["E7"])
    assert len(rows) == 9
    assert len({row["run_key"] for row in rows}) == 9
    assert {row["min_confidence"] for row in rows} == {0.1, 0.2, 0.3}
    assert make_run_key(rows[0]) == rows[0]["run_key"]
    e2 = plan_grid(experiments=["E2"])
    assert len(e2) == 99
    assert len({row["run_key"] for row in e2}) == 99


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


def test_champion_selection_uses_validation_then_smallest_model():
    chosen = select_champion(
        [
            {"eval_part": "test", "run_key": "test", "order_coverage": 1, "hit_rate_at_10": 1, "n_rules": 1},
            {"eval_part": "val", "run_key": "a", "order_coverage": 0.6, "hit_rate_at_10": 0.4, "n_rules": 100},
            {"eval_part": "val", "run_key": "b", "order_coverage": 0.7, "hit_rate_at_10": 0.4, "n_rules": 50},
        ]
    )
    assert chosen["run_key"] == "b"
