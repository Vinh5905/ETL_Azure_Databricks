"""Kiểm quy tắc chọn champion bằng các dòng bảng holdout/rules đồ chơi."""

import pandas as pd
import pytest

from ml.champion import _write_local_parquet, select_champion


def test_write_local_parquet_is_readable_by_pyfunc(tmp_path):
    class SparkFrame:
        def toPandas(self):
            return pd.DataFrame({"antecedent": [[1, 2]], "item": [3], "confidence": [0.8], "lift": [1.5]})

    artifact = tmp_path / "rules.parquet"
    _write_local_parquet(SparkFrame(), str(artifact))

    restored = pd.read_parquet(artifact)

    assert restored.iloc[0]["antecedent"].tolist() == [1, 2]
    assert restored.iloc[0]["item"] == 3
    assert restored.iloc[0]["confidence"] == 0.8
    assert restored.iloc[0]["lift"] == 1.5


def test_select_champion_filters_then_uses_hit_rate_and_rule_count():
    toy_metrics = [
        {
            "experiment": "E7",
            "eval_part": "val",
            "k": 10,
            "run_key": "low-coverage",
            "min_confidence": 0.1,
            "order_coverage": 0.49,
            "hit_rate_at_k": 0.99,
            "rule_count": 1,
        },
        {
            "experiment": "E7",
            "eval_part": "val",
            "k": 10,
            "run_key": "few-rules",
            "min_confidence": 0.3,
            "order_coverage": 0.6,
            "hit_rate_at_k": 0.75,
            "rule_count": 12,
        },
        {
            "experiment": "E7",
            "eval_part": "val",
            "k": 10,
            "run_key": "more-rules",
            "min_confidence": 0.2,
            "order_coverage": 0.8,
            "hit_rate_at_k": 0.75,
            "rule_count": 20,
        },
        {
            "experiment": "E7",
            "eval_part": "test",
            "k": 10,
            "run_key": "test-only",
            "min_confidence": 0.1,
            "order_coverage": 1.0,
            "hit_rate_at_k": 1.0,
            "rule_count": 0,
        },
        {
            "experiment": "E1",
            "eval_part": "val",
            "k": 10,
            "run_key": "not-e7",
            "min_confidence": 0.1,
            "order_coverage": 1.0,
            "hit_rate_at_k": 1.0,
            "rule_count": 0,
        },
    ]

    champion = select_champion(toy_metrics)

    assert champion["run_key"] == "few-rules"


def test_select_champion_rejects_when_no_e7_reaches_coverage():
    with pytest.raises(ValueError, match="order_coverage >= 0.5"):
        select_champion(
            [
                {
                    "experiment": "E7",
                    "eval_part": "val",
                    "k": 10,
                    "run_key": "under-covered",
                    "order_coverage": 0.1,
                    "hit_rate_at_k": 1.0,
                    "rule_count": 1,
                }
            ]
        )
