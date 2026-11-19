"""Kiểm model BasketRules: xếp hạng, bù món phổ biến, load_context và log/load qua MLflow."""

import mlflow
import pandas as pd
import pytest
from mlflow.models import infer_signature

from ml.serving.basket_rules import BasketRules


def _model():
    model = BasketRules()
    model.rules = [
        {"antecedent": [10], "consequent": [12], "confidence": 0.7, "lift": 1.4},
        {"antecedent": [10, 11], "consequent": [13], "confidence": 0.9, "lift": 1.8},
    ]
    model.pid2idx = {100: 10, 101: 11, 102: 12, 103: 13, 104: 14}
    model.idx2pid = {idx: pid for pid, idx in model.pid2idx.items()}
    model.idx2name = {idx: f"item-{idx}" for idx in model.idx2pid}
    model.popular = [10, 11, 14, 12]
    return model


def test_model_ranks_rules_then_fills_with_popularity():
    output = _model().predict(None, pd.DataFrame({"product_ids": [[100, 101]]}), {"k": 3})
    assert output.product_id.tolist() == [103, 102, 104]
    assert output.source.tolist() == ["rule", "rule", "popular"]


def test_model_rejects_invalid_k():
    with pytest.raises(ValueError):
        _model().predict(None, pd.DataFrame({"product_ids": [[]]}), {"k": 0})


def test_model_uses_popular_fallback_when_no_rule_matches():
    model = _model()
    model.rules = []
    output = model.predict(None, pd.DataFrame({"product_ids": [[100]]}), {"k": 3})
    assert output.product_id.tolist() == [101, 104, 102]
    assert output.source.tolist() == ["popular", "popular", "popular"]
    assert str(output.confidence.dtype) == "float64"
    assert str(output.lift.dtype) == "float64"


def test_pyfunc_model_logs_loads_and_predicts(tmp_path):
    rules_path = tmp_path / "rules.parquet"
    items_path = tmp_path / "items.parquet"
    pd.DataFrame({"antecedent": [[10]], "item": [12], "confidence": [0.7], "lift": [1.4]}).to_parquet(
        rules_path, index=False
    )
    pd.DataFrame(
        {
            "product_id": [100, 102, 104],
            "idx": [10, 12, 14],
            "product_name": ["basket", "rule item", "popular item"],
        }
    ).to_parquet(items_path, index=False)
    input_example = pd.DataFrame({"product_ids": [[100]]})
    signature = infer_signature(input_example, _model().predict(None, input_example), params={"k": 2})

    previous_uri = mlflow.get_tracking_uri()
    mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())
    try:
        with mlflow.start_run():
            model_info = mlflow.pyfunc.log_model(
                artifact_path="basket_rules",
                python_model=BasketRules(),
                artifacts={"rules": str(rules_path), "items": str(items_path)},
                input_example=input_example,
                signature=signature,
            )
        model = mlflow.pyfunc.load_model(model_info.model_uri)
        predictions = model.predict(input_example, params={"k": 2})
    finally:
        mlflow.set_tracking_uri(previous_uri)

    assert predictions["product_id"].tolist() == [102, 104]


def test_load_context_fills_k_popular_items_when_no_rule_fires(tmp_path):
    # Danh mục 150 món, k tối đa 100: phần bù phải lấy được đủ k món ngoài giỏ.
    rules_path = tmp_path / "rules.parquet"
    items_path = tmp_path / "items.parquet"
    pd.DataFrame({"antecedent": [[149]], "item": [148], "confidence": [0.9], "lift": [2.0]}).to_parquet(
        rules_path, index=False
    )
    pd.DataFrame(
        {"product_id": [1000 + i for i in range(150)], "idx": list(range(150)), "product_name": ["x"] * 150}
    ).to_parquet(items_path, index=False)
    model = BasketRules()
    model.load_context(type("Ctx", (), {"artifacts": {"rules": str(rules_path), "items": str(items_path)}})())

    output = model.predict(None, pd.DataFrame({"product_ids": [[1000, 1001]]}), {"k": 100})

    assert output.product_id.tolist() == [1000 + i for i in range(2, 102)]
    assert set(output.source) == {"popular"}
