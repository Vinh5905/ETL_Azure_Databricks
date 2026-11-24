from __future__ import annotations

import json
import threading
from contextlib import contextmanager

import pytest

from ml.run_experiment import _calculate, execute_run

TOYS = [([1, 2, 3],), ([1, 2],), ([1, 3],), ([1, 2, 3],), ([2, 3],)]


@pytest.fixture(scope="module")
def toy_df(spark):
    return spark.createDataFrame(TOYS, "items array<int>")


def config(algo: str) -> dict:
    return {
        "run_key": f"toy-{algo}",
        "algo_version": "fim-v1",
        "experiment": "E1",
        "platform": "databricks",
        "algo": algo,
        "grain": "product",
        "scale_pct": 10,
        "top_n": None,
        "min_support": 0.4,
        "min_confidence": 0.5,
        "max_k": 3,
        "cores": 1,
        "rep": 1,
        "data_version": "test",
        "timeout_min": 5,
        "persist": False,
        "sweep": None,
    }


@pytest.mark.parametrize("algo", ["dapriori", "datid", "fpgrowth", "all"])
def test_calculate_one_algorithm_or_all_matches_on_toy_data(spark, toy_df, algo):
    result = _calculate(spark, toy_df, config(algo), len(TOYS))

    assert result["metrics"]["n_itemsets"] == 7
    assert json.loads(result["metrics"]["n_itemsets_by_k"]) == {"1": 3, "2": 3, "3": 1}
    assert result["metrics"]["n_rules_single"] > 0
    if algo == "all":
        assert result["metrics"]["check_passed"] is True


def test_timeout_is_recorded_without_raising(monkeypatch):
    expired = threading.Event()

    class FakeFrame:
        def count(self):
            return 1

    class FakeSpark:
        sparkContext = object()

    @contextmanager
    def fake_timeout(_sc, _minutes, _group):
        yield expired
        expired.set()

    class FakePeak:
        delta_mb = 1.0

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

    monkeypatch.setattr(
        "ml.run_experiment._calculate",
        lambda *_args: {"freq": {}, "rules": None, "metrics": {"n_itemsets": 0}},
    )
    row, freq, rules = execute_run(
        FakeSpark(),
        config("dapriori"),
        FakeFrame(),
        timeout_factory=fake_timeout,
        peak_factory=FakePeak,
    )

    assert row["status"] == "timeout"
    assert row["error"] == "Vượt quá timeout 5 phút"
    assert freq is None
    assert rules is None
