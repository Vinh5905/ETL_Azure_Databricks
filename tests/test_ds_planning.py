"""Kiểm tra mở rộng lưới thực nghiệm."""

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
