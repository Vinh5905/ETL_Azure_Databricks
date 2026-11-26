"""Kiểm tra lưới, run_key, lọc kết quả và task values."""

from ml.plan_grid import group_e7_by_support, make_run_key, pending_runs, plan_grid, task_values_by_cores


def test_grid_counts_and_defaults():
    expected = {"E1": 3, "E2": 99, "E3": 45, "E4": 36, "E5": 54, "E7": 9}
    assert len(plan_grid()) == sum(expected.values())
    assert plan_grid(experiments=[]) == []
    for experiment, count in expected.items():
        rows = plan_grid(experiments=[experiment])
        assert len(rows) == count
        assert all(row["platform"] == "databricks" for row in rows)
        assert all(row["max_k"] in (None, 3) for row in rows)
    rows = plan_grid(experiments=["E2"])
    assert {row["min_confidence"] for row in rows if row["grain"] == "aisle"} == {0.3}
    assert {row["min_confidence"] for row in rows if row["grain"] == "department"} == {0.5}
    assert all(row["data_version"] == "v2" for row in plan_grid(experiments=["E1"], data_version="v2"))


def test_run_key_is_stable_and_tracks_algorithm_version():
    config = plan_grid(experiments=["E1"])[0]
    original = make_run_key(config)
    config.update(timeout_min=1, persist=not config["persist"], reps=100)
    assert make_run_key(config) == original
    config["algo_version"] = "fim-v2"
    assert make_run_key(config) != original


def test_queue_filters_ok_runs_groups_e7_and_splits_by_cores():
    rows = plan_grid(experiments=["E5", "E7"])
    e7 = [row for row in rows if row["experiment"] == "E7"]
    assert len(e7) == 9
    grouped = group_e7_by_support(rows)
    grouped_e7 = [row for row in grouped if row["experiment"] == "E7"]
    assert len(grouped_e7) == 3
    queued = pending_runs(rows, {grouped_e7[0]["run_key"]})
    assert len([row for row in queued if row["experiment"] == "E7"]) == 2
    assert all(row["sweep"] for row in queued if row["experiment"] == "E7")
    values = task_values_by_cores(queued)
    assert set(values) == {"queue_c1", "queue_c2", "queue_c4"}
    assert all(len(key) == 12 for keys in values.values() for key in keys)
    assert {key: len(keys) for key, keys in values.items()} == {"queue_c1": 18, "queue_c2": 18, "queue_c4": 20}
