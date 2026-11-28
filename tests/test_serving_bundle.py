"""Kiểm cấu hình serving chỉ có ở target chuyên biệt sau khi champion tồn tại."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_serving_endpoint_has_an_isolated_bundle():
    base = yaml.safe_load((ROOT / "bundles/databricks.yml").read_text(encoding="utf-8"))
    serving_root = ROOT / "bundles/serving"
    bundle = yaml.safe_load((serving_root / "databricks.yml").read_text(encoding="utf-8"))
    serving = yaml.safe_load((serving_root / "resources/serving_ds.yml").read_text(encoding="utf-8"))
    endpoint = serving["resources"]["model_serving_endpoints"]["basket_rules"]

    assert bundle["bundle"]["name"] == "instacart-basket-serving"
    assert bundle["include"] == ["resources/*.yml"]
    assert set(bundle["targets"]) == {"prod"}
    assert bundle["targets"]["prod"]["workspace"]["root_path"].startswith(
        "/Workspace/Users/${workspace.current_user.userName}/"
    )
    assert "serving" not in base["targets"]
    assert "champion_version" not in base["variables"]
    assert set(serving["resources"]) == {"model_serving_endpoints"}
    assert not any("serving" in path.name for path in (ROOT / "bundles/resources").glob("*.yml"))
    assert endpoint["name"] == "instacart-basket-rules"
    entity = endpoint["config"]["served_entities"][0]
    assert entity == {
        "entity_name": "instacart.ml.basket_rules",
        "entity_version": "${var.champion_version}",
        "workload_size": "Small",
        "scale_to_zero_enabled": True,
    }
    assert endpoint["permissions"] == [
        {"user_name": "${workspace.current_user.userName}", "level": "CAN_MANAGE"},
        {"service_principal_name": "${var.func_sp_application_id}", "level": "CAN_QUERY"},
    ]
