"""Kiểm cấu hình serving chỉ có ở target chuyên biệt sau khi champion tồn tại."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_serving_endpoint_is_isolated_from_dev_and_normal_prod_deploys():
    base = yaml.safe_load((ROOT / "bundles/databricks.yml").read_text(encoding="utf-8"))
    serving = yaml.safe_load((ROOT / "bundles/resources/serving_ds.yml").read_text(encoding="utf-8"))
    endpoint = serving["targets"]["serving"]["resources"]["model_serving_endpoints"]["basket_rules"]

    assert "serving" in base["targets"]
    assert base["variables"]["champion_version"]["default"] == ""
    assert "model_serving_endpoints" not in base["targets"]["dev"].get("resources", {})
    assert "model_serving_endpoints" not in base["targets"]["prod"].get("resources", {})
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
