"""Kiểm tra hai chế độ của HTTP Function bằng mock, không gọi dịch vụ thật."""

import json

from serving_func import function_app


class _Request:
    def __init__(self, body):
        self.body = body

    def get_json(self):
        return self.body


class _Credential:
    def get_token(self, scope):
        assert scope == function_app.DATABRICKS_SCOPE
        return type("Token", (), {"token": "test-token"})()


def test_recommend_calls_endpoint(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com/")
    monkeypatch.setenv("ENDPOINT_NAME", "test-endpoint")
    monkeypatch.setenv("SERVING_MODE", "endpoint")
    monkeypatch.setattr(function_app, "_credential", lambda: _Credential())
    monkeypatch.setattr(
        function_app.requests,
        "post",
        lambda *args, **kwargs: type(
            "Response",
            (),
            {
                "json": lambda self: {"predictions": [{"input_row": 0, "product_id": 7}]},
                "raise_for_status": lambda self: None,
            },
        )(),
    )

    response = function_app.recommend(_Request({"product_ids": [1, 2], "k": 3}))

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {"recommendations": [{"input_row": 0, "product_id": 7}]}


def test_recommend_fallback_uses_cached_model(monkeypatch):
    monkeypatch.setenv("SERVING_MODE", "fallback")
    model = type("Model", (), {"_recommend": lambda self, ids, k: [{"product_id": 9}]})()
    monkeypatch.setattr(function_app, "_load_fallback_model", lambda: model)

    response = function_app.recommend(_Request({"product_ids": [1], "k": 1}))

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {"recommendations": [{"product_id": 9}]}


def test_recommend_rejects_empty_ids():
    response = function_app.recommend(_Request({"product_ids": []}))

    assert response.status_code == 400
    assert "không được rỗng" in json.loads(response.get_body())["error"]
