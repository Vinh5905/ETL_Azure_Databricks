from __future__ import annotations

import json
from pathlib import Path

import pytest

from serving_func import function_app


class Request:
    def __init__(self, body):
        self.body = body

    def get_json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


class Credential:
    def get_token(self, scope):
        assert scope == function_app.DATABRICKS_SCOPE
        return type("AccessToken", (), {"token": "test-token"})()


class Response:
    def json(self):
        return {"predictions": [{"input_row": 0, "rank": 1, "product_id": 7}, {"input_row": 1}]}

    def raise_for_status(self):
        return None


def body(response):
    return json.loads(response.get_body())


def test_endpoint_mode_uses_managed_identity_and_normalizes_predictions(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com/")
    monkeypatch.setenv("ENDPOINT_NAME", "basket-rules-test")
    monkeypatch.setenv("SERVING_MODE", "endpoint")
    monkeypatch.setattr(function_app, "_credential", lambda: Credential())
    calls = {}

    def post(url, **kwargs):
        calls.update(url=url, **kwargs)
        return Response()

    monkeypatch.setattr(function_app.requests, "post", post)
    response = function_app.recommend(Request({"product_ids": [1, 2], "k": 3}))

    assert response.status_code == 200
    assert body(response) == {"recommendations": [{"input_row": 0, "rank": 1, "product_id": 7}]}
    assert calls["url"] == "https://example.databricks.com/serving-endpoints/basket-rules-test/invocations"
    assert calls["headers"] == {"Authorization": "Bearer test-token"}
    assert calls["json"] == {"dataframe_records": [{"product_ids": [1, 2]}], "params": {"k": 3}}
    assert calls["timeout"] == 60


def test_endpoint_caps_input_at_50_ids(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "example.databricks.com")
    monkeypatch.setattr(function_app, "_credential", lambda: Credential())
    calls = {}

    def post(_url, **kwargs):
        calls.update(kwargs)
        return Response()

    monkeypatch.setattr(function_app.requests, "post", post)

    response = function_app.recommend(Request({"product_ids": list(range(100)), "k": 10}))

    assert response.status_code == 200
    assert calls["json"] == {"dataframe_records": [{"product_ids": list(range(50))}], "params": {"k": 10}}


def test_fallback_mode_uses_cached_model_and_adds_rank(monkeypatch):
    monkeypatch.setenv("SERVING_MODE", "fallback")
    model = type("Model", (), {"_recommend": lambda self, ids, k: [{"product_id": ids[0], "k": k}]})()
    monkeypatch.setattr(function_app, "_load_fallback_model", lambda: model)

    response = function_app.recommend(Request({"product_ids": [42], "k": 2}))

    assert response.status_code == 200
    assert body(response) == {"recommendations": [{"rank": 1, "product_id": 42, "k": 2}]}


def test_fallback_loader_uses_managed_identity_and_downloads_both_artifacts(monkeypatch):
    from ml.serving import basket_rules

    monkeypatch.setenv("STORAGE_ACCOUNT_NAME", "account")
    monkeypatch.setenv("DS_CONTAINER_NAME", "ds-data")
    monkeypatch.setattr(function_app, "_fallback_model", None)
    credential = object()
    monkeypatch.setattr(function_app, "_credential", lambda: credential)
    downloaded = []
    service_args = {}

    class Container:
        def download_blob(self, blob_name):
            downloaded.append(blob_name)
            return type("Blob", (), {"readall": lambda self: blob_name.encode()})()

    class BlobService:
        def __init__(self, url, credential):
            service_args.update(url=url, credential=credential)

        def get_container_client(self, container):
            service_args["container"] = container
            return Container()

    class Model:
        def load_context(self, context):
            self.artifacts = context.artifacts

        def _recommend(self, ids, k):
            return [{"product_id": ids[0], "k": k}]

    monkeypatch.setattr("azure.storage.blob.BlobServiceClient", BlobService)
    monkeypatch.setattr(basket_rules, "BasketRules", Model)

    model = function_app._load_fallback_model()

    assert service_args == {
        "url": "https://account.blob.core.windows.net",
        "credential": credential,
        "container": "ds-data",
    }
    assert downloaded == ["exports/serving/rules.parquet", "exports/serving/items.parquet"]
    assert model._recommend([42], 1) == [{"product_id": 42, "k": 1}]
    assert all(Path(path).exists() is False for path in model.artifacts.values())


@pytest.mark.parametrize(
    ("request_body", "expected_status"),
    [
        ({"product_ids": []}, 400),
        ({"product_ids": "not-a-list"}, 400),
        ({"product_ids": [1], "k": 101}, 400),
        (ValueError("malformed json"), 400),
        ([1, 2], 400),
    ],
)
def test_invalid_requests_return_400(request_body, expected_status):
    response = function_app.recommend(Request(request_body))

    assert response.status_code == expected_status


def test_fallback_failure_returns_generic_503(monkeypatch):
    monkeypatch.setenv("SERVING_MODE", "fallback")
    monkeypatch.setattr(function_app, "_load_fallback_model", lambda: (_ for _ in ()).throw(OSError("secret path")))

    response = function_app.recommend(Request({"product_ids": [1]}))

    assert response.status_code == 503
    assert "secret path" not in response.get_body().decode()
