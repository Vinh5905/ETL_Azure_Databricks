"""Azure Function proxy cho gợi ý sản phẩm từ Databricks."""

import json
import os
import sys
import tempfile
from pathlib import Path

import azure.functions as func
import requests

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
_fallback_model = None


def _credential():
    from azure.identity import ManagedIdentityCredential

    client_id = os.environ.get("AZURE_CLIENT_ID")
    return ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()


def _load_fallback_model():
    """Đọc Parquet một lần bằng managed identity; dùng cùng lõi xếp hạng với model pyfunc (không cần MLflow)."""
    global _fallback_model
    if _fallback_model is not None:
        return _fallback_model
    from azure.storage.blob import BlobServiceClient

    account = os.environ["STORAGE_ACCOUNT_NAME"]
    container = os.environ.get("DS_CONTAINER_NAME", "ds-data")
    rules_blob = os.environ.get("RULES_BLOB_NAME", "serving/rules.parquet")
    items_blob = os.environ.get("ITEMS_BLOB_NAME", "serving/items.parquet")
    service = BlobServiceClient(f"https://{account}.blob.core.windows.net", credential=_credential())
    client = service.get_container_client(container)
    with tempfile.TemporaryDirectory() as temp_dir:
        rules_path = Path(temp_dir) / "rules.parquet"
        items_path = Path(temp_dir) / "items.parquet"
        rules_path.write_bytes(client.download_blob(rules_blob).readall())
        items_path.write_bytes(client.download_blob(items_blob).readall())
        # Hỗ trợ chạy local từ src/serving_func và gói deploy có kèm thư mục src/ml.
        app_root = Path(__file__).resolve().parents[1]
        source_roots = (app_root, app_root / "src", Path(__file__).resolve().parents[2] / "src")
        for source_root in source_roots:
            if (source_root / "ml" / "serving" / "basket_core.py").is_file():
                if str(source_root) not in sys.path:
                    sys.path.insert(0, str(source_root))
                break
        else:
            raise RuntimeError("Function package thiếu gói ml (ml/serving/basket_core.py, ml/recommendations.py)")
        from ml.serving.basket_core import BasketCore

        model = BasketCore()
        model.load(str(rules_path), str(items_path))
        # Giữ dữ liệu trong bộ nhớ trước khi thư mục tạm bị xóa.
        _fallback_model = model
    return _fallback_model


def _recommend(body: dict) -> tuple[int, dict]:
    raw_ids = body.get("product_ids", [])
    if not isinstance(raw_ids, list):
        return 400, {"error": "product_ids phải là một danh sách số nguyên"}
    try:
        ids = [int(value) for value in raw_ids[:50]]
        k = int(body.get("k", 10))
    except (TypeError, ValueError, OverflowError):
        return 400, {"error": "product_ids và k phải là số nguyên hợp lệ"}
    if not ids:
        return 400, {"error": "product_ids không được rỗng"}
    if k < 1 or k > 100:
        return 400, {"error": "k phải nằm trong [1, 100]"}

    if os.environ.get("SERVING_MODE", "endpoint").lower() == "fallback":
        try:
            recommendations = [
                {"rank": rank, **recommendation}
                for rank, recommendation in enumerate(_load_fallback_model()._recommend(ids, k), start=1)
            ]
        except Exception:  # noqa: BLE001 - không trả nội dung lỗi chứa thông tin hạ tầng
            return 503, {"error": "Không tải được model gợi ý"}
        return 200, {"recommendations": recommendations}

    try:
        host = os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
        endpoint = os.environ.get("ENDPOINT_NAME", "instacart-basket-rules")
        token = _credential().get_token(DATABRICKS_SCOPE).token
        response = requests.post(
            f"https://{host}/serving-endpoints/{endpoint}/invocations",
            headers={"Authorization": f"Bearer {token}"},
            json={"dataframe_records": [{"product_ids": ids}], "params": {"k": k}},
            timeout=60,
        )
        response.raise_for_status()
        predictions = response.json().get("predictions", [])
        recommendations = [row for row in predictions if row.get("input_row") == 0]
        return 200, {"recommendations": recommendations}
    except Exception:  # noqa: BLE001 - không trả nội dung lỗi chứa thông tin hạ tầng
        return 502, {"error": "Không gọi được dịch vụ gợi ý"}


@app.route(route="recommend", methods=["POST"])
def recommend(req: func.HttpRequest) -> func.HttpResponse:
    """Xác thực đầu vào rồi gọi endpoint hoặc phương án dự phòng trên Blob Storage."""
    try:
        body = req.get_json()
    except (ValueError, TypeError):
        return func.HttpResponse(
            json.dumps({"error": "Body phải là JSON object"}), status_code=400, mimetype="application/json"
        )
    if not isinstance(body, dict):
        return func.HttpResponse(
            json.dumps({"error": "Body phải là JSON object"}), status_code=400, mimetype="application/json"
        )
    status, result = _recommend(body)
    return func.HttpResponse(json.dumps(result, ensure_ascii=False), status_code=status, mimetype="application/json")
