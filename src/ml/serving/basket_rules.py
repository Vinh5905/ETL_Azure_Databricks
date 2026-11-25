"""Model MLflow pyfunc phục vụ gợi ý từ luật kết hợp đã xuất."""

from typing import Any

import mlflow.pyfunc
import pandas as pd

from ml.serving.basket_core import BasketCore


class BasketRules(BasketCore, mlflow.pyfunc.PythonModel):
    """Bọc BasketCore thành pyfunc: nạp artifacts khi load model, predict theo lô giỏ."""

    def load_context(self, context: Any) -> None:
        """Nạp artifacts Parquet: rules và item metadata."""
        self.load(context.artifacts["rules"], context.artifacts["items"])

    def predict(self, context, model_input: pd.DataFrame, params: dict | None = None) -> pd.DataFrame:
        """Nhận một list product_ids trên mỗi dòng và trả về các dòng gợi ý có xếp hạng."""
        if "product_ids" not in model_input:
            raise ValueError("model_input phải có cột product_ids")
        params = params or {}
        k = int(params.get("k", 10))
        if k < 1 or k > 100:
            raise ValueError("k phải nằm trong [1, 100]")
        rows = []
        for row_number, product_ids in enumerate(model_input["product_ids"]):
            ids = [] if product_ids is None else list(product_ids)
            rows.extend(
                {"input_row": row_number, "rank": rank, **recommendation}
                for rank, recommendation in enumerate(self._recommend(ids, k), start=1)
            )
        columns = ["input_row", "rank", "product_id", "product_name", "confidence", "lift", "source"]
        return pd.DataFrame(rows, columns=columns).astype({"confidence": "float64", "lift": "float64"})
