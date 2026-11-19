"""Model Python thuần để phục vụ gợi ý từ luật kết hợp đã xuất."""

from typing import Any

import mlflow.pyfunc
import pandas as pd

from ml import recommendations


class BasketRules(mlflow.pyfunc.PythonModel):
    """Tra luật theo vế trái, xếp hạng confidence/lift và bù món phổ biến."""

    def load_context(self, context: Any) -> None:
        """Nạp artifacts Parquet: rules và item metadata."""
        rules = pd.read_parquet(context.artifacts["rules"])
        items = pd.read_parquet(context.artifacts["items"])
        self.rules: list[dict[str, Any]] = []
        for antecedent, item, confidence, lift in rules[["antecedent", "item", "confidence", "lift"]].itertuples(
            index=False, name=None
        ):
            self.rules.append(
                {
                    "antecedent": [int(value) for value in antecedent],
                    "consequent": [int(item)],
                    "confidence": float(confidence),
                    "lift": float(lift),
                }
            )
        self.pid2idx = {int(pid): int(idx) for pid, idx in zip(items["product_id"], items["idx"], strict=True)}
        self.idx2pid = {idx: pid for pid, idx in self.pid2idx.items()}
        self.idx2name = {int(idx): str(name) for idx, name in zip(items["idx"], items["product_name"], strict=True)}
        # Giữ cả danh mục theo idx (idx nhỏ = phổ biến nhất): cắt ngắn thì giỏ không có luật
        # với k lớn sẽ không đủ k món. Vòng bù dừng ngay khi đủ k nên không tốn thêm.
        self.popular = [int(idx) for idx in items.sort_values("idx")["idx"]]

    def _recommend(self, product_ids: list[int], k: int) -> list[dict[str, Any]]:
        if k < 1:
            raise ValueError("k phải >= 1")
        ctx = sorted({self.pid2idx[int(pid)] for pid in product_ids if int(pid) in self.pid2idx})
        return [
            {
                "product_id": self.idx2pid[item],
                "product_name": self.idx2name[item],
                "confidence": confidence,
                "lift": lift,
                "source": "rule" if confidence is not None else "popular",
            }
            for item, confidence, lift in recommendations.recommend_scored_indices(ctx, self.rules, self.popular, k=k)
        ]

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
