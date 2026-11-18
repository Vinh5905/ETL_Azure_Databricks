"""Model Python thuần để phục vụ gợi ý từ luật kết hợp đã xuất."""

from itertools import combinations
from typing import Any

import pandas as pd

try:
    import mlflow.pyfunc

    _PythonModel = mlflow.pyfunc.PythonModel
except ImportError:  # cho phép kiểm thử logic local khi chưa cài MLflow

    class _PythonModel:
        pass


class BasketRules(_PythonModel):
    """Tra luật theo vế trái, xếp hạng confidence/lift và bù món phổ biến."""

    def load_context(self, context: Any) -> None:
        """Nạp artifacts Parquet: rules và item metadata."""
        rules = pd.read_parquet(context.artifacts["rules"])
        items = pd.read_parquet(context.artifacts["items"])
        self.by_antecedent: dict[tuple[int, ...], list[tuple[int, float, float]]] = {}
        for antecedent, item, confidence, lift in rules[["antecedent", "item", "confidence", "lift"]].itertuples(
            index=False, name=None
        ):
            key = tuple(sorted(int(value) for value in antecedent))
            self.by_antecedent.setdefault(key, []).append((int(item), float(confidence), float(lift)))
        self.pid2idx = {int(pid): int(idx) for pid, idx in zip(items["product_id"], items["idx"], strict=True)}
        self.idx2pid = {idx: pid for pid, idx in self.pid2idx.items()}
        self.idx2name = {int(idx): str(name) for idx, name in zip(items["idx"], items["product_name"], strict=True)}
        self.popular = [int(idx) for idx in items.sort_values("idx")["idx"].head(50)]
        self.max_left = max((len(antecedent) for antecedent in self.by_antecedent), default=1)

    def _recommend(self, product_ids: list[int], k: int) -> list[dict[str, Any]]:
        if k < 1:
            raise ValueError("k phải >= 1")
        ctx = sorted({self.pid2idx[int(pid)] for pid in product_ids if int(pid) in self.pid2idx})
        best: dict[int, tuple[float, float]] = {}
        for size in range(1, min(self.max_left, len(ctx)) + 1):
            for antecedent in combinations(ctx, size):
                for item, confidence, lift in self.by_antecedent.get(antecedent, ()):
                    if item not in ctx:
                        best[item] = max(best.get(item, (-1.0, -1.0)), (confidence, lift))
        ranked = sorted(best, key=lambda item: (-best[item][0], -best[item][1], item))[:k]
        result = [
            {
                "product_id": self.idx2pid[item],
                "product_name": self.idx2name[item],
                "confidence": best[item][0],
                "lift": best[item][1],
                "source": "rule",
            }
            for item in ranked
        ]
        for item in self.popular:
            if len(result) >= k:
                break
            if item not in ctx and item not in best:
                result.append(
                    {
                        "product_id": self.idx2pid[item],
                        "product_name": self.idx2name[item],
                        "confidence": None,
                        "lift": None,
                        "source": "popular",
                    }
                )
        return result

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
        return pd.DataFrame(rows, columns=columns)
