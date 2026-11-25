"""Lõi tra luật và xếp hạng của BasketRules, không phụ thuộc MLflow.

Model pyfunc (basket_rules.py) và Azure Function phương án B (docs/ds/05 §3.3) dùng chung lớp này,
nên Function không phải cài MLflow mà vẫn ra đúng thứ tự gợi ý như endpoint.
"""

from typing import Any

import pandas as pd

from ml import recommendations


class BasketCore:
    """Tra luật theo vế trái, xếp hạng confidence/lift và bù món phổ biến."""

    def load(self, rules_path: str, items_path: str) -> None:
        """Nạp hai file Parquet: rules (antecedent, item, confidence, lift) và items (idx, product_id, product_name)."""
        rules = pd.read_parquet(rules_path)
        items = pd.read_parquet(items_path)
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
