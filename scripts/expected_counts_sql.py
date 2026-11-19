"""In SQL cấu hình số kỳ vọng từ manifest AD-DE-01, không đọc bronze hoặc kết nối cloud."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

FILES = {
    "orders": "orders.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv",
}


def expected_counts(manifest: dict) -> dict[str, int]:
    """Chỉ lấy số tổng hợp; không xuất user_ids hoặc đường dẫn nguồn vào SQL."""
    try:
        values = {key: manifest["rows"][filename] for key, filename in FILES.items()}
        values |= {f"orders_{split}": manifest["orders_by_eval_set"][split] for split in ("prior", "train", "test")}
    except (KeyError, TypeError) as exc:
        raise ValueError("Manifest thiếu rows/orders_by_eval_set của sáu CSV") from exc
    if any(type(value) is not int or value < 0 for value in values.values()):
        raise ValueError("Số kỳ vọng phải là số nguyên không âm, không nhận bool/chuỗi")
    if values["orders"] != sum(values[f"orders_{s}"] for s in ("prior", "train", "test")):
        raise ValueError("Tổng ba split không bằng số orders trong manifest")
    if not values["orders"] or not values["products"]:
        raise ValueError("Không dùng manifest rỗng để nghiệm thu")
    return values


def render(catalog: str, ops_schema: str, profile: str, manifest: dict) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", profile):
        raise ValueError("Profile phải có 1–64 chữ/số/underscore/gạch ngang")
    names = (catalog, ops_schema)
    if any(not name.strip() or any(ord(c) < 32 for c in name) for name in names):
        raise ValueError("Catalog/schema phải khác rỗng, không chứa ký tự điều khiển")
    target = ".".join("`" + name.replace("`", "``") + "`" for name in names) + ".`expected_counts`"
    counts = expected_counts(manifest)
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    rows = ",\n".join(
        f"  ('{profile}', '{metric}', {value}, 'manifest_sha256:{digest}')" for metric, value in sorted(counts.items())
    )
    return f"""-- Chạy init_reconciliation.sql trước. Chỉ cấu hình kỳ vọng, không ghi kết quả đối soát.
MERGE INTO {target} t
USING (SELECT * FROM VALUES
{rows}
 AS v(profile, metric, expected, source)) s
ON t.profile = s.profile AND t.metric = s.metric
WHEN MATCHED THEN UPDATE SET expected = s.expected, source = s.source, configured_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (profile, metric, expected, source, configured_at)
VALUES (s.profile, s.metric, s.expected, s.source, current_timestamp());
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--ops-schema", required=True)
    parser.add_argument("--profile", required=True)
    args = parser.parse_args()
    try:
        print(render(args.catalog, args.ops_schema, args.profile, json.loads(args.manifest.read_text())), end="")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
