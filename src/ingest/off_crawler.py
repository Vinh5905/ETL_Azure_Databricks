"""Crawler OFF có giới hạn tốc độ, journal phục hồi và landing bất biến."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import re
import sys
import time
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

SEARCH = "https://search.openfoodfacts.org/search"
FIELDS = "code,product_name,brands,categories_tags,countries_tags,nutriscore_grade,nova_group,nutriments,image_url"
USER_AGENT = "InstacartCourseProject/1.0 (https://github.com/Vinh5905/ETL_Azure_Databricks)"


def canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def batch_id(products: list[dict], max_attempts: int = 5) -> str:
    """Tên lô phụ thuộc sản phẩm/tên tra/lần thử, không phụ thuộc ngày hay run ID."""
    ids = [product["product_id"] for product in products]
    if not products or len(ids) != len(set(ids)) or any(type(pid) is not int or pid <= 0 for pid in ids):
        raise ValueError("Lô phải có product_id nguyên dương, duy nhất")
    payload = {
        "version": 1,
        "products": sorted(products, key=lambda row: row["product_id"]),
        "max_attempts": max_attempts,
    }
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def hit_record(product: dict, hit: dict) -> dict:
    """Giữ payload nguồn, nhưng metadata Instacart do crawler quản lý."""
    if not isinstance(hit, dict):
        raise ValueError("Hit OFF phải là object")
    code = str(hit.get("code", ""))
    if not re.fullmatch(r"[0-9]{1,32}", code):
        raise ValueError("Mã OFF không hợp lệ")
    record = {**hit, "code": code, "_product_id": product["product_id"], "_search_query": product["product_name"]}
    canonical_json(record)  # Từ chối NaN hoặc payload không thể ghi JSON hợp lệ.
    return record


class RateLimiter:
    """Giãn mọi request, kể cả retry; chờ đầu phiên để không burst khi task khởi động lại."""

    def __init__(self, interval=6.5, *, clock=time.monotonic, sleep=time.sleep):
        if interval < 0 or not math.isfinite(interval):
            raise ValueError("Khoảng cách request không hợp lệ")
        self.interval, self.clock, self.sleep = interval, clock, sleep
        self.next_at = clock() + interval

    def wait(self):
        delay = self.next_at - self.clock()
        if delay > 0:
            self.sleep(delay)
        self.next_at = self.clock() + self.interval


def retry_delay(value: str | None, attempt: int, *, now=None) -> float:
    """Retry-After có thể là số giây hoặc HTTP date; fallback tăng dần."""
    fallback = min(10 * 2**attempt, 300)
    if value is None:
        return fallback
    try:
        delay = float(value)
        if not math.isfinite(delay):
            raise ValueError("Retry-After không hữu hạn")
    except ValueError:
        try:
            until = parsedate_to_datetime(value)
            if until.tzinfo is None:
                until = until.replace(tzinfo=dt.UTC)
            delay = (until - (now or dt.datetime.now(dt.UTC))).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return fallback
    return max(delay, 0)


class PermanentHTTPError(RuntimeError):
    """Lỗi HTTP không nên thử lại trong lô tiếp theo."""


class ServiceAccessError(PermanentHTTPError):
    """401/403 là lỗi truy cập dịch vụ; dừng lô thay vì đánh failed mọi sản phẩm."""


class OffClient:
    def __init__(self, session=None, limiter=None, *, sleep=time.sleep, max_requests=5):
        if not 1 <= max_requests <= 5:
            raise ValueError("Số request mỗi lần tra phải nằm trong 1–5")
        self.session = session or requests.Session()
        self.limiter = limiter or RateLimiter()
        self.sleep, self.max_requests = sleep, max_requests
        self.requests_sent = 0

    def get(self, url, *, limiter=None, **kwargs):
        """Không theo redirect; response lỗi được đóng trước khi đợi/retry."""
        for attempt in range(self.max_requests):
            (limiter or self.limiter).wait()
            self.requests_sent += 1
            try:
                response = self.session.get(
                    url, headers={"User-Agent": USER_AGENT}, timeout=(10, 30), allow_redirects=False, **kwargs
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt + 1 == self.max_requests:
                    raise
                self.sleep(retry_delay(None, attempt))
                continue
            if response.status_code == 429 or 500 <= response.status_code <= 599:
                delay = retry_delay(response.headers.get("Retry-After"), attempt)
                response.close()
                if attempt + 1 == self.max_requests:
                    raise requests.exceptions.RetryError("OFF hết lượt retry HTTP")
                self.sleep(delay)
                continue
            if response.status_code != 200:
                status = response.status_code
                response.close()
                if status in (401, 403):
                    raise ServiceAccessError(f"OFF trả HTTP {status}")
                raise PermanentHTTPError(f"OFF trả HTTP {status}")
            return response
        raise AssertionError("Không có lượt request")

    def search(self, name: str) -> list[dict]:
        # Escape cú pháp Lucene để tên hàng không trở thành biểu thức tìm kiếm tùy ý.
        query = re.sub(r'([+\-!(){}\[\]^"~*?:\\/|&])', r"\\\1", name)
        response = self.get(SEARCH, params={"q": query, "fields": FIELDS, "page_size": 5, "langs": "en"})
        try:
            payload = response.json()
        finally:
            response.close()
        if not isinstance(payload, dict) or payload.get("timed_out"):
            raise ValueError("Phản hồi OFF không hợp lệ hoặc đã timeout")
        hits = payload.get("hits")
        if not isinstance(hits, list) or len(hits) > 5 or any(not isinstance(hit, dict) for hit in hits):
            raise ValueError("Phản hồi OFF thiếu danh sách hits hợp lệ")
        return hits


def write_atomic(path: Path, content: bytes):
    """Rename cùng volume, không fallback copy; nếu filesystem không hỗ trợ thì dừng."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + ".pending")
    pending.write_bytes(content)
    pending.replace(path)


def publish(stage: Path, final: Path, content: bytes):
    """Không thay nội dung file đã công bố; journal cho phép tiếp tục sau lỗi MERGE."""
    if final.exists():
        if final.read_bytes() != content:
            raise RuntimeError("File landing đã tồn tại với nội dung khác")
        return
    write_atomic(stage, content)
    final.parent.mkdir(parents=True, exist_ok=True)
    stage.rename(final)


def run_batch(root: Path, state, client: OffClient, *, n=300, top=2000, max_attempts=5, now=None) -> dict:
    """Một writer: phục hồi active plan trước khi chọn lô mới, publish trước MERGE."""
    if not 1 <= n <= top <= 2000 or not 1 <= max_attempts <= 5:
        raise ValueError("Cần 1 ≤ batch size ≤ top ≤ 2000 và 1–5 lần thử")
    control = root / "_staging/off"
    active = control / "active.json"
    if active.exists():
        plan = json.loads(active.read_bytes())
    else:
        products = state.pick_batch(n, top, max_attempts)
        if not products:
            return {"selected": 0, "invalid_hits": 0}
        plan = {"products": products, "max_attempts": max_attempts}
        write_atomic(active, canonical_json(plan))
    products = plan["products"]
    identity = batch_id(products, plan["max_attempts"])
    directory = control / identity
    results = []
    records = []
    for product in products:
        cache = directory / f"result_{product['product_id']}.json"
        if cache.exists():
            result = json.loads(cache.read_bytes())
        else:
            attempt = product["attempts"] + 1
            result = {
                "product_id": product["product_id"],
                "attempts": attempt,
                "updated_at": (now or dt.datetime.now(dt.UTC)).isoformat(),
                "last_error": None,
                "records": [],
                "invalid_hits": 0,
            }
            try:
                hits = client.search(product["product_name"])
                for hit in hits:
                    try:
                        result["records"].append(hit_record(product, hit))
                    except ValueError:
                        result["invalid_hits"] += 1
                result["status"] = "done" if result["records"] else "no_result"
            except (requests.RequestException, ValueError, PermanentHTTPError) as exc:
                if isinstance(exc, ServiceAccessError):
                    raise
                result["status"] = (
                    "failed" if isinstance(exc, PermanentHTTPError) or attempt >= plan["max_attempts"] else "retry"
                )
                # Không lưu URL/response body vào lỗi trạng thái.
                result["last_error"] = type(exc).__name__
            write_atomic(cache, canonical_json(result))
        records.extend(result["records"])
        results.append(result)
    body = b"".join(canonical_json(record) + b"\n" for record in records)
    final = root / "products" / f"batch={identity}" / f"batch_{hashlib.sha256(body).hexdigest()}.json"
    if body:
        publish(directory / "products.json", final, body)
    updates = []
    for result in results:
        codes = [record["code"] for record in result["records"]]
        updates.append(
            {
                **{key: value for key, value in result.items() if key not in ("records", "invalid_hits")},
                "off_code": codes[0] if codes else None,
                "batch_file": str(final) if codes else None,
                "batch_id": identity,
            }
        )
    state.merge(updates)
    active.unlink()
    return {
        "selected": len(results),
        "invalid_hits": sum(row.get("invalid_hits", 0) for row in results),
        **{status: sum(r["status"] == status for r in results) for status in ("done", "no_result", "retry", "failed")},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("crawl", "images"), required=True)
    parser.add_argument("--catalog", default="instacart")
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--state-schema", required=True)
    parser.add_argument("--off-root", required=True)
    parser.add_argument("--source", choices=("bronze", "landing"), default="bronze")
    parser.add_argument("--landing-root")
    parser.add_argument("--batch-size", type=int, default=300)
    parser.add_argument("--top", type=int, default=2000)
    args = parser.parse_args()
    root = Path(args.off_root)
    if not str(root).startswith(f"/Volumes/{args.catalog}/") or ".." in root.parts:
        parser.error("off-root phải nằm trong volume của catalog, không chứa ..")
    if not 1 <= args.batch_size <= args.top <= 2000:
        parser.error("Cần 1 ≤ batch size ≤ top ≤ 2000")
    from pyspark.sql import SparkSession

    from ingest.off_images import download_images
    from ingest.off_state import SparkState

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if args.source == "landing" and (
        not args.landing_root
        or not args.landing_root.startswith(f"/Volumes/{args.catalog}/")
        or ".." in Path(args.landing_root).parts
    ):
        parser.error("Bootstrap cần landing-root trong volume của catalog")
    state = SparkState(
        spark,
        args.catalog,
        args.bronze_schema,
        args.state_schema,
        source=args.source,
        landing_root=args.landing_root,
    )
    with requests.Session() as session:
        client = OffClient(session)
        if args.mode == "crawl":
            summary = run_batch(root, state, client, n=args.batch_size, top=args.top)
            summary["search_requests"] = client.requests_sent
        else:
            summary = download_images(root, state.batch_files(), client)
            summary["image_requests"] = client.requests_sent
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    # Spark Python Task có thể gọi exec(compile(...)) mà không đặt __file__.
    script_path = Path(globals().get("__file__", sys._getframe().f_code.co_filename))
    sys.path.insert(0, str(script_path.resolve().parents[1]))
    # Dùng cùng module với off_images để hai task chia sẻ lớp lỗi HTTP.
    from ingest.off_crawler import main as run_task

    run_task()
