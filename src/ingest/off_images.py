"""Tải các ảnh OFF còn thiếu từ file JSONL đã công bố, không phụ thuộc bronze.off_products."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from ingest.off_crawler import (
    PermanentHTTPError,
    RateLimiter,
    ServiceAccessError,
    canonical_json,
    publish,
    write_atomic,
)

MAX_IMAGE_BYTES = 10 * 1024 * 1024


def validate_jpeg(content):
    if not content.startswith(b"\xff\xd8\xff") or not content.endswith(b"\xff\xd9"):
        raise ValueError("Ảnh không phải JPEG hoàn chỉnh")


def image_candidates(root: Path, files):
    candidates = {}
    products = (root / "products").resolve()
    for filename in sorted(set(files)):
        path = Path(filename).resolve()
        if not path.is_relative_to(products) or path.suffix != ".json":
            raise ValueError("batch_file không thuộc nhánh products của OFF")
        if hashlib.sha256(path.read_bytes()).hexdigest() != path.stem.removeprefix("batch_"):
            raise ValueError("Checksum JSONL không khớp tên file")
        for line in path.read_text().splitlines():
            record = json.loads(line)
            code, url = record["code"], record.get("image_url")
            if url:
                if not isinstance(code, str) or not code.isascii() or not code.isdigit() or len(code) > 32:
                    raise ValueError("Mã OFF không hợp lệ")
                candidates.setdefault(code, url)
    return sorted(candidates.items())


def _download(client, url: str, limiter) -> bytes:
    if not isinstance(url, str):
        raise ValueError("URL ảnh không hợp lệ")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "images.openfoodfacts.org"
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
    ):
        raise ValueError("URL ảnh không thuộc HTTPS images.openfoodfacts.org")
    response = client.get(url, limiter=limiter, stream=True)
    try:
        if response.headers.get("Content-Type", "").split(";")[0].lower() not in ("image/jpeg", "image/jpg"):
            raise ValueError("Content-Type ảnh không phải JPEG")
        content = bytearray()
        for chunk in response.iter_content(chunk_size=65536):
            content.extend(chunk)
            if len(content) > MAX_IMAGE_BYTES:
                raise ValueError("Ảnh vượt giới hạn 10 MiB")
        payload = bytes(content)
        validate_jpeg(payload)
        return payload
    finally:
        response.close()


def download_images(root: Path, files, client, *, limiter=None):
    # CDN ảnh tách khỏi endpoint search; giãn tối đa 60 request/phút, retry vẫn qua limiter.
    limiter = limiter or RateLimiter(1.0)
    (root / "images").mkdir(parents=True, exist_ok=True)
    staging = root / "_staging/images"
    journal = staging / "failed.json"
    failures = json.loads(journal.read_bytes()) if journal.exists() else {}
    counts = {"downloaded": 0, "existing": 0, "skipped": 0, "failed": 0}
    for code, url in image_candidates(root, files):
        final = root / "images" / f"{code}.jpg"
        url_hash = hashlib.sha256(canonical_json(url)).hexdigest()
        if failures.get(code) == url_hash and not final.exists():
            counts["skipped"] += 1
            continue
        try:
            if final.exists():
                validate_jpeg(final.read_bytes())
                counts["existing"] += 1
                continue
            payload = _download(client, url, limiter)
        except ServiceAccessError:
            raise
        except (PermanentHTTPError, ValueError) as exc:
            failures[code] = url_hash
            write_atomic(journal, canonical_json(failures))
            counts["skipped" if isinstance(exc, PermanentHTTPError) else "failed"] += 1
            continue
        publish(staging / f"{code}.jpg", final, payload)
        counts["downloaded"] += 1
    return counts
