"""Gửi KPI theo phút tới Push URL của Power BI Service."""

import json
import re
from collections.abc import Iterable, Iterator
from contextlib import closing
from datetime import UTC, datetime
from http.client import HTTPException, HTTPSConnection
from urllib.parse import parse_qs, urlsplit

_HOST = "api.powerbi.com"
_MAX_PAYLOAD_BYTES = 14_000


def _payloads(rows: Iterable[tuple[int, int, int]]) -> Iterator[bytes]:
    payload = bytearray(b"[")
    for minute_epoch, orders, items in rows:
        minute = datetime.fromtimestamp(minute_epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        row = json.dumps({"minute": minute, "orders": orders, "items": items}, separators=(",", ":")).encode("utf-8")
        if len(row) + 2 > _MAX_PAYLOAD_BYTES:
            raise ValueError("Một dòng KPI vượt giới hạn payload Power BI.")
        if len(payload) > 1 and len(payload) + len(row) + 2 > _MAX_PAYLOAD_BYTES:
            yield bytes(payload) + b"]"
            payload = bytearray(b"[")
        if len(payload) > 1:
            payload.extend(b",")
        payload.extend(row)
    if len(payload) > 1:
        yield bytes(payload) + b"]"


def _request_target(push_url: str) -> str:
    invalid = "Push URL của Power BI không hợp lệ."
    try:
        url = urlsplit(push_url.strip())
        if (
            url.scheme != "https"
            or url.hostname != _HOST
            or url.port not in (None, 443)
            or url.username is not None
            or url.fragment
            or not re.fullmatch(r"/beta/[^/]+/datasets/[^/]+/rows/?", url.path)
            or not parse_qs(url.query).get("key")
        ):
            raise ValueError(invalid)
    except ValueError:
        raise ValueError(invalid) from None
    return f"{url.path}?{url.query}"


def push_kpi_rows(rows: Iterable[tuple[int, int, int]], push_url: str) -> None:
    """Gửi các giá trị theo phút. Lượt thử lại dùng cùng payload."""
    target = _request_target(push_url)
    for payload in _payloads(rows):
        try:
            with closing(HTTPSConnection(_HOST, timeout=10)) as conn:
                conn.request("POST", target, body=payload, headers={"Content-Type": "application/json"})
                response = conn.getresponse()
                status = response.status
        except (HTTPException, OSError):
            raise RuntimeError("Chưa xác định được kết quả POST KPI tới Power BI.") from None
        if not 200 <= status < 300:
            raise RuntimeError(f"Power BI trả HTTP {status} khi gửi KPI.")
