"""Kiểm payload, phân lô và lỗi khi gửi KPI tới Power BI."""

import json
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from common import power_bi_push

PUSH_URL = "https://api.powerbi.com/beta/tenant/datasets/dataset/rows?key=fake-key"
MINUTE = int(datetime(2026, 11, 27, 14, 30, tzinfo=UTC).timestamp())


@pytest.fixture
def connection(monkeypatch):
    factory = Mock()
    factory.return_value.getresponse.return_value.status = 200
    monkeypatch.setattr(power_bi_push, "HTTPSConnection", factory)
    return factory


def test_push_sends_stable_utc_payload_on_retry(connection):
    rows = [(MINUTE, 2, 5), (MINUTE + 60, 3, 7)]

    power_bi_push.push_kpi_rows(rows, PUSH_URL)
    power_bi_push.push_kpi_rows(rows, PUSH_URL)

    conn = connection.return_value
    first, second = conn.request.call_args_list
    assert first == second
    assert first.args == ("POST", "/beta/tenant/datasets/dataset/rows?key=fake-key")
    assert first.kwargs["headers"] == {"Content-Type": "application/json"}
    assert json.loads(first.kwargs["body"]) == [
        {"minute": "2026-11-27T14:30:00Z", "orders": 2, "items": 5},
        {"minute": "2026-11-27T14:31:00Z", "orders": 3, "items": 7},
    ]


def test_push_splits_under_streaming_limit(connection):
    rows = [(MINUTE + i * 60, 2, 5) for i in range(500)]

    power_bi_push.push_kpi_rows(iter(rows), PUSH_URL)

    bodies = [call.kwargs["body"] for call in connection.return_value.request.call_args_list]
    assert len(bodies) > 1
    assert all(len(body) <= 14_000 for body in bodies)
    records = [item for body in bodies for item in json.loads(body)]
    assert len(records) == len(rows)
    assert len({record["minute"] for record in records}) == len(rows)
    assert records == sorted(records, key=lambda record: record["minute"])


def test_empty_batch_makes_no_request(connection):
    power_bi_push.push_kpi_rows([], PUSH_URL)
    connection.assert_not_called()


@pytest.mark.parametrize(
    "url",
    [
        PUSH_URL.replace("https:", "http:"),
        PUSH_URL.replace("api.powerbi.com", "api.powerbi.com.example"),
        PUSH_URL.replace("api.powerbi.com", "user@api.powerbi.com"),
        PUSH_URL.replace("api.powerbi.com", "api.powerbi.com:80"),
        PUSH_URL.replace("key=", "wrong="),
    ],
)
def test_invalid_target_does_not_make_request(connection, url):
    with pytest.raises(ValueError, match="Push URL") as error:
        power_bi_push.push_kpi_rows([(MINUTE, 2, 5)], url)
    assert "fake-key" not in str(error.value)
    connection.assert_not_called()


@pytest.mark.parametrize("status", [302, 403, 429, 500])
def test_http_error_never_follows_redirects_or_discloses_key(connection, status):
    conn = connection.return_value
    conn.getresponse.return_value.status = status

    with pytest.raises(RuntimeError, match=f"HTTP {status}") as error:
        power_bi_push.push_kpi_rows([(MINUTE, 2, 5)], PUSH_URL)

    conn.request.assert_called_once()
    assert "fake-key" not in str(error.value)


def test_transport_failure_sanitizes_original_error(connection):
    connection.return_value.request.side_effect = TimeoutError(PUSH_URL)

    with pytest.raises(RuntimeError, match="Chưa xác định") as error:
        power_bi_push.push_kpi_rows([(MINUTE, 2, 5)], PUSH_URL)

    assert "fake-key" not in str(error.value)
    assert error.value.__suppress_context__
