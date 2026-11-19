"""Kiểm rate limit, phục hồi publish/MERGE và cache ảnh bằng filesystem/HTTP giả lập."""

from __future__ import annotations

import datetime as dt
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from ingest.off_crawler import (
    OffClient,
    PermanentHTTPError,
    RateLimiter,
    ServiceAccessError,
    batch_id,
    canonical_json,
    hit_record,
    publish,
    retry_delay,
    run_batch,
)
from ingest.off_images import download_images


class Clock:
    def __init__(self):
        self.seconds = 0
        self.sleeps = []

    def now(self):
        return self.seconds

    def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.seconds += seconds


class Response:
    def __init__(self, status=200, payload=None, headers=None, content=b""):
        self.status_code, self.payload = status, payload
        self.headers, self.content = headers or {}, content
        self.closed = False

    def json(self):
        return self.payload

    def close(self):
        self.closed = True

    def iter_content(self, chunk_size):
        yield self.content


class Session:
    def __init__(self, responses, clock):
        self.responses, self.clock = iter(responses), clock
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((self.clock.now(), url, kwargs))
        result = next(self.responses)
        if isinstance(result, BaseException):
            raise result
        return result


def http_client(responses):
    clock = Clock()
    session = Session(responses, clock)
    client = OffClient(session, RateLimiter(clock=clock.now, sleep=clock.sleep), sleep=clock.sleep)
    return client, session, clock


class State:
    """Mô hình trạng thái cho test luồng; không giả định đã kiểm MERGE Delta thật."""

    def __init__(self, products):
        self.products = products
        self.rows = {}
        self.fail_merge = False

    def pick_batch(self, n, top, max_attempts):
        return [
            {**product, "attempts": self.rows.get(product["product_id"], {}).get("attempts", 0)}
            for product in self.products[:top]
            if product["product_id"] not in self.rows
            or (
                self.rows[product["product_id"]]["status"] == "retry"
                and self.rows[product["product_id"]]["attempts"] < max_attempts
            )
        ][:n]

    def merge(self, rows):
        if self.fail_merge:
            raise RuntimeError("lỗi MERGE tự dựng")
        for row in rows:
            previous = self.rows.get(row["product_id"])
            if previous is None or (previous["status"] == "retry" and row["attempts"] > previous["attempts"]):
                self.rows[row["product_id"]] = row


class Search:
    def __init__(self, results):
        self.results = iter(results)
        self.names = []

    def search(self, name):
        self.names.append(name)
        result = next(self.results)
        if isinstance(result, BaseException):
            raise result
        return result


def products(n=2):
    return [{"product_id": pid, "product_name": f"Tea {pid}"} for pid in range(1, n + 1)]


def test_batch_id_is_order_independent_and_changes_with_query_or_attempt():
    rows = [{**row, "attempts": 0} for row in products()]
    assert batch_id(rows) == batch_id(rows[::-1])
    assert batch_id(rows) != batch_id([{**row, "attempts": 1} for row in rows])
    assert batch_id(rows) != batch_id([{**row, "product_name": "New tea"} for row in rows])


@pytest.mark.parametrize("rows", [[], [{"product_id": -1}], [{"product_id": 1}, {"product_id": 1}]])
def test_batch_id_rejects_invalid_ids(rows):
    with pytest.raises(ValueError):
        batch_id(rows)


def test_record_preserves_nested_payload_and_overrides_untrusted_metadata():
    hit = {"code": "00123", "nutriments": {"sugars_100g": "1.2"}, "_product_id": 999, "_search_query": "wrong"}
    result = hit_record(products(1)[0], hit)
    assert result["code"] == "00123" and result["nutriments"] == hit["nutriments"]
    assert result["_product_id"] == 1 and result["_search_query"] == "Tea 1"
    assert hit["_product_id"] == 999


@pytest.mark.parametrize("hit", [{}, {"code": "../x"}, {"code": "123", "x": float("nan")}, []])
def test_record_rejects_invalid_payload(hit):
    with pytest.raises(ValueError):
        hit_record(products(1)[0], hit)


def test_http_retries_429_and_503_and_spaces_every_request():
    responses = [Response(429, headers={"Retry-After": "1"}), Response(503), Response(payload={"hits": []})]
    client, session, _ = http_client(responses)
    assert client.search('Tea "Green" + Lime') == []
    assert client.requests_sent == len(session.calls) == 3
    times = [call[0] for call in session.calls]
    assert times[0] >= 6.5
    assert all(right - left >= 6.5 for left, right in zip(times, times[1:], strict=False))
    assert all(response.closed for response in responses)
    options = session.calls[-1][2]
    assert options["allow_redirects"] is False and options["timeout"] == (10, 30)
    assert "User-Agent" in options["headers"]
    assert options["params"]["page_size"] == 5
    assert '\\"Green\\"' in options["params"]["q"]


def test_connection_timeout_is_retried_and_maximum_is_bounded():
    client, session, _ = http_client([requests.Timeout()] * 5)
    with pytest.raises(requests.Timeout):
        client.search("Tea")
    assert len(session.calls) == 5


def test_429_exhaustion_does_not_become_no_result():
    client, session, _ = http_client([Response(429)] * 5)
    with pytest.raises(requests.exceptions.RetryError):
        client.search("Tea")
    assert len(session.calls) == 5


@pytest.mark.parametrize("status", [301, 400, 403, 404])
def test_http_permanent_error_does_not_retry(status):
    response = Response(status)
    client, session, _ = http_client([response])
    with pytest.raises(PermanentHTTPError):
        client.search("Tea")
    assert len(session.calls) == 1 and response.closed


@pytest.mark.parametrize("payload", [{}, {"hits": None}, {"hits": [1]}, {"hits": [], "timed_out": True}])
def test_bad_response_never_marks_no_result(payload):
    client, _, _ = http_client([Response(payload=payload)])
    with pytest.raises(ValueError):
        client.search("Tea")


def test_retry_after_supports_http_date_and_invalid_values():
    now = dt.datetime(2026, 11, 17, tzinfo=dt.UTC)
    assert retry_delay("Tue, 17 Nov 2026 00:02:00 GMT", 0, now=now) == 120
    assert retry_delay("bad", 2) == 40
    assert retry_delay("NaN", 0) == 10
    assert retry_delay("-1", 0) == 0


def test_replay_does_not_search_terminal_products_and_no_empty_json_is_published(tmp_path):
    state = State(products())
    client = Search([[{"code": "00123"}], []])
    summary = run_batch(tmp_path, state, client)
    assert summary == {"selected": 2, "done": 1, "no_result": 1, "retry": 0, "failed": 0, "invalid_hits": 0}
    before = {path: path.read_bytes() for path in (tmp_path / "products").rglob("*.json")}
    assert len(before) == 1 and next(iter(before.values())).count(b"\n") == 1
    replay, session, _ = http_client([])
    assert run_batch(tmp_path, state, replay) == {"selected": 0, "invalid_hits": 0}
    assert replay.requests_sent == len(session.calls) == 0
    assert before == {path: path.read_bytes() for path in (tmp_path / "products").rglob("*.json")}
    assert len(client.names) == 2
    assert state.rows[2]["batch_file"] is None


def test_all_no_result_updates_state_without_empty_source_file(tmp_path):
    state = State(products())
    assert run_batch(tmp_path, state, Search([[], []]))["no_result"] == 2
    assert not list((tmp_path / "products").rglob("*.json"))


def test_publish_then_merge_failure_recovers_without_refetch_or_new_file(tmp_path):
    state = State(products())
    state.fail_merge = True
    client = Search([[{"code": "00123"}], []])
    with pytest.raises(RuntimeError, match="MERGE"):
        run_batch(tmp_path, state, client)
    before = {path: path.read_bytes() for path in (tmp_path / "products").rglob("*.json")}
    state.fail_merge = False
    assert run_batch(tmp_path, state, Search([]))["done"] == 1
    assert before == {path: path.read_bytes() for path in (tmp_path / "products").rglob("*.json")}
    assert all(row["attempts"] == 1 for row in state.rows.values())


def test_crash_after_one_search_resumes_remaining_products_only(tmp_path):
    state = State(products())
    with pytest.raises(KeyboardInterrupt):
        run_batch(tmp_path, state, Search([[{"code": "00123"}], KeyboardInterrupt()]))
    client = Search([[]])
    assert run_batch(tmp_path, state, client)["selected"] == 2
    assert client.names == ["Tea 2"]


def test_crash_after_merge_replays_without_incrementing_attempts(tmp_path):
    state = State(products(1))
    merge = state.merge

    def merge_then_crash(rows):
        merge(rows)
        raise RuntimeError("đã commit, task bị ngắt")

    state.merge = merge_then_crash
    with pytest.raises(RuntimeError, match="commit"):
        run_batch(tmp_path, state, Search([[{"code": "00123"}]]))
    state.merge = merge
    assert run_batch(tmp_path, state, Search([]))["done"] == 1
    assert state.rows[1]["attempts"] == 1


def test_failed_publication_recovers_cached_responses_without_search(tmp_path, monkeypatch):
    def fail_rename(*args):
        raise OSError("rename tạm lỗi")

    state = State(products(1))
    with monkeypatch.context() as patch:
        patch.setattr(Path, "rename", fail_rename)
        with pytest.raises(OSError):
            run_batch(tmp_path, state, Search([[{"code": "00123"}]]))
    assert not state.rows
    assert run_batch(tmp_path, state, Search([]))["done"] == 1


def test_retry_attempts_increment_and_stop_after_limit(tmp_path):
    state = State(products(1))
    for attempt in range(1, 6):
        run_batch(tmp_path, state, Search([requests.Timeout()]))
        assert state.rows[1]["attempts"] == attempt
        assert state.rows[1]["status"] == ("retry" if attempt < 5 else "failed")
    assert run_batch(tmp_path, state, Search([])) == {"selected": 0, "invalid_hits": 0}


def test_access_block_keeps_plan_without_poisoning_product_state(tmp_path):
    state = State(products())
    with pytest.raises(ServiceAccessError):
        run_batch(tmp_path, state, Search([[{"code": "00123"}], ServiceAccessError("HTTP 403")]))
    assert not state.rows
    client = Search([[]])
    assert run_batch(tmp_path, state, client)["no_result"] == 1
    assert client.names == ["Tea 2"]


def test_publish_rejects_different_existing_content(tmp_path):
    final = tmp_path / "products/x.json"
    publish(tmp_path / "_staging/x.json", final, b"original")
    publish(tmp_path / "_staging/x.json", final, b"original")
    with pytest.raises(RuntimeError, match="nội dung khác"):
        publish(tmp_path / "_staging/x.json", final, b"replacement")
    assert final.read_bytes() == b"original"


def test_unsupported_rename_does_not_fallback_to_partial_copy(tmp_path, monkeypatch):
    def fail_rename(*args):
        raise OSError("rename không hỗ trợ")

    monkeypatch.setattr(Path, "rename", fail_rename)
    with pytest.raises(OSError):
        publish(tmp_path / "_staging/x.json", tmp_path / "products/x.json", b"body")
    assert not (tmp_path / "products/x.json").exists()


def image_batch(root, *, code="00123", url="https://images.openfoodfacts.org/images/x.jpg"):
    state = State(products(1))
    run_batch(root, state, Search([[{"code": code, "image_url": url}]]))
    return [state.rows[1]["batch_file"]]


def test_images_download_once_keep_zero_prefix_and_never_store_bytes_in_json(tmp_path):
    files = image_batch(tmp_path)
    payload = b"\xff\xd8\xff\x01\xff\xd9"
    client, session, _ = http_client([Response(content=payload, headers={"Content-Type": "image/jpeg"})])
    assert download_images(tmp_path, files, client, limiter=client.limiter) == {
        "downloaded": 1,
        "existing": 0,
        "skipped": 0,
        "failed": 0,
    }
    assert download_images(tmp_path, files, client, limiter=client.limiter) == {
        "downloaded": 0,
        "existing": 1,
        "skipped": 0,
        "failed": 0,
    }
    assert len(session.calls) == 1
    assert (tmp_path / "images/00123.jpg").read_bytes() == payload
    assert json.loads(Path(files[0]).read_text())["code"] == "00123"


@pytest.mark.parametrize(
    "url", ["http://images.openfoodfacts.org/x", "https://other.example/x", "https://user@images.openfoodfacts.org/x"]
)
def test_images_reject_unknown_hosts_before_any_request(tmp_path, url):
    client, session, _ = http_client([])
    assert download_images(tmp_path, image_batch(tmp_path, url=url), client)["failed"] == 1
    assert session.calls == []


def test_images_reject_corrupt_jpeg_and_close_response(tmp_path):
    response = Response(content=b"not jpeg", headers={"Content-Type": "image/jpeg"})
    client, _, _ = http_client([response])
    assert download_images(tmp_path, image_batch(tmp_path), client)["failed"] == 1
    assert response.closed and not (tmp_path / "images/00123.jpg").exists()


def test_images_reject_modified_jsonl_without_network(tmp_path):
    files = image_batch(tmp_path)
    Path(files[0]).write_bytes(canonical_json({"code": "999"}))
    client, session, _ = http_client([])
    with pytest.raises(ValueError, match="Checksum"):
        download_images(tmp_path, files, client)
    assert not session.calls


@pytest.mark.parametrize("content_type", ["text/html", "image/png"])
def test_images_reject_non_jpeg_response_before_publication(tmp_path, content_type):
    response = Response(content=b"not jpeg", headers={"Content-Type": content_type})
    client, _, _ = http_client([response])
    assert download_images(tmp_path, image_batch(tmp_path), client)["failed"] == 1
    assert response.closed and not list((tmp_path / "images").glob("*.jpg"))


def test_images_cap_streamed_body_and_close_response(tmp_path, monkeypatch):
    monkeypatch.setattr("ingest.off_images.MAX_IMAGE_BYTES", 3)
    response = Response(content=b"\xff\xd8\xff\x01\xff\xd9", headers={"Content-Type": "image/jpeg"})
    client, _, _ = http_client([response])
    assert download_images(tmp_path, image_batch(tmp_path), client)["failed"] == 1
    assert response.closed and not list((tmp_path / "images").glob("*.jpg"))


def test_invalid_hit_does_not_discard_valid_hits(tmp_path):
    state = State(products(1))
    hits = [dict(code="not-valid"), dict(code="00123"), dict(code="00124")]
    summary = run_batch(tmp_path, state, Search([hits]))
    assert summary["invalid_hits"] == 1
    assert summary["done"] == 1
    records = Path(state.rows[1]["batch_file"]).read_text().splitlines()
    assert [json.loads(line)["code"] for line in records] == ["00123", "00124"]


def test_permanent_image_failure_is_cached_without_blocking_next_image(tmp_path):
    state = State(products(1))
    hits = [
        {"code": code, "image_url": f"https://images.openfoodfacts.org/images/{code}.jpg"} for code in ("123", "456")
    ]
    run_batch(tmp_path, state, Search([hits]))
    files = [state.rows[1]["batch_file"]]
    response = Response(content=b"\xff\xd8\xff\x01\xff\xd9", headers={"Content-Type": "image/jpeg"})
    client, session, _ = http_client([Response(404), response])
    assert download_images(tmp_path, files, client, limiter=client.limiter) == {
        "downloaded": 1,
        "existing": 0,
        "skipped": 1,
        "failed": 0,
    }
    assert download_images(tmp_path, files, client, limiter=client.limiter) == {
        "downloaded": 0,
        "existing": 1,
        "skipped": 1,
        "failed": 0,
    }
    assert len(session.calls) == 2


def test_python_task_entrypoint_accepts_exec_without_dunder_file(monkeypatch):
    source = Path(__file__).parents[1] / "src/ingest/off_crawler.py"
    monkeypatch.setattr(sys, "path", sys.path[:])
    monkeypatch.setattr(sys, "argv", [str(source), "--help"])
    with pytest.raises(SystemExit) as result:
        exec(compile(source.read_text(), str(source), "exec"), {"__name__": "__main__"})
    assert result.value.code == 0
    assert sys.path[0] == str(source.resolve().parents[1])


def test_python_task_entrypoint_skips_http_404_and_downloads_next_image(tmp_path, monkeypatch, capsys):
    state = State(products(1))
    hits = [
        {"code": code, "image_url": f"https://images.openfoodfacts.org/images/{code}.jpg"} for code in ("123", "456")
    ]
    run_batch(tmp_path, state, Search([hits]))
    files = [state.rows[1]["batch_file"]]
    jpeg = Response(content=b"\xff\xd8\xff\x01\xff\xd9", headers={"Content-Type": "image/jpeg"})
    session = Session([Response(404), jpeg], Clock())
    spark = SimpleNamespace(conf=SimpleNamespace(set=lambda *_: None))
    monkeypatch.setattr("pyspark.sql.SparkSession.Builder.getOrCreate", lambda _: spark)
    monkeypatch.setattr(
        "ingest.off_state.SparkState", lambda *_args, **_kwargs: SimpleNamespace(batch_files=lambda: files)
    )
    monkeypatch.setattr("requests.Session", lambda: nullcontext(session))
    monkeypatch.setattr(
        "ingest.off_images.download_images",
        lambda _root, batch_files, client: download_images(tmp_path, batch_files, client, limiter=RateLimiter(0)),
    )
    source = Path(__file__).parents[1] / "src/ingest/off_crawler.py"
    monkeypatch.setattr(sys, "path", sys.path[:])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(source),
            "--mode",
            "images",
            "--bronze-schema",
            "bronze",
            "--state-schema",
            "ops",
            "--off-root",
            "/Volumes/instacart/dev/raw/off_api",
        ],
    )

    exec(compile(source.read_text(), str(source), "exec"), {"__name__": "__main__"})

    assert json.loads(capsys.readouterr().out) == {
        "downloaded": 1,
        "existing": 0,
        "skipped": 1,
        "failed": 0,
        "image_requests": 2,
    }
    assert (tmp_path / "images/456.jpg").is_file()
