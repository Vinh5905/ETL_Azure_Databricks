"""Gửi sự kiện đơn hàng vào Event Hubs qua Kafka."""

import logging
import os
import re
from collections.abc import Iterable
from time import sleep

TOPIC = "orders-stream"
FLUSH_TIMEOUT_SECONDS = 125


class KafkaSendError(Exception):
    """Lỗi cấu hình hoặc gửi Kafka có thể hiển thị mà không lộ thông tin xác thực."""


def _producer_config(compression: str) -> dict[str, str | int | bool]:
    connection = os.environ.get("EH_SEND_CONN", "").strip()
    if not connection:
        raise KafkaSendError("Cần đặt EH_SEND_CONN bằng chuỗi kết nối SAS có quyền Send")

    invalid_connection = "EH_SEND_CONN phải là chuỗi kết nối SAS hợp lệ cho Event Hubs"
    try:
        fields = dict(part.split("=", 1) for part in connection.split(";") if part)
    except ValueError:
        raise KafkaSendError(invalid_connection) from None
    endpoint = re.fullmatch(r"sb://([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+)/?", fields.get("Endpoint", ""))
    if not endpoint or not fields.get("SharedAccessKeyName") or not fields.get("SharedAccessKey"):
        raise KafkaSendError(invalid_connection)
    if fields.get("EntityPath", TOPIC) != TOPIC:
        raise KafkaSendError(f"EH_SEND_CONN phải cấp quyền cho Event Hub {TOPIC}")

    return {
        "bootstrap.servers": f"{endpoint[1]}:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": connection,
        "enable.idempotence": True,
        "compression.type": compression,
        "request.timeout.ms": 60_000,
        "message.timeout.ms": 120_000,
        "socket.keepalive.enable": True,
        "metadata.max.age.ms": 180_000,
    }


def publish_events(messages: Iterable[tuple[str, str]], *, rate: int, compression: str) -> int:
    """Gửi cặp khóa và JSON, trả về số sự kiện đã được broker xác nhận."""
    config = _producer_config(compression)
    try:
        from confluent_kafka import KafkaException, Producer
    except ImportError:
        raise KafkaSendError("Cần cài Kafka client bằng uv sync --frozen --extra ingest") from None

    acknowledged = 0
    failed = 0
    pending = 0
    failure_codes: set[int] = set()

    def on_delivery(error, _message):
        nonlocal acknowledged, failed
        if error is None:
            acknowledged += 1
        else:
            failed += 1
            failure_codes.add(error.code())

    # Chỉ hiển thị lỗi đã lược thông tin xác thực, kể cả khi native client gặp lỗi.
    logger = logging.Logger(__name__, level=logging.CRITICAL + 1)
    try:
        producer = Producer(config, logger=logger)
        for key, value in messages:
            try:
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            except BufferError:
                pending = producer.flush(FLUSH_TIMEOUT_SECONDS)
                if pending or failed:
                    break
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            producer.poll(0)
            if failed:
                break
            sleep(1 / rate)
        if not pending:
            pending = producer.flush(FLUSH_TIMEOUT_SECONDS)
    except (BufferError, KafkaException, ValueError):
        raise KafkaSendError(f"Kafka producer gặp lỗi, broker đã xác nhận {acknowledged} sự kiện") from None

    if failed or pending:
        codes = ", ".join(map(str, sorted(failure_codes)))
        detail = f" (mã Kafka: {codes})" if codes else ""
        raise KafkaSendError(
            f"Broker xác nhận {acknowledged} sự kiện, {failed} lỗi{detail}, {pending} chưa được xác nhận"
        )
    return acknowledged
