"""Đọc endpoint Kafka và phạm vi hub từ chuỗi kết nối SAS."""

import re


def sas_endpoint(connection: str) -> tuple[str, str | None]:
    """Trả về địa chỉ Kafka và EntityPath nếu SAS được cấp cho một hub."""
    invalid_connection = "Cần chuỗi kết nối SAS hợp lệ cho Event Hubs"
    try:
        fields = dict(part.split("=", 1) for part in connection.split(";") if part)
    except ValueError:
        raise ValueError(invalid_connection) from None

    endpoint = re.fullmatch(r"sb://([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+)/?", fields.get("Endpoint", ""))
    if not endpoint or not fields.get("SharedAccessKeyName") or not fields.get("SharedAccessKey"):
        raise ValueError(invalid_connection)

    return f"{endpoint[1]}:9093", fields.get("EntityPath")
