"""Chạy SQL của ds-prepare trong một SparkSession, truyền tham số theo tên."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Spark Python Task có thể thực thi file bằng exec(compile(...)) mà không đặt __file__.
SQL_DIR = Path(globals().get("__file__", sys._getframe().f_code.co_filename)).resolve().parent
PREPARE_FILES = ("01_snapshot.sql", "02_index_transactions.sql", "03_holdout.sql")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
REPLACE_VERSION = re.compile(
    r"\A(INSERT\s+INTO\s+IDENTIFIER\(\s*:(?:item_index|transactions)\s*\)\s+"
    r"REPLACE\s+WHERE\s+data_version\s*=\s*):v\b",
    re.IGNORECASE,
)

SQL_TOKEN = re.compile(
    r"--|/\*|['\"`]|\bIDENTIFIER\(\s*:(?P<identifier>[a-z_]+)\s*\)|:(?P<literal>[a-z_]+)\b",
    re.IGNORECASE,
)
NAME_PART = r"(?:`(?:[^`]|``)+`|[A-Za-z_]\w*)"
QUALIFIED_NAME = re.compile(rf"{NAME_PART}(?:\.{NAME_PART}){{0,2}}")


def _block_end(sql: str, start: int) -> int:
    depth, pos = 1, start + 2
    while pos < len(sql):
        if sql.startswith("/*", pos):
            depth += 1
            pos += 2
        elif sql.startswith("*/", pos):
            depth -= 1
            pos += 2
            if depth == 0:
                return pos
        else:
            pos += 1
    raise ValueError("Comment SQL chưa đóng")


def _quoted_end(sql: str, start: int) -> int:
    quote = sql[start]
    raw_prefix = (
        start > 0
        and sql[start - 1].lower() == "r"
        and (start < 2 or not (sql[start - 2].isalnum() or sql[start - 2] == "_"))
    )
    escapes = quote != "`" and not raw_prefix
    pos = start + 1
    while pos < len(sql):
        if escapes and sql[pos] == "\\":
            pos += 2
        elif sql[pos] == quote:
            if pos + 1 < len(sql) and sql[pos + 1] == quote:
                pos += 2
            else:
                return pos + 1
        else:
            pos += 1
    raise ValueError("Chuỗi hoặc tên SQL chưa đóng")


def split_sql(sql: str) -> list[str]:
    """Tách câu theo dấu chấm phẩy ngoài literal, tên và comment Spark SQL."""
    statements = []
    start = pos = 0
    has_sql = False
    while pos < len(sql):
        char = sql[pos]
        if sql.startswith("--", pos):
            end = sql.find("\n", pos + 2)
            pos = len(sql) if end < 0 else end + 1
        elif sql.startswith("/*", pos):
            pos = _block_end(sql, pos)
        elif char in "'\"`":
            pos = _quoted_end(sql, pos)
            has_sql = True
        elif char == ";":
            if has_sql:
                statements.append(sql[start:pos].strip())
            pos += 1
            start, has_sql = pos, False
        else:
            has_sql |= not char.isspace()
            pos += 1
    if has_sql:
        statements.append(sql[start:].strip())
    return statements


def _quoted(name: str) -> str:
    if not name.strip() or any(ord(char) < 32 or ord(char) == 127 for char in name):
        raise ValueError("Catalog hoặc schema phải khác rỗng và không chứa ký tự điều khiển")
    return "`" + name.replace("`", "``") + "`"


def sql_args(
    data_version: str, *, catalog: str = "instacart", silver_schema: str = "silver", ds_schema: str = "ds_data"
) -> dict[str, str]:
    if not VERSION.fullmatch(data_version):
        raise ValueError("data_version cần 1–64 ký tự chữ, số, gạch ngang hoặc gạch dưới")
    if silver_schema.casefold() == ds_schema.casefold():
        raise ValueError("Schema snapshot phải tách khỏi schema nguồn")
    source = f"{_quoted(catalog)}.{_quoted(silver_schema)}"
    target = f"{_quoted(catalog)}.{_quoted(ds_schema)}"
    return {
        "v": data_version,
        "source_baskets": f"{source}.`baskets`",
        "source_products": f"{source}.`dim_product`",
        "source_order_items": f"{source}.`order_items`",
        "data_versions": f"{target}.`data_versions`",
        "baskets": f"{target}.`baskets`",
        "frozen_products": f"{target}.`ds_prepare_products`",
        "frozen_holdout": f"{target}.`ds_prepare_holdout`",
        "item_index": f"{target}.`item_index`",
        "transactions": f"{target}.`transactions`",
        "holdout_eval": f"{target}.`holdout_eval`",
    }


def _load_sql(files: Sequence[str]) -> list[tuple[str, list[str]]]:
    loaded = []
    for name in files:
        if not re.fullmatch(r"[a-z0-9_]+\.sql", name):
            raise ValueError(f"Cần tên file SQL trong thư mục ds_prepare: {name}")
        statements = split_sql((SQL_DIR / name).read_text(encoding="utf-8"))
        if not statements:
            raise ValueError(f"File không có câu SQL: {name}")
        loaded.append((name, statements))
    return loaded


def _render_sql(statement: str, args: dict[str, str]) -> str:
    # DBR 15.4 có thể bỏ sót marker tên bảng trong INSERT có CTE.
    parts = []
    pos = 0
    while token := SQL_TOKEN.search(statement, pos):
        start, end = token.span()
        parts.append(statement[pos:start])
        if token.group() == "--":
            newline = statement.find("\n", end)
            end = len(statement) if newline < 0 else newline
        elif token.group() == "/*":
            end = _block_end(statement, start)
        elif token.group() in "'\"`":
            end = _quoted_end(statement, start)
        elif name := token.group("identifier"):
            identifier = args[name]
            if not QUALIFIED_NAME.fullmatch(identifier):
                raise ValueError(f"Tên bảng SQL không hợp lệ: {name}")
            parts.append(identifier)
            pos = end
            continue
        elif token.group("literal") == "v":
            parts.append(f"'{args['v']}'")
            pos = end
            continue
        elif token.group("literal") == "source_baskets":
            encoded = args["source_baskets"].encode("utf-8").hex()
            parts.append(f"CAST(unhex('{encoded}') AS STRING)")
            pos = end
            continue
        else:
            raise ValueError(f"Marker SQL không được hỗ trợ: {token.group()}")
        parts.append(statement[start:end])
        pos = end
    return "".join(parts) + statement[pos:]


def run_files(spark: SparkSession, args: dict[str, str], files: Sequence[str] = PREPARE_FILES) -> None:
    version = args["v"]
    if not VERSION.fullmatch(version):
        raise ValueError("data_version không hợp lệ")
    # Đọc và tách toàn bộ file trước lần ghi đầu tiên. Chỉ collect kết quả kiểm tra hoặc DML.
    for name, statements in _load_sql(files):
        for index, statement in enumerate(statements, start=1):
            try:
                if re.search(r"\bREPLACE\s+WHERE\b", statement, flags=re.IGNORECASE) and not REPLACE_VERSION.match(
                    statement
                ):
                    raise ValueError(
                        "REPLACE WHERE cần dạng INSERT INTO IDENTIFIER(:bảng) REPLACE WHERE data_version = :v"
                    )
                spark.sql(_render_sql(statement, args)).collect()
            except Exception as exc:
                raise RuntimeError(f"{name}, câu {index}, phiên bản {version}: {exc}") from exc


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-version", required=True)
    parser.add_argument("--catalog", default="instacart")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--ds-schema", default="ds_data")
    parser.add_argument("files", nargs="*", default=PREPARE_FILES)
    options = parser.parse_args()
    try:
        args = sql_args(
            options.data_version,
            catalog=options.catalog,
            silver_schema=options.silver_schema,
            ds_schema=options.ds_schema,
        )
    except ValueError as exc:
        parser.error(str(exc))
    run_files(SparkSession.builder.getOrCreate(), args, options.files)


if __name__ == "__main__":
    main()
