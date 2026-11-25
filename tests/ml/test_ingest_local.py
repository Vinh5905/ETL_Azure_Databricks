from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[2] / "src/ml/sql"


def _statements(path: Path, database: str) -> list[str]:
    sql = path.read_text(encoding="utf-8").replace("instacart.ml.", f"{database}.")
    out = []
    for statement in re.split(r";\s*(?=(?:[^']*'[^']*')*[^']*$)", sql):
        statement = "\n".join(line for line in statement.splitlines() if not line.lstrip().startswith("--")).strip()
        if statement:
            out.append(statement)
    return out


def _e6_row(run_key: str, wall_s: float, measured_at: str) -> str:
    # Đúng dạng dòng bench.py ghi ra (ds/03 §6): machine là object, không có finished_at,
    # thiếu các cột chỉ có ở Databricks.
    return json.dumps(
        {
            "experiment": "E6",
            "platform": "local_mapreduce",
            "algo": "pair_count_mr",
            "grain": "product",
            "scale_pct": 10,
            "top_n": None,
            "min_support": 0.001,
            "min_confidence": None,
            "max_k": 2,
            "cores": 8,
            "rep": 1,
            "data_version": "v1",
            "run_key": run_key,
            "status": "ok",
            "wall_s": wall_s,
            "machine": {"name": "p4-laptop", "docker_cpus": 8, "docker_mem_gb": 16},
            "measured_at": measured_at,
        }
    )


def test_ingest_local_loads_e6_rows_twice_without_duplicates(spark):
    database = f"ml_ingest_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {database}")
    try:
        ddl = (SQL_DIR / "ddl_ml.sql").read_text(encoding="utf-8")
        (runs_ddl,) = [s for s in ddl.split(";") if "experiment_runs (" in s]
        spark.sql(runs_ddl.replace("instacart.ml.", f"{database}."))

        create, copy_into, merge = _statements(SQL_DIR / "ingest_local.sql", database)
        assert copy_into.startswith("COPY INTO") and "FILEFORMAT = TEXT" in copy_into
        spark.sql(create)

        first_file = [
            _e6_row("a", 10.0, "2026-11-20T01:00:00.123456+00:00"),
            _e6_row("b", 20.0, "2026-11-20T02:00:00+00:00"),
        ]
        # File upload sau chứa lại dòng a (đo lại, mới hơn) và một dòng trống.
        second_file = [_e6_row("a", 11.0, "2026-11-21T01:00:00+00:00"), ""]
        for lines in (first_file, second_file):
            # COPY INTO không có trong Spark OSS: ghi thẳng từng dòng vào bảng thô như COPY INTO … FILEFORMAT = TEXT.
            spark.createDataFrame([(line,) for line in lines], "value string").write.format("delta").mode(
                "append"
            ).saveAsTable(f"{database}.experiment_runs_local_raw")
            spark.sql(merge)
        spark.sql(merge)  # chạy lại job khi không có file mới

        rows = {r.run_key: r for r in spark.table(f"{database}.experiment_runs").collect()}
        assert sorted(rows) == ["a", "b"]
        assert rows["a"].wall_s == 11.0
        assert str(rows["a"].finished_at).startswith("2026-11-21")
        assert rows["b"].scale_pct == 10 and rows["b"].top_n is None and rows["b"].experiment == "E6"
        assert json.loads(rows["b"].machine)["name"] == "p4-laptop"
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
