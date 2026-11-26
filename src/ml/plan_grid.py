"""Bung lưới thực nghiệm, loại run đã xong và tạo hàng đợi Databricks."""

import argparse
import hashlib
import itertools
import json
from pathlib import Path
from typing import Any

import yaml

DEFAULT_GRID = Path(__file__).with_name("experiments.yml")
RUN_KEY_FIELDS = (
    "experiment",
    "platform",
    "algo",
    "grain",
    "scale_pct",
    "top_n",
    "min_support",
    "min_confidence",
    "max_k",
    "cores",
    "rep",
    "data_version",
    "algo_version",
)
DEFAULT_MIN_CONFIDENCE = {"product": 0.1, "aisle": 0.3, "department": 0.5}


def _values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def make_run_key(config: dict[str, Any]) -> str:
    """Hash đúng tham số ảnh hưởng kết quả; timeout và cờ persist không đổi danh tính run."""
    payload = "|".join(json.dumps(config.get(key), sort_keys=True, separators=(",", ":")) for key in RUN_KEY_FIELDS)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def plan_grid(
    path: str | Path = DEFAULT_GRID,
    experiments: list[str] | None = None,
    *,
    data_version: str | None = None,
) -> list[dict[str, Any]]:
    """Mở rộng defaults và lưới; E7 trả đủ 9 ngưỡng để kiểm thử/đánh giá."""
    content = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    defaults = dict(content.get("defaults", {}))
    if data_version is not None:
        defaults["data_version"] = data_version
    definitions = content.get("experiments", {})
    chosen = list(definitions) if experiments is None else experiments
    unknown = set(chosen) - definitions.keys()
    if unknown:
        raise ValueError(f"Mã experiment không tồn tại: {', '.join(sorted(unknown))}")
    planned = []
    for name in chosen:
        definition = definitions[name]
        fixed = {**defaults, **{key: value for key, value in definition.items() if key != "runs"}}
        for run in definition.get("runs", []):
            merged = {**fixed, **run}
            axes = {key: _values(value) for key, value in merged.items() if isinstance(value, list)}
            scalar = {key: value for key, value in merged.items() if not isinstance(value, list)}
            keys = list(axes)
            for values in itertools.product(*(axes[key] for key in keys)):
                config = {**scalar, **dict(zip(keys, values, strict=True)), "experiment": name}
                for algorithm in _values(config["algo"]):
                    item = {**config, "algo": algorithm}
                    if item.get("min_confidence") is None:
                        item["min_confidence"] = DEFAULT_MIN_CONFIDENCE[item["grain"]]
                    for rep in range(1, int(item["reps"]) + 1):
                        replicated = {**item, "rep": rep}
                        replicated["run_key"] = make_run_key(replicated)
                        planned.append(replicated)
    return planned


def group_e7_by_support(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Gộp ba ngưỡng confidence thành một lần FP-Growth cho mỗi MinSupport."""
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    result = []
    for row in rows:
        if row["experiment"] != "E7":
            result.append(dict(row))
            continue
        key = tuple((field, row[field]) for field in RUN_KEY_FIELDS if field != "min_confidence")
        if key not in grouped:
            grouped[key] = {**row, "_confidence_sweep": []}
        grouped[key]["_confidence_sweep"].append(float(row["min_confidence"]))
    for row in grouped.values():
        sweep = sorted(set(row.pop("_confidence_sweep")))
        row["min_confidence"] = sweep[0]
        row["sweep"] = json.dumps({"min_confidence": sweep}, separators=(",", ":"))
        row["run_key"] = make_run_key(row)
        result.append(row)
    return result


def pending_runs(rows: list[dict[str, Any]], completed_run_keys: set[str]) -> list[dict[str, Any]]:
    """Bỏ các run đã có status ok; timeout/failed vẫn được xếp lại."""
    grouped = group_e7_by_support(rows)
    return [row for row in grouped if row["run_key"] not in completed_run_keys]


def task_values_by_cores(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Chia run_key rút gọn theo số core của task value."""
    return {
        f"queue_c{cores}": [row["run_key"][:12] for row in rows if int(row.get("cores", 4)) == cores]
        for cores in (1, 2, 4)
    }


def _enqueue(spark, rows: list[dict[str, Any]]) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    fields = [
        ("run_key", StringType()),
        ("algo_version", StringType()),
        ("experiment", StringType()),
        ("platform", StringType()),
        ("algo", StringType()),
        ("grain", StringType()),
        ("scale_pct", IntegerType()),
        ("top_n", IntegerType()),
        ("min_support", DoubleType()),
        ("min_confidence", DoubleType()),
        ("max_k", IntegerType()),
        ("cores", IntegerType()),
        ("rep", IntegerType()),
        ("data_version", StringType()),
        ("timeout_min", IntegerType()),
        ("persist", BooleanType()),
        ("sweep", StringType()),
        ("n_tx", LongType()),
    ]
    if rows:
        schema = StructType([StructField(name, dtype, True) for name, dtype in fields])
        records = [tuple(row.get(name) for name, _ in fields) for row in rows]
        frame = spark.createDataFrame(records, schema)
        queue_name = "instacart.ml.experiment_queue"
        (
            DeltaTable.forName(spark, queue_name)
            .alias("target")
            .merge(frame.alias("source"), "target.run_key = source.run_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError as exc:
        raise RuntimeError("Chạy không có --output cần Databricks Runtime để đặt task values") from exc
    for key, values in task_values_by_cores(rows).items():
        dbutils.jobs.taskValues.set(key=key, value=values)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid", type=Path, default=DEFAULT_GRID)
    parser.add_argument("--experiment", action="append", dest="experiments_single")
    parser.add_argument("--experiments", dest="experiments_csv", help="Danh sách mã experiment phân tách bằng dấu phẩy")
    parser.add_argument("--data-version", default="v1")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    selected = args.experiments_single
    if args.experiments_csv is not None:
        selected = [item.strip() for item in args.experiments_csv.split(",") if item.strip()]
    rows = plan_grid(args.grid, selected, data_version=args.data_version)
    if args.output:
        result = "\n".join(json.dumps(row, sort_keys=True) for row in rows)
        args.output.write_text(result + ("\n" if result else ""), encoding="utf-8")
        return

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    completed = {
        row.run_key
        for row in spark.table("instacart.ml.experiment_runs")
        .where(F.col("status") == "ok")
        .select("run_key")
        .collect()
    }
    pending = pending_runs(rows, completed)
    _enqueue(spark, pending)
    print(f"Đã xếp {len(pending)} cấu hình mới vào instacart.ml.experiment_queue")


if __name__ == "__main__":
    main()
