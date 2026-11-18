"""Bung lưới thực nghiệm thành các cấu hình có run_key ổn định."""

import argparse
import hashlib
import itertools
import json
from pathlib import Path
from typing import Any

import yaml

DEFAULT_GRID = Path(__file__).with_name("experiments.yml")


def _values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def make_run_key(config: dict[str, Any]) -> str:
    """Tính SHA-1 trên cấu hình thực nghiệm chuẩn hóa để retry không sinh dòng trùng."""
    stable = {key: config[key] for key in sorted(config) if key != "run_key"}
    payload = json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def plan_grid(path: str | Path = DEFAULT_GRID, experiments: list[str] | None = None) -> list[dict[str, Any]]:
    """Mở rộng defaults, experiment, cores và mọi mảng tham số thành từng lần chạy."""
    content = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    defaults = content.get("defaults", {})
    definitions = content.get("experiments", {})
    chosen = experiments or list(definitions)
    unknown = set(chosen) - definitions.keys()
    if unknown:
        raise ValueError(f"Mã experiment không tồn tại: {', '.join(sorted(unknown))}")
    planned = []
    for name in chosen:
        definition = definitions[name]
        fixed = {**defaults, **{k: v for k, v in definition.items() if k != "runs"}}
        for run in definition.get("runs", []):
            merged = {**fixed, **run}
            axes = {key: _values(value) for key, value in merged.items() if isinstance(value, list)}
            scalar = {key: value for key, value in merged.items() if not isinstance(value, list)}
            keys = list(axes)
            for values in itertools.product(*(axes[key] for key in keys)):
                config = {**scalar, **dict(zip(keys, values, strict=True)), "experiment": name}
                algo = config["algo"]
                for algorithm in _values(algo):
                    item = {**config, "algo": algorithm}
                    if item.get("min_confidence") is None:
                        item["min_confidence"] = 0.1
                    for rep in range(1, int(item["reps"]) + 1):
                        replicated = {**item, "rep": rep}
                        replicated["run_key"] = make_run_key(replicated)
                        planned.append(replicated)
    return planned


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid", type=Path, default=DEFAULT_GRID)
    parser.add_argument("--experiment", action="append", dest="experiments_single")
    parser.add_argument("--experiments", dest="experiments_csv", help="Danh sách mã experiment phân tách bằng dấu phẩy")
    parser.add_argument("--data-version", default="v1")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    # argparse dùng hai cờ riêng: --experiment lặp lại cho CLI local, --experiments
    # dùng tham số job trên Databricks.
    selected = args.experiments_single
    if args.experiments_csv is not None:
        selected = [item.strip() for item in args.experiments_csv.split(",") if item.strip()]
    rows = plan_grid(args.grid, selected)
    for row in rows:
        row["data_version"] = args.data_version
        row["platform"] = "databricks"
        row["run_key"] = make_run_key(row)
    result = "\n".join(json.dumps(row, sort_keys=True) for row in rows)
    if args.output:
        args.output.write_text(result + ("\n" if result else ""), encoding="utf-8")
    else:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        spark = SparkSession.builder.getOrCreate()
        queue_name = "instacart.ml.experiment_queue"
        runs_name = "instacart.ml.experiment_runs"
        completed = {
            row.run_key for row in spark.table(runs_name).where(F.col("status") == "ok").select("run_key").collect()
        }
        pending = [row for row in rows if row["run_key"] not in completed]
        if pending:
            fields = [
                ("run_key", StringType()),
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
            schema = StructType([StructField(name, data_type, True) for name, data_type in fields])
            queue_rows = [tuple(row.get(name) for name, _ in fields) for row in pending]
            frame = spark.createDataFrame(queue_rows, schema)
            if spark.catalog.tableExists(queue_name):
                from delta.tables import DeltaTable

                (
                    DeltaTable.forName(spark, queue_name)
                    .alias("target")
                    .merge(frame.alias("source"), "target.run_key = source.run_key")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                frame.write.format("delta").mode("overwrite").saveAsTable(queue_name)

        try:
            from databricks.sdk.runtime import dbutils
        except ImportError as exc:
            raise RuntimeError("Chạy không có --output cần Databricks Runtime để đặt task values") from exc
        for cores in (1, 2, 4):
            keys = [row["run_key"][:12] for row in pending if int(row.get("cores", 4)) == cores]
            dbutils.jobs.taskValues.set(key=f"queue_c{cores}", value=keys)
        print(f"Đã xếp {len(pending)} cấu hình mới vào {queue_name}")


if __name__ == "__main__":
    main()
