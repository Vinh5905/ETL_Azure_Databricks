"""Chạy lưới E6 ba cách cài và ghi kết quả JSONL có thể tiếp tục sau gián đoạn."""

import hashlib
import json
import math
import os
import platform
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

JAR = "/work/jars/instacart-mr-1.0.jar"
MIN_SUPPORT = 0.001
DATA_VERSION = "v1"
RESULTS = Path("/work/results/e6.jsonl")
PLATFORMS = {"mr": "local_mapreduce", "spark_df": "local_spark_yarn", "spark_rdd": "local_spark_yarn"}


def run_key(implementation: str, scale: int, cores: int, rep: int, sweep: str) -> dict:
    row = {
        "experiment": "E6",
        "platform": PLATFORMS[implementation],
        "algo": f"pair_count_{implementation}",
        "grain": "product",
        "scale_pct": scale,
        "top_n": None,
        "min_support": MIN_SUPPORT,
        "min_confidence": None,
        "max_k": 2,
        "cores": cores,
        "rep": rep,
        "data_version": DATA_VERSION,
        "sweep": sweep,
    }
    serialized = json.dumps(row, sort_keys=True, separators=(",", ":"))
    row["run_key"] = hashlib.sha1(serialized.encode("utf-8")).hexdigest()
    return row


def timed(command: list[str]) -> float:
    started = time.perf_counter()
    subprocess.run(command, check=True, capture_output=True, text=True)
    return time.perf_counter() - started


def execute(implementation: str, scale: int, cores: int, out: str, n_tx: int) -> float:
    min_count = str(math.ceil(MIN_SUPPORT * n_tx))
    input_path = f"hdfs:///ds/v1/product/scale={scale}"
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", out], check=False, capture_output=True)
    limits = [
        f"-Dmapreduce.job.running.map.limit={cores}",
        f"-Dmapreduce.job.running.reduce.limit={cores}",
        f"-Dmapreduce.job.reduces={cores}",
    ]
    if implementation == "mr":
        first = timed(["hadoop", "jar", JAR, "instacart.mr.ItemCount", *limits, input_path, f"{out}/items", min_count])
        second = timed(
            [
                "hadoop",
                "jar",
                JAR,
                "instacart.mr.PairCount",
                *limits,
                input_path,
                f"{out}/pairs",
                min_count,
                f"hdfs://namenode:8020{out}/items/part-r-00000",
            ]
        )
        return first + second
    script = {"spark_df": "pair_count_df.py", "spark_rdd": "pair_count_rdd.py"}[implementation]
    return timed(
        [
            "spark-submit",
            "--num-executors",
            "1",
            "--executor-cores",
            str(cores),
            "--executor-memory",
            f"{1200 * cores}m",
            f"/work/spark/{script}",
            input_path,
            f"{out}/pairs",
            min_count,
        ]
    )


def count_results(implementation: str, scale: int, cores: int, rep: int) -> int:
    path = f"/out/{implementation}/s{scale}_c{cores}_r{rep}/pairs"
    output = subprocess.run(["hdfs", "dfs", "-cat", f"{path}/part-*"], check=True, capture_output=True, text=True)
    return sum(bool(line.strip()) for line in output.stdout.splitlines())


def main() -> None:
    with open("/data/ds/v1/_manifest.json", encoding="utf-8") as manifest_file:
        counts = json.load(manifest_file)["product"]
    machine = {
        "name": os.environ.get("BENCH_MACHINE", platform.node()),
        "docker_cpus": int(os.environ.get("DOCKER_CPUS", "8")),
        "docker_mem_gb": int(os.environ.get("DOCKER_MEM_GB", "14")),
    }
    max_cores = machine["docker_cpus"]
    scale_plan = [(scale, min(8, max_cores)) for scale in (10, 25, 50, 100, 200)]
    core_plan = [(100, cores) for cores in (1, 2, 4, 8) if cores <= max_cores]
    plan = [("scale", scale, cores) for scale, cores in scale_plan]
    plan.extend(("cores", scale, cores) for scale, cores in core_plan)
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    try:
        completed = {json.loads(line)["run_key"] for line in RESULTS.read_text(encoding="utf-8").splitlines() if line}
    except FileNotFoundError:
        completed = set()
    for rep in (1, 2, 3):
        for sweep, scale, cores in plan:
            n_tx = int(counts["100"] * 2 if scale == 200 else counts[str(scale)])
            for implementation in ("mr", "spark_df", "spark_rdd"):
                row = run_key(implementation, scale, cores, rep, sweep)
                if row["run_key"] in completed:
                    continue
                output = f"/out/{implementation}/s{scale}_c{cores}_r{rep}"
                wall = execute(implementation, scale, cores, output, n_tx)
                row.update(
                    status="ok",
                    n_tx=n_tx,
                    n_result=count_results(implementation, scale, cores, rep),
                    wall_s=round(wall, 2),
                    machine=machine,
                    measured_at=datetime.now(UTC).isoformat(),
                    job_s=None,
                    peak_mem_mb=None,
                    spill_mb=None,
                )
                with RESULTS.open("a", encoding="utf-8") as results_file:
                    results_file.write(json.dumps(row, sort_keys=True) + "\n")
                completed.add(row["run_key"])


if __name__ == "__main__":
    main()
