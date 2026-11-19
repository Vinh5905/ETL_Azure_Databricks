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
                for algorithm in _values(config["algo"]):
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
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    selected = args.experiments_single
    if args.experiments_csv is not None:
        selected = [item.strip() for item in args.experiments_csv.split(",") if item.strip()]
    rows = plan_grid(args.grid, selected)
    result = "\n".join(json.dumps(row, sort_keys=True) for row in rows)
    if args.output:
        args.output.write_text(result + ("\n" if result else ""), encoding="utf-8")
    else:
        print(result)


if __name__ == "__main__":
    main()
