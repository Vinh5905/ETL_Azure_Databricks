# 04 · Thực nghiệm và đánh giá

**Đầu vào:**
- Thuật toán ở [02](02_thuat_toan_spark.md) đã qua test đúng đắn.
- Lưới MinSupport chốt ở [01 §4](01_du_lieu_thuc_nghiem.md).
- Hạ tầng cho DS: policy `instacart-ds-benchmark`, schema `ml`, volume `ml.inbox`, MLflow experiment ([devops/05](../devops/05_databricks_platform.md)).

**Đầu ra:**
- `ml.experiment_runs`: mỗi lần chạy một dòng, kể cả lần `timeout`.
- `ml.frequent_itemsets`, `ml.association_rules`: chỉ lưu cho các lần chạy được đánh dấu `persist`.
- `ml.holdout_metrics` (E7).
- MLflow experiment `/Shared/instacart-ds`.

**Xong khi:** mọi tổ hợp của E1–E5, E7 có dòng `status ∈ {ok, timeout}`; E6 có kết quả từ local ([03](03_hadoop_mapreduce_local.md)); E1 có `check_passed = true` ở cả 3 cấp item.

---

## 1. Câu hỏi thực nghiệm

| Mã | Câu hỏi | Biến thay đổi | Giữ cố định | Ứng với |
|---|---|---|---|---|
| **E1** | Các thuật toán có cho cùng kết quả không? MLlib FP-Growth có exhaustive không? | Thuật toán | 10% dữ liệu, 1 MinSupp mỗi cấp | Kiểm tra đúng đắn; nhận định về PFP của bài báo |
| **E2** | MinSupport ảnh hưởng thế nào tới số tập, số luật, thời gian, bộ nhớ? | `min_support` × cấp item × thuật toán | 4 core | Đề tài: "ngưỡng MinSupport khác nhau" |
| **E3** | Thời gian tăng thế nào khi số giao dịch tăng? | `scale_pct` 10 → 200 | product, MinSupp 0,002 | Bài báo: theo số giao dịch |
| **E4** | Thời gian tăng thế nào khi số item tăng? | `top_n` 100 → tất cả | product, 25% | Bài báo, Hình 12 (Otto). **Đề tài gốc chưa có** |
| **E5** | Thêm core thì nhanh lên bao nhiêu (Sₙ, Eₙ)? | `cores` 1, 2, 4 | 2 điểm cố định | Bài báo, Hình 14–15 |
| **E6** | Hadoop MapReduce hay Spark nhanh hơn trên cùng tác vụ? | Nền tảng × quy mô × core | Cụm local | Đề tài; bổ sung cho lập luận của bài báo ([03](03_hadoop_mapreduce_local.md)) |
| **E7** | Luật có dùng được để gợi ý không? Ngưỡng nào tốt nhất? | `min_support` × `min_confidence` | product, 100%, FP-Growth | Rubric 4 "Phân tích"; chọn champion ([05](05_mlflow_serving_bao_cao.md)) |

## 2. Lưới chi tiết và ngân sách

| Mã | Thuật toán | Cấp | Quy mô | Top-N | MinSupp | Core | Lặp | Số lần chạy |
|---|---|---|---|---|---|---|---|---|
| E1 | `all` (cả 3, so khớp) | 3 cấp | 10 | — | product 0,005; aisle 0,05; department 0,1 | 4 | 1 | 3 |
| E2 | DApriori, DATID, FP-Growth | 3 cấp | product 25; aisle, department 100 | — | lưới ở [01 §4](01_du_lieu_thuc_nghiem.md) (4 + 4 + 3 giá trị) | 4 | 3 | 99 |
| E3 | như E2 | product | 10, 25, 50, 100, 200 | — | 0,002 | 4 | 3 | 45 |
| E4 | như E2 | product | 25 | 100, 1.000, 10.000, tất cả | 0,002 | 4 | 3 | 36 |
| E5 | như E2 | product (25%, 0,002) và aisle (100%, 0,02) | | — | | 1, 2, 4 | 3 | 54 |
| E6 | MapReduce, Spark DF, Spark RDD | product | 10 → 200 | — | 0,001 | 1 → 8 | 3 | 81 (local) |
| E7 | FP-Growth + đánh giá | product | 100 | — | 0,001; 0,002; 0,005 | 4 | 1 | 9 |

**Ngân sách Databricks** (ước lượng, **đo lại ở mốc DS-1**):
- Khoảng 250 lần chạy, trung bình 3 phút mỗi lần, cộng thời gian khởi động cluster: khoảng 15–20 giờ máy `Standard_D4ds_v4`.
- Chi phí VM cộng DBU cỡ 0,4 USD/giờ, tức khoảng 6–8 USD. Cập nhật lại bằng giá của region đã chốt.
- Mỗi lần chạy có **timeout 30 phút**. Lần chạy vượt timeout ghi `status = 'timeout'`, và **vẫn là một điểm dữ liệu**: nó chứng minh giới hạn của thuật toán, giống các lần chạy hàng giờ trong bài báo.

**Mốc DS-1 (spike hiệu năng):** trước khi bật lưới, chạy E1 cộng một điểm mỗi thuật toán ở product 25% / 0,002. Nếu một thuật toán vượt 30 phút ở điểm đó, sửa lưới **trước** khi chạy E2–E5:
- Tăng MinSupp nhỏ nhất.
- Hoặc giảm quy mô của product từ 25% xuống 10%.

Ghi mọi thay đổi vào `experiments.yml` kèm lý do.

## 3. Định nghĩa lưới: `experiments.yml`

```yaml
# src/ml/experiments.yml
defaults: { data_version: v1, reps: 3, timeout_min: 30, cores: [4], top_n: [null], persist: false }
experiments:
  E1:
    reps: 1
    runs:
      - { algo: all, grain: product,    scale_pct: 10, min_support: [0.005], max_k: 3 }
      - { algo: all, grain: aisle,      scale_pct: 10, min_support: [0.05] }
      - { algo: all, grain: department, scale_pct: 10, min_support: [0.1] }
  E2:
    runs:
      - { algo: [dapriori, datid, fpgrowth], grain: product, scale_pct: 25, min_support: [0.01, 0.005, 0.002, 0.001], max_k: 3 }
      - { algo: [dapriori, datid, fpgrowth], grain: aisle, scale_pct: 100, min_support: [0.1, 0.05, 0.02, 0.01] }
      - { algo: [dapriori, datid, fpgrowth], grain: department, scale_pct: 100, min_support: [0.2, 0.1, 0.05] }
  E3:
    runs:
      - { algo: [dapriori, datid, fpgrowth], grain: product, scale_pct: [10, 25, 50, 100, 200], min_support: [0.002], max_k: 3 }
  E4:
    runs:
      - { algo: [dapriori, datid, fpgrowth], grain: product, scale_pct: 25, top_n: [100, 1000, 10000, null], min_support: [0.002], max_k: 3 }
  E5:
    cores: [1, 2, 4]
    runs:
      - { algo: [dapriori, datid, fpgrowth], grain: product, scale_pct: 25, min_support: [0.002], max_k: 3 }
      - { algo: [dapriori, datid, fpgrowth], grain: aisle, scale_pct: 100, min_support: [0.02] }
  E7:
    reps: 1
    persist: true                                   # lưu itemsets + rules để đánh giá và chọn champion
    runs:
      - { algo: fpgrowth, grain: product, scale_pct: 100, min_support: [0.001, 0.002, 0.005], min_confidence: [0.1, 0.2, 0.3], max_k: 3 }
```

- `min_confidence` mặc định ở E1–E5 lấy giá trị **nhỏ nhất** trong lưới của cấp đó ([01 §4](01_du_lieu_thuc_nghiem.md)). Thời gian sinh luật được đo riêng (`rules_s`).
- E7: 3 MinSupp × 3 MinConf, nhưng FP-Growth chỉ cần chạy **3 lần** (mỗi MinSupp một lần), rồi lọc luật theo từng MinConf. `run_experiment.py` tự gom các tổ hợp này.

## 4. Đo đạc: `measure.py`

| Chỉ số | Cách đo trên Databricks (single-node, `local[n]`) |
|---|---|
| `wall_s` | `time.perf_counter()` bao quanh phần tìm tập phổ biến (không tính đọc bảng lần đầu: dữ liệu được đếm một lần trước khi bấm giờ) |
| `rules_s` | Riêng phần sinh luật |
| `peak_mem_mb` | Lấy mẫu RSS mỗi 0,5 giây của tiến trình JVM driver và các tiến trình worker Python (`psutil`). Ghi `peak − baseline`, với baseline đo ngay trước khi chạy |
| `n_itemsets`, `n_itemsets_by_k` | Đếm trên kết quả |
| `n_rules_single`, `n_rules_all` | Luật một vế phải (so được với MLlib) / mọi dạng (chỉ DApriori, DATID) |
| `status` | `ok`, `timeout` (vượt `timeout_min`), `failed` (kèm `error`) |

```python
# src/ml/measure.py
import threading
import time
from contextlib import contextmanager

import psutil


class PeakRss:
    """Lấy mẫu tổng RSS (MB) của JVM Spark + worker PySpark trên node, chạy nền."""

    def __init__(self, every_s: float = 0.5):
        self.every_s, self.peak, self._stop = every_s, 0.0, threading.Event()

    @staticmethod
    def _sample() -> float:
        total = 0
        for p in psutil.process_iter(["name", "cmdline", "memory_info"]):
            cmd = " ".join(p.info["cmdline"] or [])
            if (p.info["name"] == "java" and "spark" in cmd.lower()) or "pyspark" in cmd:
                total += p.info["memory_info"].rss
        return total / 2**20

    def _loop(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, self._sample())
            time.sleep(self.every_s)

    def __enter__(self):
        self.baseline = self._sample()
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join()

    @property
    def delta_mb(self) -> float:
        return round(max(self.peak - self.baseline, 0.0), 1)


@contextmanager
def time_limit(sc, minutes: int, group: str):
    """Hủy mọi Spark job thuộc `group` khi quá hạn; nơi gọi bắt lỗi và ghi status = 'timeout'."""
    sc.setJobGroup(group, group, interruptOnCancel=True)
    expired = threading.Event()

    def cancel():
        expired.set()
        sc.cancelJobGroup(group)

    timer = threading.Timer(minutes * 60, cancel)
    timer.start()
    try:
        yield expired
    finally:
        timer.cancel()
```

- Đo RSS bằng `psutil` cho cách đo **giống nhau** ở cả Databricks lẫn local, và **không cần py4j**. Policy của DS vẫn dùng `SINGLE_USER`, nên py4j dùng được nếu sau này cần.
- **Giới hạn của phép đo** (ghi vào báo cáo): các vòng lặp trong một lần chạy job dùng chung một JVM driver, nên `peak − baseline` là bộ nhớ **tăng thêm** của lần chạy, không phải tổng bộ nhớ. Chỉ so tương đối giữa các thuật toán, giống cách bài báo trình bày.

## 5. Job `ds-benchmark`

### 5.1 Luồng

`plan` → `grid_c1` → `grid_c2` → `grid_c4` → `ingest_local` → `publish_gold`

| Task | Loại | Làm gì |
|---|---|---|
| `plan` | Python, cluster `c1` | Đọc `experiments.yml` (lọc theo tham số `experiments`), bung thành từng tổ hợp, **bỏ tổ hợp đã `ok`**, ghi phần còn lại vào `ml.experiment_queue`. Đặt task value `queue_c1/c2/c4` = danh sách `run_key` rút gọn (12 ký tự) theo số core |
| `grid_c{n}` | `for_each_task`, `concurrency: 1` | Mỗi vòng gọi `run_experiment.py --run-key <id>` trên cluster `local[n]`. Chạy tuần tự để các phép đo không chen nhau |
| `ingest_local` | `sql_task` (serverless) | `COPY INTO` từ `/Volumes/instacart/ml/inbox/local/` rồi `MERGE` vào `ml.experiment_runs` ([03 §7](03_hadoop_mapreduce_local.md)) |
| `publish_gold` | `sql_task` (serverless) | Tạo lại view `gold.*` của DS ([05 §4](05_mlflow_serving_bao_cao.md)) |

**Vì sao task value chỉ chứa `run_key` rút gọn:** task value có giới hạn kích thước. Tham số đầy đủ của mỗi tổ hợp nằm trong `ml.experiment_queue`, mỗi vòng tự đọc phần của mình.

**Vì sao 3 job cluster:** `spark.master = local[n]` chỉ đặt được lúc tạo cluster. Ba cluster chạy **nối tiếp** (`depends_on`), nên tại một thời điểm chỉ có một VM 4 vCPU.
- Lúc cluster trước đang tắt, cluster sau có thể vấp quota trong khoảng một phút.
- Vì vậy vòng đầu tiên của mỗi `grid_c{n}` có `max_retries: 2` và `min_retry_interval_millis: 180000`.

### 5.2 DAB

```yaml
# bundles/resources/jobs_ds.yml  (phần ds-benchmark)
resources:
  jobs:
    ds_benchmark:
      name: instacart-ds-benchmark
      max_concurrent_runs: 1
      parameters:
        - { name: experiments, default: "E1,E2,E3,E4,E5" }
        - { name: data_version, default: v1 }
      tasks:
        - task_key: plan
          job_cluster_key: c1
          spark_python_task:
            python_file: ../../src/ml/plan_grid.py
            parameters: ["--experiments", "{{job.parameters.experiments}}",
                         "--data-version", "{{job.parameters.data_version}}"]

        - task_key: grid_c1
          depends_on: [{ task_key: plan }]
          for_each_task:
            inputs: "{{tasks.plan.values.queue_c1}}"
            concurrency: 1
            task:
              task_key: grid_c1_iter
              job_cluster_key: c1
              max_retries: 2
              min_retry_interval_millis: 180000
              spark_python_task:
                python_file: ../../src/ml/run_experiment.py
                parameters: ["--run-key", "{{input}}"]

        # grid_c2 (depends_on grid_c1, cluster c2) và grid_c4 (depends_on grid_c2, cluster c4): giống hệt grid_c1

        - task_key: ingest_local
          depends_on: [{ task_key: grid_c4 }]
          run_if: ALL_DONE                          # lưới có vòng lỗi vẫn nạp kết quả local
          sql_task:
            file: { path: ../../src/ml/sql/ingest_local.sql }
            warehouse_id: ${var.warehouse_id}

        - task_key: publish_gold
          depends_on: [{ task_key: ingest_local }]
          sql_task:
            file: { path: ../../src/ml/sql/publish_gold.sql }
            warehouse_id: ${var.warehouse_id}

      job_clusters:
        - job_cluster_key: c1
          new_cluster:
            policy_id: ${var.ds_policy_id}      # khai đủ trường fixed, không dùng apply_policy_default_values (devops/05 §3 bước 6)
            spark_version: 15.4.x-scala2.12
            node_type_id: ${var.node_type_id}
            num_workers: 0
            data_security_mode: SINGLE_USER
            spark_conf:
              spark.databricks.cluster.profile: singleNode
              spark.master: "local[1]"
              spark.sql.shuffle.partitions: "16"      # cố định giữa 3 cluster để so được theo số core
              spark.default.parallelism: "16"
              spark.sql.adaptive.enabled: "false"
            spark_env_vars:
              PYTHONPATH: ${workspace.file_path}/src
            custom_tags: { ResourceClass: SingleNode, cost_owner: p4 }
        # c2, c4: giống c1, chỉ khác spark.master = local[2] / local[4]
```

- `ds_policy_id` là biến DAB, lookup theo tên policy `instacart-ds-benchmark` ([devops/05 bước 6](../devops/05_databricks_platform.md)).
- `PYTHONPATH` trỏ vào thư mục `src` mà bundle đã đồng bộ lên workspace, để `run_experiment.py` import được `ml.fim.*`. `workspace.file_path` của Databricks CLI mới đã có tiền tố `/Workspace`; kiểm tra lại giá trị thật bằng `databricks bundle validate -o json`.
- **Chạy ở đâu:** deploy job này với target `sandbox` lên workspace Sub C (bắt buộc, [devops/03 §5](../devops/03_terraform_azure.md)). Sub C có quota vCPU riêng và đọc `ds_data` qua metastore chung, nên chạy song song với DE. Không chạy job này trên workspace chính: nó chiếm hết 4 vCPU của pipeline batch và stream.

### 5.3 Một lần chạy: `run_experiment.py`

```python
# src/ml/run_experiment.py  (khung; các hàm _persist/_log_mlflow/_merge_result viết đầy đủ trong code)
import argparse
import json
import math
import time

import mlflow
from pyspark.sql import SparkSession

from ml.data import load_transactions, to_rdd
from ml.fim.dapriori import dapriori
from ml.fim.datid import datid
from ml.fim.fpgrowth import fpgrowth
from ml.fim.rules import gen_rules
from ml.measure import PeakRss, time_limit

QUEUE, RUNS = "instacart.ml.experiment_queue", "instacart.ml.experiment_runs"


def main() -> None:
    args = argparse.ArgumentParser()
    args.add_argument("--run-key", required=True)
    run_key = args.parse_args().run_key
    spark = SparkSession.builder.getOrCreate()
    cfg = spark.table(QUEUE).where(f"run_key LIKE '{run_key}%'").first().asDict()

    df = load_transactions(spark, grain=cfg["grain"], scale_pct=cfg["scale_pct"], top_n=cfg["top_n"],
                           data_version=cfg["data_version"])
    n_tx = df.count()                                   # đếm trước khi bấm giờ
    min_count = math.ceil(cfg["min_support"] * n_tx)
    row = {**cfg, "n_tx": n_tx, "status": "ok", "started_at": time.time()}
    expired = None

    try:
        with PeakRss() as mem, time_limit(spark.sparkContext, cfg["timeout_min"], cfg["run_key"]) as expired:
            t0 = time.perf_counter()
            if cfg["algo"] == "fpgrowth":
                model = fpgrowth(df, cfg["min_support"], cfg["min_confidence"])
                freq = {tuple(sorted(r["items"])): r["freq"] for r in model.freqItemsets.toLocalIterator()
                        if cfg["max_k"] is None or len(r["items"]) <= cfg["max_k"]}
            else:
                algo = {"dapriori": dapriori, "datid": datid}[cfg["algo"]]
                freq = algo(to_rdd(df), min_count, cfg["max_k"])
            row["wall_s"] = round(time.perf_counter() - t0, 2)
            t1 = time.perf_counter()
            rules = gen_rules(spark, freq, n_tx, cfg["min_confidence"]).cache()
            row["n_rules_all"] = rules.count()
            row["n_rules_single"] = rules.where("size(consequent) = 1").count()
            row["rules_s"] = round(time.perf_counter() - t1, 2)
        row["peak_mem_mb"] = mem.delta_mb
        row["n_itemsets"] = len(freq)
        row["n_itemsets_by_k"] = json.dumps({k: sum(len(s) == k for s in freq) for k in {len(s) for s in freq}})
        if cfg["persist"]:
            _persist(spark, cfg, freq, rules)            # ghi ml.frequent_itemsets / association_rules theo run_key
    except Exception as e:                               # noqa: BLE001: mọi lỗi đều phải được ghi lại
        row["status"] = "timeout" if expired is not None and expired.is_set() else "failed"
        row["error"] = str(e)[:2000]
    row["mlflow_run_id"] = _log_mlflow(cfg, row)
    _merge_result(spark, row)                            # MERGE INTO ml.experiment_runs ON run_key
    if row["status"] == "failed":
        raise SystemExit(1)                              # để task đỏ, Repair run chạy lại đúng vòng này
```

- `algo = all` (E1) chạy cả 3 thuật toán trên cùng `df`, so khớp như [02 §6.2](02_thuat_toan_spark.md), rồi ghi `check_passed`.
- **`timeout` không làm task đỏ.** Một tổ hợp quá nặng là kết quả hợp lệ, không được chặn các vòng sau.
- `_persist` ghi bằng `replaceWhere("run_key = '…'")`. Chạy lại một tổ hợp sẽ thay đúng phần dữ liệu của nó.

### 5.4 Bảng kết quả

```sql
CREATE TABLE IF NOT EXISTS instacart.ml.experiment_runs (
  run_key STRING NOT NULL, experiment STRING, platform STRING, algo STRING, grain STRING,
  scale_pct INT, top_n INT, min_support DOUBLE, min_confidence DOUBLE, max_k INT, cores INT, rep INT,
  data_version STRING, status STRING, error STRING,
  n_tx BIGINT, n_itemsets BIGINT, n_itemsets_by_k STRING, n_rules_single BIGINT, n_rules_all BIGINT,
  wall_s DOUBLE, rules_s DOUBLE, job_s DOUBLE, peak_mem_mb DOUBLE, spill_mb DOUBLE, n_result BIGINT,
  check_passed BOOLEAN, mlflow_run_id STRING, machine STRING, node_type STRING, spark_version STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP
) CLUSTER BY (experiment, algo);
```

`platform` có giá trị `databricks`, `local_spark_yarn`, `local_mapreduce`. Các cột `job_s`, `spill_mb`, `n_result` chủ yếu dành cho kết quả local (E6).

## 6. Chạy lại không trùng, không mất

| Tình huống | Cách chạy lại | Vì sao an toàn |
|---|---|---|
| Một vòng `for_each` lỗi (OOM, mất cluster) | **Repair run** → chỉ chạy lại vòng lỗi và các task sau | `MERGE … ON run_key` ghi đè đúng dòng; `_persist` dùng `replaceWhere` |
| Cả job bị hủy giữa lưới | Chạy lại job với cùng tham số | `plan` bỏ qua tổ hợp đã `ok`, chỉ xếp hàng phần còn lại |
| Muốn đo lại một tổ hợp đã `ok` | `DELETE FROM ml.experiment_runs WHERE run_key = …` rồi chạy lại job | Không có cách "ghi đè ngầm": phải xóa có chủ đích |
| Sửa lưới (đổi MinSupp) | Sửa `experiments.yml`, deploy, chạy lại | Tổ hợp mới có `run_key` mới; dòng cũ giữ nguyên, lọc theo `experiment` khi vẽ |
| Sửa thuật toán sau khi đã chạy | Tăng `algo_version` trong `run_key` (vd. `dapriori@2`), chạy lại phần liên quan | Không trộn số đo của hai phiên bản code |
| Vấp quota khi đổi cluster | Tự retry sau 3 phút (`max_retries: 2`) | Vòng chưa bắt đầu thì chưa ghi gì |

## 7. E7: đánh giá luật trên holdout

### 7.1 Cách đánh giá

Với mỗi đơn trong `ds_data.holdout_eval`:
- **`context`** là nửa đầu các món theo thứ tự bỏ vào giỏ. **`targets`** là phần còn lại ([de/08 §4](../de/08_du_lieu_cho_ds.md)).
- Gợi ý top-k món **không nằm trong `context`**. Điểm của món y là `max(confidence)` trên các luật X → y có X ⊆ `context`. Hòa điểm thì xét `lift`.

| Chỉ số | Công thức | Ý nghĩa |
|---|---|---|
| HitRate@k | Tỉ lệ đơn có ít nhất 1 món gợi ý thuộc `targets` | Gợi ý có "trúng" không |
| Precision@k | trung bình \|topk ∩ targets\| / k | Độ chính xác của danh sách |
| Recall@k | trung bình \|topk ∩ targets\| / \|targets\| | Bắt được bao nhiêu phần giỏ còn lại |
| Coverage (đơn) | Tỉ lệ đơn có ít nhất 1 luật kích hoạt, không tính bù phổ biến | MinSupp cao → ít luật → ít đơn có luật |
| Coverage (danh mục) | Số món khác nhau được gợi ý / số món trong `item_index` | Luật có chỉ xoay quanh vài món phổ biến không |

**Fallback khi không đủ luật:** danh sách gợi ý được bù bằng top-k món phổ biến nhất không nằm trong `context`, để API luôn có thể trả đủ món. Để đánh giá riêng đóng góp của luật, `ml.holdout_metrics` lưu thêm các cột `rule_only_*`, tính trên các luật mà không bù popular. Baseline `run_key = 'popularity'` chỉ dùng top-k món phổ biến. **Baseline bắt buộc:** luật kết hợp phải hơn baseline ở các cột `rule_only_*` thì mới có ý nghĩa; nếu không hơn, báo cáo phải nói thẳng như vậy. Báo cáo cũng nêu metric đã bù fallback để phản ánh hành vi API.

**Chống "chọn trên tập kiểm tra":** chọn cấu hình champion bằng `eval_part = 'val'`, báo cáo số cuối trên `eval_part = 'test'`.

### 7.2 SQL đánh giá (k = 10)

Truy vấn nguồn tại [`src/ml/sql/eval_holdout.sql`](../../src/ml/sql/eval_holdout.sql) tạo hai danh sách: gợi ý luật có bù popular để khớp hành vi serving và gợi ý chỉ từ luật để đo riêng chất lượng luật. Trường hợp `run_key = 'popularity'` chạy baseline độc lập. Popularity được xếp theo `item_index.idx` (chỉ số mã hóa), không dùng `item_key`.

- Mảng `antecedent` trong `ml.association_rules` và mảng tập con ở CTE `subsets` đều **tăng dần** (vì `context` tăng dần theo `idx`), nên so `=` giữa hai mảng là đúng. `holdout_eval.context` được DE sắp sẵn ([de/08 §4](../de/08_du_lieu_cho_ds.md)).
- Kết quả ghi vào `ml.holdout_metrics` (khóa: `run_key`, `min_confidence`, `eval_part`, `k`), có thêm một dòng cho baseline (`run_key = 'popularity'`). Test Spark chạy truy vấn trên bảng toy để kiểm tra cả run luật và baseline: [`tests/ml/test_evaluate_holdout.py`](../../tests/ml/test_evaluate_holdout.py).

## 8. Phân tích kết quả

```sql
-- Trung vị 3 lần đo, rồi speedup Sₙ = T₁/Tₙ và efficiency Eₙ = Sₙ/n (theo bài báo, công thức 6–7)
WITH m AS (SELECT experiment, platform, algo, grain, scale_pct, min_support, cores,
                  percentile(wall_s, 0.5) AS t, count(*) AS n_ok
           FROM instacart.ml.experiment_runs
           WHERE status = 'ok' AND experiment IN ('E5', 'E6')
           GROUP BY ALL)
SELECT m.*, t1.t / m.t AS speedup, (t1.t / m.t) / m.cores AS efficiency
FROM m JOIN m t1
  ON t1.cores = 1 AND (t1.experiment, t1.platform, t1.algo, t1.grain, t1.scale_pct, t1.min_support)
                    = (m.experiment, m.platform, m.algo, m.grain, m.scale_pct, m.min_support);
```

**Biểu đồ cần có trong báo cáo và Power BI trang 7:**

| Biểu đồ | Dữ liệu | So với bài báo |
|---|---|---|
| Thời gian theo MinSupp, mỗi thuật toán một đường, 3 cấp item | E2 | Hình 10–11 |
| Số tập phổ biến theo k, và số luật, theo MinSupp | E2 | Hình 16–17 |
| Thời gian theo quy mô (10 → 200%) | E3 | Theo số giao dịch |
| Thời gian theo số item (log) | E4 | Hình 12 |
| Bộ nhớ tăng thêm theo thuật toán | E2, E3 | Hình 13 |
| Speedup và efficiency theo core | E5 (Databricks), E6 (local) | Hình 14–15 |
| MapReduce và Spark: thời gian và byte ghi đĩa trung gian | E6 | Phần bổ sung |
| HitRate@10 / Precision@10 theo (MinSupp, MinConf), kèm đường baseline | E7 | Phần bổ sung |

**Cách đọc kết quả, để tránh kết luận quá tay:**
- Thời gian Databricks và thời gian local **không so trực tiếp** với nhau, vì khác phần cứng. Chỉ so trong cùng `platform`.
- Speedup trên Databricks dừng ở n = 4. Hiệu suất giảm khi tăng core là điều đã biết (bài báo cũng ghi nhận). Cần giải thích bằng tỉ lệ thời gian của phần chạy trên driver (`apriori_gen`, collect) so với tổng thời gian.
- Nếu FP-Growth **nhanh hơn** DApriori/DATID: đó là kết quả đúng với bài báo. Nếu FP-Growth cũng **exhaustive** (E1): kết luận "DATID là lựa chọn duy nhất khi cần đủ luật" của bài **không còn đúng** với MLlib hiện tại.
