-- Tạo các bảng DS trước khi lập hàng đợi hoặc chạy thực nghiệm. Có thể chạy lại an toàn.
CREATE TABLE IF NOT EXISTS instacart.ml.experiment_queue (
  run_key STRING NOT NULL, algo_version STRING, experiment STRING, platform STRING, algo STRING, grain STRING,
  scale_pct INT, top_n INT, min_support DOUBLE, min_confidence DOUBLE, max_k INT,
  cores INT, rep INT, data_version STRING, timeout_min INT, persist BOOLEAN,
  sweep STRING, n_tx BIGINT
) USING DELTA;

-- Cột theo docs/ds/04 §5.4, thêm algo_version, timeout_min, persist, sweep, check_detail, mlflow_error.
-- machine là chuỗi JSON (E6 local ghi {"name", "docker_cpus", "docker_mem_gb"}), để COPY INTO và MERGE dùng chung.
CREATE TABLE IF NOT EXISTS instacart.ml.experiment_runs (
  run_key STRING NOT NULL, algo_version STRING, experiment STRING, platform STRING, algo STRING, grain STRING,
  scale_pct INT, top_n INT, min_support DOUBLE, min_confidence DOUBLE, max_k INT,
  cores INT, rep INT, data_version STRING, timeout_min INT, persist BOOLEAN, sweep STRING,
  status STRING, error STRING,
  n_tx BIGINT, n_itemsets BIGINT, n_itemsets_by_k STRING, n_rules_single BIGINT, n_rules_all BIGINT,
  wall_s DOUBLE, rules_s DOUBLE, job_s DOUBLE, peak_mem_mb DOUBLE, spill_mb DOUBLE, n_result BIGINT,
  check_passed BOOLEAN, check_detail STRING, mlflow_run_id STRING, mlflow_error STRING,
  machine STRING, node_type STRING, spark_version STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP
) USING DELTA
CLUSTER BY (experiment, algo);

CREATE TABLE IF NOT EXISTS instacart.ml.frequent_itemsets (
  run_key STRING, items ARRAY<INT>, count BIGINT, support DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS instacart.ml.association_rules (
  run_key STRING, antecedent ARRAY<INT>, consequent ARRAY<INT>,
  support DOUBLE, confidence DOUBLE, lift DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS instacart.ml.holdout_metrics (
  run_key STRING, min_confidence DOUBLE, eval_part STRING, k INT, n_eval BIGINT,
  hit_rate_at_k DOUBLE, precision_at_k DOUBLE, recall_at_k DOUBLE,
  order_coverage DOUBLE, catalog_coverage DOUBLE,
  rule_only_hit_rate_at_k DOUBLE, rule_only_precision_at_k DOUBLE,
  rule_only_recall_at_k DOUBLE, rule_only_catalog_coverage DOUBLE,
  evaluated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS instacart.ml.champion (
  alias STRING, run_key STRING, min_confidence DOUBLE, model_version STRING,
  hit_rate_at_10 DOUBLE, baseline_hit_rate_at_10 DOUBLE, chosen_at TIMESTAMP
) USING DELTA;
