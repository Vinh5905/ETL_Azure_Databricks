-- Tạo các bảng DS trước khi lập hàng đợi hoặc chạy thực nghiệm. Có thể chạy lại an toàn.
CREATE TABLE IF NOT EXISTS instacart.ml.experiment_queue (
  run_key STRING, experiment STRING, platform STRING, algo STRING, grain STRING,
  scale_pct INT, top_n INT, min_support DOUBLE, min_confidence DOUBLE, max_k INT,
  cores INT, rep INT, data_version STRING, timeout_min INT, persist BOOLEAN,
  sweep STRING, n_tx BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS instacart.ml.experiment_runs (
  run_key STRING, experiment STRING, platform STRING, algo STRING, grain STRING,
  scale_pct INT, top_n INT, min_support DOUBLE, min_confidence DOUBLE, max_k INT,
  cores INT, rep INT, data_version STRING, timeout_min INT, persist BOOLEAN,
  sweep STRING, n_tx BIGINT, n_result BIGINT, wall_s DOUBLE, rules_s DOUBLE,
  peak_mem_mb DOUBLE, spill_mb DOUBLE, n_itemsets BIGINT, n_itemsets_by_k STRING,
  n_rules_single BIGINT, n_rules_all BIGINT, check_passed BOOLEAN, check_detail STRING,
  status STRING, error STRING, mlflow_error STRING, mlflow_run_id STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP,
  machine STRUCT<name:STRING,docker_cpus:INT,docker_mem_gb:INT>
) USING DELTA;

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
  order_coverage DOUBLE, catalog_coverage DOUBLE, evaluated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS instacart.ml.champion (
  alias STRING, run_key STRING, min_confidence DOUBLE, model_version STRING,
  hit_rate_at_10 DOUBLE, baseline_hit_rate_at_10 DOUBLE, chosen_at TIMESTAMP
) USING DELTA;
