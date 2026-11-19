CREATE TABLE IF NOT EXISTS instacart.ml.holdout_metrics (
  run_key STRING, min_confidence DOUBLE, eval_part STRING, k INT, n_eval BIGINT,
  hit_rate_at_k DOUBLE, precision_at_k DOUBLE, recall_at_k DOUBLE,
  order_coverage DOUBLE, catalog_coverage DOUBLE,
  rule_only_hit_rate_at_k DOUBLE, rule_only_precision_at_k DOUBLE,
  rule_only_recall_at_k DOUBLE, rule_only_catalog_coverage DOUBLE,
  evaluated_at TIMESTAMP
) USING DELTA;
