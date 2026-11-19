USE IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`');
CREATE TABLE IF NOT EXISTS expected_counts (
  profile STRING NOT NULL, metric STRING NOT NULL, expected BIGINT NOT NULL,
  source STRING NOT NULL, configured_at TIMESTAMP_LTZ NOT NULL
) USING DELTA;
CREATE TABLE IF NOT EXISTS reconciliation (
  check_date DATE NOT NULL, check_name STRING NOT NULL, expected BIGINT NOT NULL, actual BIGINT NOT NULL,
  passed BOOLEAN NOT NULL, checked_at TIMESTAMP_LTZ NOT NULL,
  run_id STRING NOT NULL, profile STRING NOT NULL, stage STRING NOT NULL, config_hash STRING NOT NULL
) USING DELTA;
-- Số tham chiếu, phải xác minh với nguồn Zenodo trước dùng. Không ghi đè kỳ vọng người chạy đã cấu hình.
MERGE INTO expected_counts t
USING (SELECT *
      FROM VALUES
  ('instacart_2017', 'orders', 3421083, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'order_products_prior', 32434489, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'order_products_train', 1384617, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'products', 49688, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'aisles', 134, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'departments', 21, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'orders_prior', 3214874, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'orders_train', 131209, 'instacart-2017-reference-verify-source'),
  ('instacart_2017', 'orders_test', 75000, 'instacart-2017-reference-verify-source') AS v(profile, metric, expected, source)) s
ON t.profile = s.profile
  AND t.metric = s.metric
WHEN NOT MATCHED THEN INSERT (profile, metric, expected, source, configured_at)
VALUES (s.profile, s.metric, s.expected, s.source, current_timestamp());
