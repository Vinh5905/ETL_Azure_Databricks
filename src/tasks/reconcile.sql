-- batch: chạy nguyên file, không bỏ SELECT assert_true. Cần init_reconciliation.sql trước.
SELECT assert_true(length(trim(:catalog)) > 0
  AND length(trim(:ops_schema)) > 0
  AND length(trim(:bronze_schema)) > 0
  AND length(trim(:silver_schema)) > 0
  AND length(trim(:gold_schema)) > 0
  AND length(trim(:profile)) > 0
  AND length(trim(:run_id)) > 0
  AND :check_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  AND try_cast(:check_date AS DATE) IS NOT NULL, 'invalid reconciliation parameters');
USE IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`');
SELECT assert_true(count(*) = 9
  AND count(DISTINCT metric) = 9
  AND min(expected) >= 0
  AND count_if(metric IN ('orders', 'order_products_prior', 'order_products_train', 'products', 'aisles', 'departments', 'orders_prior', 'orders_train', 'orders_test')) = 9, 'missing or duplicate expected_counts profile')
      FROM expected_counts
      WHERE profile = :profile;
SELECT assert_true(max(CASE WHEN metric = 'orders' THEN expected END) = sum(CASE WHEN metric IN ('orders_prior','orders_train','orders_test') THEN expected ELSE 0 END), 'expected order splits do not add up')
      FROM expected_counts
      WHERE profile = :profile;
MERGE INTO reconciliation t
USING (
  WITH e AS (
    SELECT map_from_entries(collect_list(named_struct('key', metric, 'value', expected))) AS counts

      FROM expected_counts
      WHERE profile = :profile
  ),
  orders_raw AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`orders`')),
  order_products_prior_raw AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`order_products_prior`')),
  order_products_train AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`order_products_train`')),
  products AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`products`')),
  aisles AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`aisles`')),
  departments AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`departments`')),
  clock AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`order_time_sim`')),
  batch AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`orders_batch`')),
  prior AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`order_items_prior`')),
  orders AS (
    SELECT * FROM (
      SELECT b.*, row_number() OVER (PARTITION BY order_id ORDER BY
        CASE WHEN _rescued_data IS NULL AND order_id > 0 AND user_id > 0
          AND eval_set IN ('prior','train','test') AND order_number >= 1
          AND order_dow BETWEEN 0 AND 6 AND order_hour_of_day BETWEEN 0 AND 23
          AND (days_since_prior_order IS NULL OR days_since_prior_order BETWEEN 0 AND 30)
          AND (order_number = 1 OR days_since_prior_order IS NOT NULL)
          AND _ingest_ts IS NOT NULL THEN 1 ELSE 0 END DESC,
        _ingest_ts DESC NULLS LAST, sha2(to_json(struct(b.*)), 256)) AS _canonical_rank
      FROM orders_raw b
    ) WHERE _canonical_rank = 1
  ),
  order_products_prior AS (
    SELECT * FROM (
      SELECT b.*, row_number() OVER (PARTITION BY order_id, product_id ORDER BY
        _ingest_ts DESC NULLS LAST, _file_mtime DESC NULLS LAST, _source_file DESC NULLS LAST) AS _canonical_rank
      FROM order_products_prior_raw b
      WHERE _rescued_data IS NULL AND order_id > 0 AND product_id > 0
        AND add_to_cart_order >= 1 AND reordered IN (0, 1) AND _ingest_ts IS NOT NULL
    ) WHERE _canonical_rank = 1
  ),
  batch_expected AS (SELECT order_id, user_id, eval_set FROM orders WHERE eval_set IN ('prior','test')),
  checks AS (
    SELECT 'bronze.orders' AS check_name, CAST(counts['orders'] AS BIGINT) AS expected,
      (SELECT count(DISTINCT order_id) FROM orders_raw) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.order_products_prior' AS check_name, CAST(counts['order_products_prior'] AS BIGINT) AS expected,
      (SELECT count(DISTINCT order_id, product_id) FROM order_products_prior_raw) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.order_products_train' AS check_name, CAST(counts['order_products_train'] AS BIGINT) AS expected,
      (SELECT count(DISTINCT order_id, product_id) FROM order_products_train) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.products' AS check_name, CAST(counts['products'] AS BIGINT) AS expected,
      (SELECT count(DISTINCT product_id) FROM products) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.aisles' AS check_name, CAST(counts['aisles'] AS BIGINT) AS expected,
      (SELECT count(DISTINCT aisle_id) FROM aisles) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.departments' AS check_name, CAST(counts['departments'] AS BIGINT) AS expected,
      (SELECT count(DISTINCT department_id) FROM departments) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.orders.prior' AS check_name, CAST(counts['orders_prior'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM orders
      WHERE eval_set = 'prior') AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.orders.train' AS check_name, CAST(counts['orders_train'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM orders
      WHERE eval_set = 'train') AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.orders.test' AS check_name, CAST(counts['orders_test'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM orders
      WHERE eval_set = 'test') AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.orders.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count_if(order_id IS NULL OR order_id <= 0) FROM orders) AS actual
      FROM e
    UNION ALL
    SELECT 'bronze.orders.invalid' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM orders
      WHERE order_id IS NULL OR user_id IS NULL OR order_id <= 0 OR user_id <= 0 OR eval_set IS NULL OR eval_set NOT IN ('prior','train','test')) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.clock.rows' AS check_name, CAST(counts['orders'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM clock) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.clock.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id)
      FROM clock) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.clock.null_ts' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM clock
      WHERE order_ts_sim IS NULL) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.batch.rows' AS check_name, CAST(counts['orders_prior'] + counts['orders_test'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM batch) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.batch.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id)
      FROM batch) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.batch.headers' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM batch l
       FULL OUTER JOIN batch_expected r USING (order_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL
         OR NOT (l.user_id <=> r.user_id)
         OR NOT (l.eval_set <=> r.eval_set)) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.prior.rows' AS check_name, CAST(counts['order_products_prior'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM prior) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.prior.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id, product_id)
      FROM prior) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.prior.payload' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM prior l
       FULL OUTER JOIN order_products_prior r USING (order_id, product_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL
         OR NOT (l.add_to_cart_order <=> r.add_to_cart_order)
         OR NOT (l.reordered <=> r.reordered)) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.prior.orphans' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM prior i LEFT ANTI JOIN batch o ON i.order_id = o.order_id
  AND o.eval_set = 'prior') AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'silver.prior.invalid' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM prior
      WHERE order_id IS NULL OR product_id IS NULL OR order_id <= 0 OR product_id <= 0) AS BIGINT) AS actual
      FROM e
  )
  SELECT CAST(:check_date AS DATE) AS check_date, check_name, expected, actual, (expected = actual) AS passed, current_timestamp() AS checked_at, :run_id AS run_id, :profile AS profile, 'batch' AS stage, (SELECT sha2(to_json(named_struct('catalog', :catalog, 'ops', :ops_schema, 'bronze', :bronze_schema, 'silver', :silver_schema, 'gold', :gold_schema, 'counts', array_sort(collect_list(named_struct('metric', metric, 'expected', expected, 'source', source))))), 256)
      FROM expected_counts
      WHERE profile = :profile) AS config_hash
      FROM checks
) s
ON t.check_date = s.check_date
  AND t.check_name = s.check_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
SELECT assert_true(count(*) = 22
  AND count_if(NOT passed OR run_id <> :run_id OR profile <> :profile OR config_hash <> (SELECT sha2(to_json(named_struct('catalog', :catalog, 'ops', :ops_schema, 'bronze', :bronze_schema, 'silver', :silver_schema, 'gold', :gold_schema, 'counts', array_sort(collect_list(named_struct('metric', metric, 'expected', expected, 'source', source))))), 256)
      FROM expected_counts
      WHERE profile = :profile)) = 0,
  'reconciliation failed or prerequisite checks are stale')
FROM reconciliation
      WHERE check_date = CAST(:check_date AS DATE)
  AND check_name IN ('bronze.orders',
  'bronze.order_products_prior',
  'bronze.order_products_train',
  'bronze.products',
  'bronze.aisles',
  'bronze.departments',
  'bronze.orders.prior',
  'bronze.orders.train',
  'bronze.orders.test',
  'bronze.orders.keys',
  'bronze.orders.invalid',
  'silver.clock.rows',
  'silver.clock.keys',
  'silver.clock.null_ts',
  'silver.batch.rows',
  'silver.batch.keys',
  'silver.batch.headers',
  'silver.prior.rows',
  'silver.prior.keys',
  'silver.prior.payload',
  'silver.prior.orphans',
  'silver.prior.invalid');
