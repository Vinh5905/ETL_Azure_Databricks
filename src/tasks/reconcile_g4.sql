-- G4: chạy nguyên file, không bỏ SELECT assert_true. Cần init_reconciliation.sql trước.
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
SELECT assert_true(count(*) = 47
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
  'silver.prior.invalid',
  'g3.dim.rows',
  'g3.dim.keys',
  'g3.dim.payload',
  'g3.dim.guards',
  'g3.prices.open_duplicates',
  'g3.stream.orders',
  'g3.stream.header_keys',
  'g3.stream.headers',
  'g3.stream.items',
  'g3.stream.item_keys',
  'g3.stream.payload',
  'g3.stream.orphans',
  'g3.sources.overlap',
  'g3.orders.rows',
  'g3.orders.keys',
  'g3.orders.flags',
  'g3.items.rows',
  'g3.items.keys',
  'g3.items.payload',
  'g3.baskets.prior',
  'g3.baskets.holdout',
  'g3.baskets.keys',
  'g3.baskets.headers',
  'g3.baskets.valid',
  'g3.baskets.items');
MERGE INTO reconciliation t
USING (
  WITH e AS (
    SELECT map_from_entries(collect_list(named_struct('key', metric, 'value', expected))) AS counts

      FROM expected_counts
      WHERE profile = :profile
  ),
  fact AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:gold_schema, '`', '``') || '`.`fact_order_lines`')),
  all_items AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`order_items`')),
  fact_keys AS (
    SELECT order_id, product_id, count(*) AS key_count FROM fact GROUP BY order_id, product_id
  ),
  item_keys AS (
    SELECT order_id, product_id, count(*) AS key_count FROM all_items GROUP BY order_id, product_id
  ),
  checks AS (
    SELECT 'g4.fact.rows' AS check_name, CAST(counts['order_products_prior'] + counts['order_products_train'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM fact) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g4.fact.no_fanout' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id, product_id)
      FROM fact) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g4.fact.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT coalesce(sum(abs(coalesce(f.key_count, 0L) - coalesce(i.key_count, 0L))), 0L)
      FROM fact_keys f
      FULL OUTER JOIN item_keys i
        ON f.order_id <=> i.order_id AND f.product_id <=> i.product_id) AS actual
      FROM e
  )
  SELECT CAST(:check_date AS DATE) AS check_date, check_name, expected, actual, (expected = actual) AS passed, current_timestamp() AS checked_at, :run_id AS run_id, :profile AS profile, 'G4' AS stage, (SELECT sha2(to_json(named_struct('catalog', :catalog, 'ops', :ops_schema, 'bronze', :bronze_schema, 'silver', :silver_schema, 'gold', :gold_schema, 'counts', array_sort(collect_list(named_struct('metric', metric, 'expected', expected, 'source', source))))), 256)
      FROM expected_counts
      WHERE profile = :profile) AS config_hash
      FROM checks
) s
ON t.check_date = s.check_date
  AND t.check_name = s.check_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
SELECT assert_true(count(*) = 50
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
  'silver.prior.invalid',
  'g3.dim.rows',
  'g3.dim.keys',
  'g3.dim.payload',
  'g3.dim.guards',
  'g3.prices.open_duplicates',
  'g3.stream.orders',
  'g3.stream.header_keys',
  'g3.stream.headers',
  'g3.stream.items',
  'g3.stream.item_keys',
  'g3.stream.payload',
  'g3.stream.orphans',
  'g3.sources.overlap',
  'g3.orders.rows',
  'g3.orders.keys',
  'g3.orders.flags',
  'g3.items.rows',
  'g3.items.keys',
  'g3.items.payload',
  'g3.baskets.prior',
  'g3.baskets.holdout',
  'g3.baskets.keys',
  'g3.baskets.headers',
  'g3.baskets.valid',
  'g3.baskets.items',
  'g4.fact.rows',
  'g4.fact.no_fanout',
  'g4.fact.keys');
