-- G3: chạy nguyên file, không bỏ SELECT assert_true. Cần init_reconciliation.sql trước.
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
MERGE INTO reconciliation t
USING (
  WITH e AS (
    SELECT map_from_entries(collect_list(named_struct('key', metric, 'value', expected))) AS counts

      FROM expected_counts
      WHERE profile = :profile
  ),
  orders_raw AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`orders`')),
  products_raw AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`products`')),
  order_products_train_raw AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:bronze_schema, '`', '``') || '`.`order_products_train`')),
  batch AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`orders_batch`')),
  prior AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`order_items_prior`')),
  dim AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`dim_product`')),
  prices AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`product_prices_history`')),
  stream_orders AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`stream_orders`')),
  stream_items AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`stream_order_items`')),
  all_orders AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`orders`')),
  all_items AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`order_items`')),
  baskets AS (SELECT *
      FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`.`baskets`')),
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
  products AS (
    SELECT * FROM (
      SELECT b.*, row_number() OVER (PARTITION BY product_id ORDER BY
        _ingest_ts DESC NULLS LAST, _file_mtime DESC NULLS LAST, _source_file DESC NULLS LAST, sha2(to_json(struct(product_name, aisle_id, department_id)), 256)) AS _canonical_rank
      FROM products_raw b
      WHERE _rescued_data IS NULL AND product_id IS NOT NULL
    ) WHERE _canonical_rank = 1
  ),
  order_products_train AS (
    SELECT * FROM (
      SELECT b.*, row_number() OVER (PARTITION BY order_id, product_id ORDER BY
        _ingest_ts DESC NULLS LAST, _file_mtime DESC NULLS LAST, _source_file DESC NULLS LAST) AS _canonical_rank
      FROM order_products_train_raw b
      WHERE _rescued_data IS NULL AND order_id > 0 AND product_id > 0
        AND add_to_cart_order >= 1 AND reordered IN (0, 1) AND _ingest_ts IS NOT NULL
    ) WHERE _canonical_rank = 1
  ),
  expected_stream_headers AS (SELECT order_id, user_id FROM orders WHERE eval_set = 'train'),
  expected_items AS (
    SELECT order_id, product_id, add_to_cart_order, reordered, 'batch' AS source FROM prior
    UNION ALL
    SELECT order_id, product_id, add_to_cart_order, reordered, 'stream' AS source FROM stream_items
  ),
  expected_basket_headers AS (
    SELECT order_id, user_id, CASE WHEN eval_set = 'prior' THEN 'prior' ELSE 'holdout' END AS split
    FROM orders WHERE eval_set IN ('prior','train')
  ),
  basket_items AS (SELECT order_id, explode(items) AS product_id FROM baskets),
  checks AS (
    SELECT 'g3.dim.rows' AS check_name, CAST(counts['products'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM dim) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.dim.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT product_id)
      FROM dim) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.dim.payload' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM dim l
       FULL OUTER JOIN products r USING (product_id)
       WHERE l.product_id IS NULL OR r.product_id IS NULL
         OR NOT (l.product_name <=> r.product_name)
         OR NOT (l.aisle_id <=> r.aisle_id)
         OR NOT (l.department_id <=> r.department_id)) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.dim.guards' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count_if(product_id IS NULL OR product_id <= 0) FROM dim) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.prices.open_duplicates' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM (SELECT product_id
      FROM prices
      WHERE __END_AT IS NULL GROUP BY product_id HAVING count(*) > 1)) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.orders' AS check_name, CAST(counts['orders_train'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM stream_orders) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.header_keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id)
      FROM stream_orders) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.headers' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM stream_orders l
       FULL OUTER JOIN expected_stream_headers r USING (order_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL
         OR NOT (l.user_id <=> r.user_id)) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.items' AS check_name, CAST(counts['order_products_train'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM stream_items) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.item_keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id, product_id)
      FROM stream_items) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.payload' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM stream_items l
       FULL OUTER JOIN order_products_train r USING (order_id, product_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL
         OR NOT (l.add_to_cart_order <=> r.add_to_cart_order)
         OR NOT (l.reordered <=> r.reordered)) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.stream.orphans' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM stream_items i LEFT ANTI JOIN stream_orders o ON i.order_id = o.order_id) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.sources.overlap' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM batch b JOIN stream_orders s ON b.order_id = s.order_id) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.orders.rows' AS check_name, CAST(counts['orders'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM all_orders) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.orders.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id)
      FROM all_orders) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.orders.flags' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM all_orders
      WHERE NOT coalesce((eval_set IN ('prior','test')
  AND source = 'batch'
  AND is_test = (eval_set = 'test')) OR (eval_set = 'train'
  AND source = 'stream'
  AND NOT is_test), false)) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.items.rows' AS check_name, CAST(counts['order_products_prior'] + counts['order_products_train'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM all_items) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.items.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id, product_id)
      FROM all_items) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.items.payload' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM all_items l
       FULL OUTER JOIN expected_items r USING (order_id, product_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL
         OR NOT (l.add_to_cart_order <=> r.add_to_cart_order)
         OR NOT (l.reordered <=> r.reordered)
         OR NOT (l.source <=> r.source)) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.baskets.prior' AS check_name, CAST(counts['orders_prior'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM baskets
      WHERE split = 'prior') AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.baskets.holdout' AS check_name, CAST(counts['orders_train'] AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM baskets
      WHERE split = 'holdout') AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.baskets.keys' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*) - count(DISTINCT order_id)
      FROM baskets) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.baskets.headers' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM baskets l
       FULL OUTER JOIN expected_basket_headers r USING (order_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL
         OR NOT (l.user_id <=> r.user_id)
         OR NOT (l.split <=> r.split)) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.baskets.valid' AS check_name, CAST(0 AS BIGINT) AS expected,
      CAST((SELECT count(*)
      FROM baskets
      WHERE NOT coalesce(split IN ('prior','holdout')
  AND n_items >= 1
  AND n_items = size(items)
  AND items = array_sort(array_distinct(items))
  AND NOT exists(items, x -> x IS NULL OR x <= 0)
  AND order_id > 0 AND user_id > 0, false)) AS BIGINT) AS actual
      FROM e
    UNION ALL
    SELECT 'g3.baskets.items' AS check_name, CAST(0 AS BIGINT) AS expected,
      (SELECT count(*) FROM basket_items l
       FULL OUTER JOIN expected_items r USING (order_id, product_id)
       WHERE l.order_id IS NULL OR r.order_id IS NULL) AS actual
      FROM e
  )
  SELECT CAST(:check_date AS DATE) AS check_date, check_name, expected, actual, (expected = actual) AS passed, current_timestamp() AS checked_at, :run_id AS run_id, :profile AS profile, 'G3' AS stage, (SELECT sha2(to_json(named_struct('catalog', :catalog, 'ops', :ops_schema, 'bronze', :bronze_schema, 'silver', :silver_schema, 'gold', :gold_schema, 'counts', array_sort(collect_list(named_struct('metric', metric, 'expected', expected, 'source', source))))), 256)
      FROM expected_counts
      WHERE profile = :profile) AS config_hash
      FROM checks
) s
ON t.check_date = s.check_date
  AND t.check_name = s.check_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
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
