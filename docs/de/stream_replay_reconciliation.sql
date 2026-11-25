-- Gán tên bảng đầy đủ. Các bảng stream cùng thuộc một pipeline dev.
-- train_items là bảng batch instacart.bronze.order_products_train.
WITH train AS (
    SELECT count(*) AS train_items,
           count(DISTINCT order_id) AS train_orders,
           count(DISTINCT order_id, product_id) AS train_item_keys
    FROM IDENTIFIER(:train_items)
), inbound AS (
    SELECT get_json_object(value_json, '$.replay_run_id') AS run_id,
           get_json_object(value_json, '$.event_id') AS event_id
    FROM IDENTIFIER(:bronze_events)
), bronze AS (
    SELECT count(*) AS bronze_rows,
           count_if(run_id = :first_run_id) AS bronze_first_run,
           count_if(run_id = :replay_run_id) AS bronze_replay_run,
           count(DISTINCT CASE WHEN run_id = :first_run_id THEN event_id END) AS bronze_first_event_ids,
           count(DISTINCT CASE WHEN run_id = :replay_run_id THEN event_id END) AS bronze_replay_event_ids
    FROM inbound
), silver_orders AS (
    SELECT count(*) AS silver_orders,
           count(DISTINCT order_id) AS silver_order_keys,
           count_if(replay_run_id = :first_run_id) AS silver_first_run,
           count_if(replay_run_id = :replay_run_id) AS silver_replay_run
    FROM IDENTIFIER(:silver_orders)
), silver_items AS (
    SELECT count(*) AS silver_items,
           count(DISTINCT order_id, product_id) AS silver_item_keys
    FROM IDENTIFIER(:silver_items)
), quarantine AS (
    SELECT count(*) AS quarantined
    FROM IDENTIFIER(:quarantine_events)
)
SELECT train.*, bronze.*, silver_orders.*, silver_items.*, quarantine.*
FROM train CROSS JOIN bronze CROSS JOIN silver_orders CROSS JOIN silver_items CROSS JOIN quarantine;
