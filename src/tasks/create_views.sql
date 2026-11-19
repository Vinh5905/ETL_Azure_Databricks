-- Tham số STRING bắt buộc: catalog, silver_schema (từng thành phần, không kèm backtick).
-- Chạy cả file trong cùng phiên SQL: chọn namespace trước, không đặt parameter trong thân view.
USE IDENTIFIER(
  '`' || replace(:catalog, '`', '``') || '`.`' || replace(:silver_schema, '`', '``') || '`'
);

CREATE OR REPLACE VIEW orders AS
SELECT order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
       days_since_prior_order, order_ts_sim, source, is_test
FROM orders_batch
UNION ALL
SELECT order_id, user_id, 'train' AS eval_set, order_number, order_dow, order_hour_of_day,
       days_since_prior_order, event_time AS order_ts_sim, 'stream' AS source, false AS is_test
FROM stream_orders;

CREATE OR REPLACE VIEW order_items AS
SELECT order_id, product_id, add_to_cart_order, reordered, 'batch' AS source
FROM order_items_prior
UNION ALL
SELECT order_id, product_id, add_to_cart_order, reordered, 'stream' AS source
FROM stream_order_items;
