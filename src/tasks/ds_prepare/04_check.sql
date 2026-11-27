-- Chặn bàn giao nếu snapshot, giao dịch hoặc tập đánh giá của phiên bản chưa đạt.
WITH metadata AS (
  SELECT count(*) AS n, max(n_prior) AS n_prior, max(n_holdout) AS n_holdout
  FROM IDENTIFIER(:data_versions)
  WHERE data_version = :v
), frozen AS (
  SELECT count_if(split = 'prior') AS n_prior, count_if(split = 'holdout') AS n_holdout
  FROM IDENTIFIER(:baskets)
  WHERE data_version = :v
), live AS (
  SELECT count_if(split = 'prior') AS n_prior, count_if(split = 'holdout') AS n_holdout
  FROM IDENTIFIER(:source_baskets)
)
SELECT assert_true(
  m.n = 1 AND m.n_prior > 0 AND m.n_holdout > 0
  AND m.n_prior = f.n_prior AND m.n_holdout = f.n_holdout
  AND m.n_prior = l.n_prior AND m.n_holdout = l.n_holdout,
  'Snapshot thiếu metadata hoặc số giỏ prior/holdout lệch nguồn'
)
FROM metadata m CROSS JOIN frozen f CROSS JOIN live l;

WITH per_grain AS (
  SELECT grain, count(*) AS n, count(DISTINCT order_id) AS unique_orders,
         count_if(scale_bucket < 0 OR scale_bucket >= 100 OR size(items) <> n_items OR n_items < 1) AS malformed
  FROM IDENTIFIER(:transactions)
  WHERE data_version = :v AND split = 'prior'
  GROUP BY grain
)
SELECT assert_true(
  count(*) = 3 AND count_if(grain IN ('product', 'aisle', 'department')) = 3
  AND count_if(n <> unique_orders OR malformed <> 0 OR n <> (
    SELECT n_prior FROM IDENTIFIER(:data_versions) WHERE data_version = :v
  )) = 0,
  'Số giỏ prior theo cấp không khớp snapshot hoặc giao dịch có khóa trùng hay bucket sai'
)
FROM per_grain;

SELECT assert_true(sum(n_items) = (
    SELECT sum(n_items) FROM IDENTIFIER(:baskets) WHERE data_version = :v AND split = 'prior'
  ), 'Tổng số món cấp product lệch snapshot prior')
FROM IDENTIFIER(:transactions)
WHERE data_version = :v AND grain = 'product' AND split = 'prior';

SELECT assert_true(
  count_if(grain = 'product') > 0 AND count_if(grain = 'department') = 21 AND count_if(grain = 'aisle') = 134,
  'Item index thiếu cấp product hoặc số item department/aisle sai'
)
FROM IDENTIFIER(:item_index)
WHERE data_version = :v;

SELECT assert_true(
  count(*) > 0 AND count(*) = count(DISTINCT order_id)
  AND count_if(eval_part NOT IN ('val', 'test') OR size(context) = 0 OR size(targets) = 0) = 0
  AND abs(count_if(eval_part = 'val') - count_if(eval_part = 'test')) < 0.05 * count(*),
  'Holdout rỗng, có đơn sai hợp đồng hoặc chia val/test lệch'
)
FROM IDENTIFIER(:holdout_eval)
WHERE data_version = :v;
