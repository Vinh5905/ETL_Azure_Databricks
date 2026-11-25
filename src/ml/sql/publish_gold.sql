-- Power BI chỉ đọc gold, view chạy theo quyền của chủ view trên các bảng ml và ds_data.
CREATE OR REPLACE VIEW instacart.gold.association_rules
COMMENT 'Luật kết hợp của champion cấp product, có tên và mã sản phẩm.'
AS
WITH champion AS (
  SELECT run_key, min_confidence FROM instacart.ml.champion WHERE alias = 'champion'
),
names AS (
  SELECT idx, CAST(item_key AS INT) AS product_id, item_name
  FROM instacart.ds_data.item_index WHERE data_version = 'v1' AND grain = 'product'
),
name_maps AS (
  SELECT map_from_entries(collect_list(struct(idx, item_name))) AS item_names,
         map_from_entries(collect_list(struct(idx, product_id))) AS product_ids
  FROM names
),
rules AS (
  SELECT r.* FROM instacart.ml.association_rules r JOIN champion c
    ON r.run_key = c.run_key AND r.confidence >= c.min_confidence
)
SELECT concat_ws(' + ', transform(r.antecedent, x -> element_at(m.item_names, x))) AS antecedent_names,
       concat_ws(' + ', transform(r.consequent, x -> element_at(m.item_names, x))) AS consequent_names,
       size(r.antecedent) AS antecedent_size,
       transform(r.antecedent, x -> element_at(m.product_ids, x)) AS antecedent_product_ids,
       transform(r.consequent, x -> element_at(m.product_ids, x)) AS consequent_product_ids,
       element_at(m.product_ids, element_at(r.consequent, 1)) AS consequent_product_id,
       r.support, r.confidence, r.lift
FROM rules r CROSS JOIN name_maps m;

CREATE OR REPLACE VIEW instacart.gold.recommendations
COMMENT 'Mười luật mua kèm hàng đầu theo sản phẩm của champion.'
AS
WITH champion AS (
  SELECT run_key, min_confidence FROM instacart.ml.champion WHERE alias = 'champion'
),
ids AS (
  SELECT idx, CAST(item_key AS INT) AS product_id
  FROM instacart.ds_data.item_index WHERE data_version = 'v1' AND grain = 'product'
),
ranked AS (
  SELECT ia.product_id, ic.product_id AS recommended_product_id,
         r.confidence, r.lift,
         row_number() OVER (PARTITION BY ia.product_id ORDER BY r.confidence DESC, r.lift DESC, ic.product_id) AS rank
  FROM instacart.ml.association_rules r JOIN champion c
    ON r.run_key = c.run_key AND r.confidence >= c.min_confidence
  JOIN ids ia ON ia.idx = element_at(r.antecedent, 1)
  JOIN ids ic ON ic.idx = element_at(r.consequent, 1)
  WHERE size(r.antecedent) = 1 AND size(r.consequent) = 1
)
SELECT product_id, recommended_product_id, confidence, lift, rank FROM ranked WHERE rank <= 10;

CREATE OR REPLACE VIEW instacart.gold.experiment_metrics
COMMENT 'Trung vị (chính xác) phép đo, tốc độ tương đối và số lần thành công hoặc timeout theo cấu hình.'
AS
WITH metrics AS (
  SELECT experiment, platform, algo, grain, scale_pct, top_n, min_support, cores, sweep,
         percentile(wall_s, 0.5) AS wall_s,
         percentile(peak_mem_mb, 0.5) AS peak_mem_mb,
         percentile(spill_mb, 0.5) AS spill_mb,
         max(n_itemsets) AS n_itemsets,
         max(n_rules_single) AS n_rules_single,
         max(n_rules_all) AS n_rules_all,
         count_if(status = 'ok') AS n_ok,
         count_if(status = 'timeout') AS n_timeout
  FROM instacart.ml.experiment_runs
  GROUP BY experiment, platform, algo, grain, scale_pct, top_n, min_support, cores, sweep
)
SELECT m.*, baseline.wall_s / m.wall_s AS speedup,
       baseline.wall_s / m.wall_s / m.cores AS efficiency
FROM metrics m
LEFT JOIN metrics baseline
  ON baseline.cores = 1 AND baseline.experiment = m.experiment
 AND baseline.platform = m.platform AND baseline.algo = m.algo AND baseline.grain = m.grain
 AND baseline.scale_pct = m.scale_pct AND baseline.min_support = m.min_support
 AND baseline.sweep <=> m.sweep
 AND baseline.top_n <=> m.top_n;

CREATE OR REPLACE VIEW instacart.gold.holdout_metrics
COMMENT 'Đánh giá gợi ý trên holdout; tách validation và test.'
AS SELECT * FROM instacart.ml.holdout_metrics;
