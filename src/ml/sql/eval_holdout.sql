-- evaluate_holdout.py truyền :run_key, :min_conf và :part; k cố định bằng 10.
WITH rules AS (
  SELECT antecedent, CAST(element_at(consequent, 1) AS INT) AS item, confidence, lift
  FROM instacart.ml.association_rules
  WHERE run_key = :run_key AND confidence >= :min_conf AND size(consequent) = 1
),
holdout AS (
  SELECT order_id, context, targets
  FROM instacart.ds_data.holdout_eval
  WHERE data_version = 'v1' AND eval_part = :part AND size(targets) > 0
),
subsets AS (
  SELECT order_id, array(item) AS antecedent FROM holdout LATERAL VIEW explode(context) e AS item
  UNION ALL
  SELECT h.order_id, array(a, b)
  FROM holdout h
  LATERAL VIEW posexplode(context) e1 AS i, a
  LATERAL VIEW posexplode(context) e2 AS j, b
  WHERE i < j
),
rule_candidates AS (
  SELECT s.order_id, r.item, r.confidence, r.lift
  FROM subsets s JOIN rules r ON s.antecedent = r.antecedent
  JOIN holdout h ON h.order_id = s.order_id
  WHERE NOT array_contains(h.context, r.item)
),
best_rule AS (
  SELECT order_id, item, confidence, lift,
         row_number() OVER (PARTITION BY order_id, item ORDER BY confidence DESC, lift DESC) AS item_rank
  FROM rule_candidates
),
ranked_rules AS (
  SELECT order_id, item, confidence, lift,
         row_number() OVER (PARTITION BY order_id ORDER BY confidence DESC, lift DESC, item) AS rec_rank
  FROM best_rule WHERE item_rank = 1
),
rule_top AS (SELECT * FROM ranked_rules WHERE rec_rank <= 10),
popular AS (
  SELECT idx AS item, idx
  FROM instacart.ds_data.item_index
  WHERE data_version = 'v1' AND grain = 'product' AND idx < 100
),
rule_counts AS (SELECT order_id, count(*) AS n_rule FROM rule_top GROUP BY order_id),
rule_fill_candidates AS (
  SELECT h.order_id, p.item,
         row_number() OVER (PARTITION BY h.order_id ORDER BY p.idx) AS pop_rank,
         coalesce(rc.n_rule, 0) AS n_rule
  FROM holdout h CROSS JOIN popular p
  LEFT ANTI JOIN rule_top rt ON rt.order_id = h.order_id AND rt.item = p.item
  LEFT JOIN rule_counts rc ON rc.order_id = h.order_id
  WHERE NOT array_contains(h.context, p.item)
),
rule_fill AS (
  SELECT p.order_id, p.item, CAST(NULL AS DOUBLE) AS confidence, CAST(NULL AS DOUBLE) AS lift,
         p.n_rule + p.pop_rank AS rec_rank
  FROM rule_fill_candidates p
  WHERE p.pop_rank <= 10 AND p.pop_rank <= 10 - p.n_rule
),
baseline_candidates AS (
  SELECT h.order_id, p.item,
         row_number() OVER (PARTITION BY h.order_id ORDER BY p.idx) AS rec_rank
  FROM holdout h CROSS JOIN popular p
  WHERE NOT array_contains(h.context, p.item)
),
baseline_top AS (
  SELECT order_id, item, CAST(NULL AS DOUBLE) AS confidence, CAST(NULL AS DOUBLE) AS lift, rec_rank
  FROM baseline_candidates WHERE rec_rank <= 10
),
recommendations AS (
  SELECT order_id, item, confidence, lift, rec_rank FROM rule_top WHERE :run_key <> 'popularity'
  UNION ALL
  SELECT order_id, item, confidence, lift, rec_rank FROM rule_fill WHERE :run_key <> 'popularity'
  UNION ALL
  SELECT order_id, item, confidence, lift, rec_rank FROM baseline_top WHERE :run_key = 'popularity'
),
recommendations_per_order AS (
  SELECT order_id, collect_list(item) AS recs FROM recommendations GROUP BY order_id
),
rule_only_per_order AS (
  SELECT order_id, collect_list(item) AS rule_recs FROM rule_top GROUP BY order_id
),
has_rule AS (
  SELECT order_id, 1 AS has_rule FROM rule_top GROUP BY order_id
),
per_order AS (
  SELECT h.order_id, h.targets,
         coalesce(r.recs, CAST(array() AS ARRAY<INT>)) AS recs,
         coalesce(ro.rule_recs, CAST(array() AS ARRAY<INT>)) AS rule_recs,
         coalesce(hr.has_rule, 0) AS has_rule
  FROM holdout h
  LEFT JOIN recommendations_per_order r ON r.order_id = h.order_id
  LEFT JOIN rule_only_per_order ro ON ro.order_id = h.order_id
  LEFT JOIN has_rule hr ON hr.order_id = h.order_id
),
catalog AS (
  SELECT count(*) AS n_items FROM instacart.ds_data.item_index
  WHERE data_version = 'v1' AND grain = 'product'
)
SELECT :run_key AS run_key, CAST(:min_conf AS DOUBLE) AS min_confidence,
       :part AS eval_part, 10 AS k, count(*) AS n_eval,
       avg(CASE WHEN size(array_intersect(recs, targets)) > 0 THEN 1 ELSE 0 END) AS hit_rate_at_k,
       avg(size(array_intersect(recs, targets)) / 10) AS precision_at_k,
       avg(size(array_intersect(recs, targets)) / size(targets)) AS recall_at_k,
       avg(has_rule) AS order_coverage,
       size(array_distinct(flatten(collect_list(recs)))) / max(catalog.n_items) AS catalog_coverage,
       avg(CASE WHEN size(array_intersect(rule_recs, targets)) > 0 THEN 1 ELSE 0 END) AS rule_only_hit_rate_at_k,
       avg(size(array_intersect(rule_recs, targets)) / 10) AS rule_only_precision_at_k,
       avg(size(array_intersect(rule_recs, targets)) / size(targets)) AS rule_only_recall_at_k,
       size(array_distinct(flatten(collect_list(rule_recs)))) / max(catalog.n_items) AS rule_only_catalog_coverage,
       current_timestamp() AS evaluated_at
FROM per_order CROSS JOIN catalog;
