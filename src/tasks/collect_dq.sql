-- Chạy cả file cùng phiên SQL, tham số STRING: catalog, ops_schema, event_log_table, update_id.
-- Chạy riêng batch/stream bằng cùng file, chỉ chọn log đã có. Không dùng lookback cắt giữa update.
SELECT assert_true(
  length(trim(:catalog)) > 0 AND length(trim(:ops_schema)) > 0
  AND :event_log_table IN ('event_log_batch', 'event_log_stream') AND length(trim(:update_id)) > 0,
  'collect_dq requires catalog, ops_schema, event_log_table and update_id'
);

USE IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`');

CREATE TABLE IF NOT EXISTS dq_results (
  update_id STRING NOT NULL, pipeline STRING NOT NULL, pipeline_name STRING, flow STRING NOT NULL,
  dataset STRING NOT NULL, expectation STRING NOT NULL,
  passed BIGINT NOT NULL, failed BIGINT NOT NULL, at TIMESTAMP_LTZ NOT NULL
) USING DELTA;

MERGE INTO dq_results AS t
USING (
  WITH parsed AS (
    SELECT id, timestamp,
      from_json(
        get_json_object(to_json(named_struct('value', origin)), '$.value'),
        'STRUCT<pipeline_id:STRING,pipeline_name:STRING,update_id:STRING,flow_name:STRING,dataset_name:STRING>',
        map('mode', 'FAILFAST')
      ) AS o,
      from_json(
        get_json_object(to_json(named_struct('value', details)), '$.value'),
        'STRUCT<flow_progress:STRUCT<data_quality:STRUCT<expectations:ARRAY<STRUCT<name:STRING,dataset:STRING,passed_records:BIGINT,failed_records:BIGINT>>>>>',
        map('mode', 'FAILFAST')
      ).flow_progress.data_quality.expectations AS expectations
    FROM IDENTIFIER(
      '`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``')
      || '`.`' || replace(:event_log_table, '`', '``') || '`'
    )
    WHERE event_type = 'flow_progress' AND coalesce(maturity_level, 'STABLE') = 'STABLE'
  ), unique_events AS (
    SELECT DISTINCT id, timestamp, o, expectations FROM parsed WHERE o.update_id = :update_id
  ), checked_events AS (
    SELECT *, CASE WHEN id IS NOT NULL AND length(trim(id)) > 0
      AND count(*) OVER (PARTITION BY id) = 1 THEN id
      ELSE raise_error('collect_dq: missing or conflicting event id') END AS event_id
    FROM unique_events
  ), expanded AS (
    SELECT event_id, timestamp, o.update_id AS update_id,
      coalesce(nullif(trim(o.pipeline_id), ''), nullif(trim(o.pipeline_name), '')) AS pipeline,
      nullif(trim(o.pipeline_name), '') AS pipeline_name,
      nullif(trim(o.flow_name), '') AS flow,
      coalesce(nullif(trim(e.dataset), ''), nullif(trim(o.dataset_name), '')) AS dataset,
      nullif(trim(e.name), '') AS expectation, e.passed_records AS passed, e.failed_records AS failed
    FROM checked_events LATERAL VIEW explode(expectations) AS e
  ), per_event AS (
    SELECT event_id, update_id,
      max(pipeline_name) AS pipeline_name,
      CASE WHEN pipeline IS NOT NULL THEN pipeline ELSE raise_error('collect_dq: missing pipeline') END AS pipeline,
      CASE WHEN flow IS NOT NULL THEN flow ELSE raise_error('collect_dq: missing flow') END AS flow,
      CASE WHEN dataset IS NOT NULL THEN dataset ELSE raise_error('collect_dq: missing dataset') END AS dataset,
      CASE WHEN expectation IS NOT NULL THEN expectation ELSE raise_error('collect_dq: missing expectation') END AS expectation,
      CASE WHEN count(*) = 1 AND min(passed) >= 0 AND count(passed) = 1 THEN min(passed)
        ELSE raise_error('collect_dq: invalid or duplicate expectation counters') END AS passed,
      CASE WHEN count(*) = 1 AND min(failed) >= 0 AND count(failed) = 1 THEN min(failed)
        ELSE raise_error('collect_dq: invalid or duplicate expectation counters') END AS failed,
      CASE WHEN count(timestamp) = 1 THEN max(timestamp)
        ELSE raise_error('collect_dq: missing event timestamp') END AS at
    FROM expanded GROUP BY event_id, update_id, pipeline, flow, dataset, expectation
  )
  SELECT update_id, pipeline, flow, dataset, expectation, sum(passed) AS passed,
         sum(failed) AS failed, max(at) AS at,
         max_by(pipeline_name, named_struct('named', pipeline_name IS NOT NULL, 'at', at, 'id', event_id))
           AS pipeline_name
  FROM per_event GROUP BY update_id, pipeline, flow, dataset, expectation
) AS s
ON t.pipeline = s.pipeline AND t.update_id = s.update_id AND t.flow = s.flow
   AND t.dataset = s.dataset AND t.expectation = s.expectation
-- Snapshot cũ bỏ qua. Cùng/mới hơn nhưng mất tổng phải báo lỗi, không ghi đè hay giả vờ đã thu đủ.
WHEN MATCHED AND s.at >= t.at AND (s.passed < t.passed OR s.failed < t.failed) THEN
  UPDATE SET passed = CAST(raise_error('collect_dq: source totals regressed, check event log retention') AS BIGINT)
WHEN MATCHED AND s.at >= t.at AND (s.at > t.at OR s.passed <> t.passed OR s.failed <> t.failed
    OR (s.pipeline_name IS NOT NULL AND NOT (s.pipeline_name <=> t.pipeline_name))) THEN
  UPDATE SET passed = s.passed, failed = s.failed, at = s.at,
             pipeline_name = coalesce(s.pipeline_name, t.pipeline_name)
WHEN NOT MATCHED THEN INSERT (update_id, pipeline, pipeline_name, flow, dataset, expectation, passed, failed, at)
  VALUES (s.update_id, s.pipeline, s.pipeline_name, s.flow, s.dataset, s.expectation, s.passed, s.failed, s.at);

SELECT pipeline, update_id, count(*) AS expectation_rows, sum(passed) AS passed, sum(failed) AS failed
FROM dq_results WHERE update_id = :update_id GROUP BY pipeline, update_id;
