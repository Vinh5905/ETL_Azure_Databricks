-- Tra update theo đúng task run do Jobs cấp, kể cả khi repair sinh task run ID mới.
WITH events AS (
  SELECT get_json_object(to_json(named_struct('value', origin)), '$.value') AS source_origin,
         get_json_object(to_json(named_struct('value', details)), '$.value') AS source_details
  FROM IDENTIFIER(
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`.event_log_batch'
  )
  WHERE event_type = 'create_update'
), candidates AS (
  SELECT DISTINCT get_json_object(source_origin, '$.update_id') AS update_id
  FROM events
  WHERE get_json_object(source_origin, '$.pipeline_id') = :pipeline_id
    AND get_json_object(source_details, '$.create_update.update_cause_details.job_details.run_id') = :pipeline_task_run_id
)
SELECT CASE
  WHEN count(*) = 1 AND length(trim(max(update_id))) > 0 THEN max(update_id)
  ELSE CAST(raise_error('expected one pipeline update for the job task run') AS STRING)
END AS update_id
FROM candidates;
