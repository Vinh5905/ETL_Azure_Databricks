-- Chạy cùng tham số ở hai mốc trước và sau khi đổi eh_generation.
-- bronze_events và silver_orders là tên bảng đầy đủ của cùng một pipeline dev.
WITH inbound AS (
    SELECT _eh_generation, _kafka_partition, _kafka_offset,
           get_json_object(value_json, '$.replay_run_id') AS run_id,
           xxhash64(_eh_generation, _kafka_partition, _kafka_offset,
                    _enqueued_ts, _ingest_ts, event_key, value_json) AS row_hash
    FROM IDENTIFIER(:bronze_events)
), bronze AS (
    SELECT count(*) AS bronze_rows,
           count_if(NOT (_eh_generation <=> :new_generation)) AS previous_bronze_rows,
           sum(CASE WHEN NOT (_eh_generation <=> :new_generation)
                    THEN CAST(row_hash AS DECIMAL(38, 0)) END) AS previous_bronze_checksum,
           count_if(_eh_generation = :new_generation) AS new_generation_rows,
           count_if(_eh_generation = :new_generation AND run_id = :replay_run_id)
               AS new_replay_rows,
           count(DISTINCT CASE WHEN _eh_generation = :new_generation
                               THEN struct(_kafka_partition, _kafka_offset) END)
               AS new_kafka_keys,
           min(CASE WHEN _eh_generation = :new_generation THEN _kafka_offset END)
               AS new_min_offset
    FROM inbound
), silver AS (
    SELECT count(*) AS silver_rows,
           count(DISTINCT order_id) AS silver_order_keys,
           count_if(replay_run_id = :replay_run_id) AS silver_replayed_orders
    FROM IDENTIFIER(:silver_orders)
)
SELECT bronze.*, silver.*
FROM bronze CROSS JOIN silver;
