-- Hai run dùng cùng lô. cross_expected_events và same_expected_events là số sự kiện
-- broker xác nhận ở từng run.
WITH cases AS (
    SELECT 'cross_region' AS scenario, :cross_run_id AS run_id, :cross_region AS expected_eh_region,
           cast(:cross_expected_events AS BIGINT) AS expected_events
    UNION ALL
    SELECT 'same_region', :same_run_id, :dbx_region, cast(:same_expected_events AS BIGINT)
), samples AS (
    SELECT c.scenario, c.run_id, c.expected_eh_region, c.expected_events, l.event_id, l.replay_run_id,
           l.ms_eh_to_bronze, l.ms_end_to_end,
           l.eh_region = c.expected_eh_region AND l.dbx_region = :dbx_region
               AND l.event_id IS NOT NULL AND l.ms_eh_to_bronze >= 0 AS usable
    FROM cases c
    LEFT JOIN IDENTIFIER(:latency_table) l ON l.replay_run_id = c.run_id
), stats AS (
    SELECT scenario, run_id, expected_eh_region, :dbx_region AS expected_dbx_region, expected_events,
           count(replay_run_id) AS observed_events,
           count_if(usable) AS usable_events,
           count(DISTINCT CASE WHEN usable THEN event_id END) AS distinct_events,
           percentile_approx(CASE WHEN usable THEN ms_eh_to_bronze END, 0.50) AS p50_eh_to_bronze_ms,
           percentile_approx(CASE WHEN usable THEN ms_eh_to_bronze END, 0.95) AS p95_eh_to_bronze_ms,
           percentile_approx(CASE WHEN usable AND ms_end_to_end >= 0 THEN ms_end_to_end END, 0.50)
               AS p50_end_to_end_ms
    FROM samples
    GROUP BY scenario, run_id, expected_eh_region, expected_events
)
SELECT *,
       expected_events > 0
           AND :cross_run_id <> :same_run_id AND :cross_region <> :dbx_region
           AND observed_events = expected_events
           AND usable_events = observed_events AND distinct_events = observed_events AS comparable
FROM stats
ORDER BY scenario;
