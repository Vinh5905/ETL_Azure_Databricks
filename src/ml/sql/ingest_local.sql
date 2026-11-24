-- COPY INTO theo dõi file đã nạp; MERGE chống trùng theo run_key khi chạy lại.
CREATE TABLE IF NOT EXISTS instacart.ml.experiment_runs_local_stage
LIKE instacart.ml.experiment_runs;

COPY INTO instacart.ml.experiment_runs_local_stage
FROM '/Volumes/instacart/ml/inbox/local/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');

MERGE INTO instacart.ml.experiment_runs AS target
USING (
  SELECT * FROM instacart.ml.experiment_runs_local_stage
  QUALIFY row_number() OVER (PARTITION BY run_key ORDER BY finished_at DESC) = 1
) AS source
ON target.run_key = source.run_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
