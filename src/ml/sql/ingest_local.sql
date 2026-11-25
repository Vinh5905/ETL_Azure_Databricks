-- Nạp kết quả E6 chạy ở máy local (ds/03 §7) vào ml.experiment_runs.
-- Bảng thô giữ nguyên từng dòng JSONL, COPY INTO tự bỏ qua file đã nạp. Kiểu cột do from_json bên dưới quyết định:
-- trường bench.py chưa ghi thành NULL, machine (object JSON) giữ nguyên văn dạng chuỗi, measured_at thành finished_at.
CREATE TABLE IF NOT EXISTS instacart.ml.experiment_runs_local_raw (value STRING) USING DELTA;

COPY INTO instacart.ml.experiment_runs_local_raw
FROM '/Volumes/instacart/ml/inbox/local/'
FILEFORMAT = TEXT;

-- MERGE ON run_key: upload lại file cũ, hoặc file mới chứa cả dòng cũ, không sinh dòng trùng.
MERGE INTO instacart.ml.experiment_runs AS target
USING (
  SELECT *
  FROM (
    SELECT
      r.run_key, r.experiment, r.platform, r.algo, r.grain, r.scale_pct, r.top_n, r.min_support,
      r.min_confidence, r.max_k, r.cores, r.rep, r.data_version, r.status, r.error, r.n_tx,
      r.wall_s, r.job_s, r.peak_mem_mb, r.spill_mb, r.n_result, r.machine,
      try_cast(r.measured_at AS TIMESTAMP) AS finished_at,
      row_number() OVER (PARTITION BY r.run_key ORDER BY try_cast(r.measured_at AS TIMESTAMP) DESC) AS rn
    FROM (
      SELECT from_json(
        value,
        'run_key STRING, experiment STRING, platform STRING, algo STRING, grain STRING, scale_pct INT, '
        'top_n INT, min_support DOUBLE, min_confidence DOUBLE, max_k INT, cores INT, rep INT, '
        'data_version STRING, status STRING, error STRING, n_tx BIGINT, wall_s DOUBLE, job_s DOUBLE, '
        'peak_mem_mb DOUBLE, spill_mb DOUBLE, n_result BIGINT, machine STRING, measured_at STRING'
      ) AS r
      FROM instacart.ml.experiment_runs_local_raw
    )
    WHERE r.run_key IS NOT NULL -- dòng trống hoặc hỏng
  )
  WHERE rn = 1
) AS source
ON target.run_key = source.run_key
WHEN MATCHED THEN UPDATE SET
  experiment = source.experiment, platform = source.platform, algo = source.algo, grain = source.grain,
  scale_pct = source.scale_pct, top_n = source.top_n, min_support = source.min_support,
  min_confidence = source.min_confidence, max_k = source.max_k, cores = source.cores, rep = source.rep,
  data_version = source.data_version, status = source.status, error = source.error, n_tx = source.n_tx,
  wall_s = source.wall_s, job_s = source.job_s, peak_mem_mb = source.peak_mem_mb, spill_mb = source.spill_mb,
  n_result = source.n_result, machine = source.machine, finished_at = source.finished_at
WHEN NOT MATCHED THEN INSERT (
  run_key, experiment, platform, algo, grain, scale_pct, top_n, min_support, min_confidence, max_k, cores, rep,
  data_version, status, error, n_tx, wall_s, job_s, peak_mem_mb, spill_mb, n_result, machine, finished_at
) VALUES (
  source.run_key, source.experiment, source.platform, source.algo, source.grain, source.scale_pct, source.top_n,
  source.min_support, source.min_confidence, source.max_k, source.cores, source.rep, source.data_version,
  source.status, source.error, source.n_tx, source.wall_s, source.job_s, source.peak_mem_mb, source.spill_mb,
  source.n_result, source.machine, source.finished_at
);
