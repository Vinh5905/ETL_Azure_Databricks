# bundles/ – quy tắc riêng cho Databricks Asset Bundles

Đọc `docs/devops/05` §4 và `docs/devops/06` §2.

- **Target:**
  - `dev`: mode development, tiền tố `[dev <user>]`, schedule bị tạm dừng.
  - `prod`: chạy bằng SP CI (`run_as` = `${var.ci_sp_id}`). CI đặt biến này qua `BUNDLE_VAR_ci_sp_id`.
  - Chỉ CI được deploy `prod`.
  - PR từ fork không được CI deploy `dev` hay `bundle validate`. Người làm trên fork tự chạy `databricks bundle validate -t dev` và `databricks bundle deploy -t dev` từ máy mình trước khi PR ready (`CONTRIBUTING.md` §3, mục "PR từ fork").
- **Compute:**
  - Job cluster luôn dùng `policy_id: ${var.policy_id}` (tra theo tên policy `instacart-single-node`) và `custom_tags.cost_owner` (không dùng khóa `owner`: workspace đã có Azure tag `owner = p1`, Databricks chép xuống cluster làm tag mặc định nên tag trùng tên bị đổi tên và policy từ chối).
  - **Không** đặt `apply_policy_default_values: true`: policy có `autotermination_minutes`, job cluster sẽ báo "Automated clusters do not support autotermination" (spike 3.4).
  - Vì vậy job cluster tự khai đủ trường `fixed` của policy: `spark_version: 15.4.x-scala2.12`, `node_type_id: ${var.node_type_id}`, `num_workers: 0`, `data_security_mode: SINGLE_USER`, `spark_conf` (`spark.databricks.cluster.profile: singleNode`, `spark.master`), `custom_tags.ResourceClass: SingleNode`. Mẫu ở `docs/de/06` §2.
  - Không ghi cứng tên máy; đổi máy thì sửa biến `node_type_id` trong `databricks.yml` cùng lúc với policy ở `infra/stacks/30-databricks`.
- **Warehouse:** bốn bước SQL của `instacart-batch-medallion` dùng notebook Spark serverless. Khi chỉ thử pipeline thì chạy `databricks bundle run -t dev instacart_batch`. Quy tắc dùng warehouse khi nghiệm thu ở `docs/devops/chi_phi_quota.md` §6.
- **Lịch chạy:** job batch không đặt `schedule`, vì ADF `pl_master` gọi job theo job ID (`40-orchestration`). Vì vậy không đổi `name` của job prod khi chưa sửa stack 40.
- **Đường dẫn:** code nằm ở `../src` (đã khai báo trong `sync.paths`), nên đường dẫn trong `resources/*.yml` tính tương đối từ file yml (`../../src/...`).
- **Trạng thái G0:** AD-DE-01 đã bỏ job Python tạm. `resources/jobs_batch.yml` có `jobs: {}`,
  chưa tạo job batch trên workspace. Không deploy khung này lên môi trường đã có job prod.
- **Việc còn treo:** `docs/de` dùng Lakeflow Declarative Pipelines (`instacart_batch`, `instacart_stream`).
  - AD-DE-08 có `resources/pipeline_batch.yml`, hướng dẫn tạo schema và nghiệm thu ở `README.md`.
    Glob chỉ nạp `src/pipelines/batch/`; `bootstrap.py` là smoke private, không thay thế ETL nghiệp vụ.
    Không thêm schema dev thành resource DAB để cleanup không xóa schema dùng chung của người chạy.
  - AD-ST-03 khai báo pipeline stream.
  - AD-DE-18 bổ sung job `instacart-batch-medallion` dùng `pipeline_task` và các task vận hành.

## Job seed (AD-DE-05)

- `resources/jobs_ingest.yml` tạo resource `seed_retail_db`, chạy tay và không có schedule.
  Dùng classic single-node theo yêu cầu task/runtime 15.4/policy của repo, không tự đổi compute.
- Một run, một wave; không chạy đồng thời với ADF hoặc các job dùng chung quota 4 vCPU.
  Không tự retry lỗi seed; sau khi sửa cấu hình có thể chạy tay lại an toàn.
  Thư viện `pymssql==2.4.6` khớp lockfile, mật khẩu đọc từ scope `kv` trong code.
- `server` mặc định rỗng, người chạy truyền bằng job parameter; không ghi endpoint riêng vào YAML.
  Dev/prod chỉ tách tên job, không tách database nguồn; kiểm `server`/`database` trước khi chạy.
- `products_csv` phải là danh mục thật đủ 49.688 sản phẩm; xem `src/ingest/README.md`.
  Đường dẫn mặc định cần landing CSV từ AD-DE-06. Kiểm quyền đọc CSV và scope `kv` trước khi chạy job.
- Job seed khai đủ fixed fields như phần Compute, không dùng `apply_policy_default_values` hay auto-termination.
- Server chính thức đã có SQL login `sqladmin` và secret `sql-admin-password` theo #14 và #24.
  Nghiệm thu job cần kiểm kết nối bằng SQL login từ cluster dev.
