# Phân việc · DevOps / nền tảng (`AD-OPS-xx`)

34 task. Quy ước, trạng thái và bảng tổng ở [README](README.md). Vai ghi theo P1–P4, người thật được gán trên GitHub Project.

| Mã | Việc | Vai | Cổng | Phụ thuộc | Issue |
|---|---|---|---|---|---|
| [AD-OPS-01](#ad-ops-01) | Cài bộ công cụ cho cả 4 máy và ghi bảng công cụ vào spike_results | P1 | G0 | Chuẩn bị ngay | [#3](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/3) |
| [AD-OPS-02](#ad-ops-02) | Thiết lập quản trị repo: board, label, milestone, CODEOWNERS, branch protection, environment | P1 | G0 | Chuẩn bị ngay | [#4](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/4) |
| [AD-OPS-03](#ad-ops-03) | Spike region được phép, quota vCPU và quyền trong tenant trường | P1 | G0 | Chuẩn bị ngay | [#5](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/5) |
| [AD-OPS-04](#ad-ops-04) | Spike Databricks nền: workspace, Unity Catalog, serverless warehouse, cluster single-node | P1 | G0 | Bắt đầu: AD-OPS-03 | [#6](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/6) |
| [AD-OPS-05](#ad-ops-05) | Spike Databricks nâng cao: Lakeflow single-node, Kafka, policy local[n], for_each_task, Model Serving | P1 | G0 | Bắt đầu: AD-OPS-04; Hoàn tất: AD-OPS-06 | [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7) |
| [AD-OPS-06](#ad-ops-06) | Spike Azure SQL free offer, CDC, Event Hubs Kafka, ADF và Azure Functions | P1 | G0 | Bắt đầu: AD-OPS-03 | [#8](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/8) |
| [AD-OPS-07](#ad-ops-07) | Chốt quyết định G0, dọn tài nguyên spike và mở cổng G0 | P1 | G0 | Bắt đầu: AD-OPS-05, AD-OPS-06; Hoàn tất: AD-BI-01 | [#9](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/9) |
| [AD-OPS-08](#ad-ops-08) | Apply bootstrap, chuyển state lên blob và cấu hình OIDC cho GitHub Actions | P1 | G1 | Bắt đầu: AD-OPS-07 | [#10](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/10) |
| [AD-OPS-09](#ad-ops-09) | Dựng ADLS Gen2 với 7 container và lifecycle landing sang cool (10-core bước 3.2) | P1 | G1 | Bắt đầu: AD-OPS-08 | [#11](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/11) |
| [AD-OPS-10](#ad-ops-10) | Dựng Key Vault, Access Connector và danh tính id-func-recommend (10-core bước 3.3–3.4) | P1 | G1 | Bắt đầu: AD-OPS-09 | [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12) |
| [AD-OPS-11](#ad-ops-11) | Dựng Databricks workspace Premium (10-core bước 3.5) | P1 | G1 | Bắt đầu: AD-OPS-08 | [#13](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/13) |
| [AD-OPS-12](#ad-ops-12) | Dựng Azure SQL retail_db free offer, firewall và secret mật khẩu (10-core bước 3.6) | P1 | G1 | Bắt đầu: AD-OPS-10 | [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14) |
| [AD-OPS-13](#ad-ops-13) | Dựng Data Factory với managed identity và quyền (10-core bước 3.7) | P1 | G1 | Bắt đầu: AD-OPS-10, AD-OPS-11 | [#15](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/15) |
| [AD-OPS-14](#ad-ops-14) | Cấp RBAC cho 4 thành viên và đặt budget cảnh báo chi phí (10-core bước 3.8–3.9) | P1 | G1 | Bắt đầu: AD-OPS-10 | [#16](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/16) |
| [AD-OPS-15](#ad-ops-15) | Nghiệm thu stack 10-core và hoàn thiện output bước 3.10 | P1 | G1 | Bắt đầu: AD-OPS-12, AD-OPS-13, AD-OPS-14 | [#17](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/17) |
| [AD-OPS-16](#ad-ops-16) | Bổ sung job java, Infracost và chặn trivy trong pr-checks | P1 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-15 | [#18](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/18) |
| [AD-OPS-17](#ad-ops-17) | Chạy thử trọn chuỗi CI hạ tầng bằng PR đổi tag ở 10-core | P1 | G1 | Bắt đầu: AD-OPS-02, AD-OPS-15 | [#19](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/19) |
| [AD-OPS-18](#ad-ops-18) | Thêm service principal CI, SP Function và user theo vai trò vào workspace (30-databricks bước 2) | P1 | G1 | Bắt đầu: AD-OPS-10, AD-OPS-11 | [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20) |
| [AD-OPS-19](#ad-ops-19) | Tạo storage credential, external location, catalog instacart, 8 schema và volume (30-databricks bước 3–4) | P1 | G1 | Bắt đầu: AD-OPS-10, AD-OPS-11 | [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21) |
| [AD-OPS-20](#ad-ops-20) | Cấp quyền Unity Catalog theo vai trò (30-databricks bước 5) | P1 | G1 | Bắt đầu: AD-OPS-18, AD-OPS-19 | [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22) |
| [AD-OPS-21](#ad-ops-21) | Tạo cluster policy single-node, pipeline và ds-benchmark, chặn tạo cluster tự do (30-databricks bước 6) | P1 | G1 | Bắt đầu: AD-OPS-18 | [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23) |
| [AD-OPS-22](#ad-ops-22) | Tạo secret scope Key Vault, serverless SQL warehouse và output cho DAB, Power BI (30-databricks bước 7–9) | P1 | G1 | Bắt đầu: AD-OPS-18; Hoàn tất: AD-OPS-12 | [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24) |
| [AD-OPS-23](#ad-ops-23) | Nghiệm thu G1: chạy 30-databricks qua CI và thử de-deploy dev/prod | P1 | G1 | Bắt đầu: AD-OPS-17, AD-OPS-20, AD-OPS-21, AD-OPS-22; Hoàn tất: AD-DE-18 | [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25) |
| [AD-OPS-24](#ad-ops-24) | Dựng stack 20-stream (Event Hubs, SAS, secret) và nghiệm thu stream-toggle | P3 | G2 | Bắt đầu: AD-OPS-02, AD-OPS-10 | [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26) |
| [AD-OPS-25](#ad-ops-25) | Khai báo linked service và dataset ADF trong stack 40-orchestration | P1 | G3 | Bắt đầu: AD-OPS-12, AD-OPS-13; Hoàn tất: AD-OPS-17 | [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27) |
| [AD-OPS-26](#ad-ops-26) | Đưa pipeline và trigger ADF vào 40-orchestration, bật lịch hằng ngày khi pl_master đạt | P1 | G3 | Bắt đầu: AD-OPS-23, AD-OPS-25; Hoàn tất: AD-DE-06, AD-DE-07, AD-DE-19, AD-DE-18 | [#28](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/28) |
| [AD-OPS-27](#ad-ops-27) | Kiểm thử drift-check bằng một thay đổi tay có chủ đích | P1 | G3 | Bắt đầu: AD-OPS-23, AD-OPS-25 | [#29](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/29) |
| [AD-OPS-28](#ad-ops-28) | Lập bảng theo dõi chi phí theo tag và quota vCPU hằng tuần | P1 | G4 | Bắt đầu: AD-OPS-14 | [#30](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/30) |
| [AD-OPS-29](#ad-ops-29) | Dựng workspace sandbox 90-sandbox cho DS ở Sub C | P4 | G3 | Bắt đầu: AD-OPS-08; Hoàn tất: AD-DE-21 | [#31](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/31) |
| [AD-OPS-30](#ad-ops-30) | Mở rộng khung bundle và quyền cho các job DS | P1 | G3 | Bắt đầu: AD-OPS-20, AD-OPS-21; Hoàn tất: AD-OPS-29 | [#32](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/32) |
| [AD-OPS-31](#ad-ops-31) | Dựng stack 50-serving: Function App Flex Consumption cho API gợi ý | P1 | G5 | Bắt đầu: AD-OPS-23; Hoàn tất: AD-DS-23 | [#33](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/33) |
| [AD-OPS-32](#ad-ops-32) | Viết workflow func-deploy đưa code Function lên Azure | P1 | G5 | Bắt đầu: AD-OPS-31; Hoàn tất: AD-DS-24 | [#34](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/34) |
| [AD-OPS-33](#ad-ops-33) | Tổng hợp bằng chứng hạ tầng cho rubric 1.4, 3.2, 3.4 và 5.4 | P1 | G4 | Bắt đầu: AD-OPS-24, AD-OPS-27, AD-OPS-28; Hoàn tất: AD-OPS-26 | [#35](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/35) |
| [AD-OPS-34](#ad-ops-34) | Hủy hạ tầng cuối kỳ theo thứ tự ngược và chốt báo cáo chi phí | P1 | G5 | Bắt đầu: AD-OPS-33; Hoàn tất: AD-RP-09, AD-RP-10 | [#36](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/36) |

### AD-OPS-01

**Cài bộ công cụ cho cả 4 máy và ghi bảng công cụ vào spike_results**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#3](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/3)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §1`, `docs/devops/spike_results.md §1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** Không có.

Cả nhóm cần cùng phiên bản az, terraform, databricks CLI, uv, Docker để code chạy giống nhau và CI không báo đỏ. Đây là bước đầu của giai đoạn P0.

**Việc cần làm**

1. Gửi danh sách công cụ ở 01 §1 cho P2–P4 (az, terraform ~1.9, tflint, trivy, databricks >= 0.230, gh, pre-commit, uv + Python 3.11, Docker, AzCopy, Infracost)
2. Mỗi người chạy lệnh kiểm phiên bản ở 01 §1 và gửi kết quả
3. Mỗi người chạy pre-commit install trong repo
4. P1 điền bảng mục 1 của spike_results.md, không ghi ID hay email

**Đầu ra**

- docs/devops/spike_results.md mục 1 điền đủ 4 dòng P1–P4
- PR docs(docs) cập nhật spike_results

**Xong khi**

- [ ] Bảng mục 1 không còn ô trống
- [ ] `az version && terraform version && tflint --version && databricks --version && gh --version && uv --version` chạy được trên cả 4 máy
- [ ] `pre-commit run --all-files` chạy được ở ít nhất máy P1

### AD-OPS-02

**Thiết lập quản trị repo: board, label, milestone, CODEOWNERS, branch protection, environment**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#4](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/4)
- Tài liệu: `docs/devops/04_github_actions_cicd.md §2`, `docs/devops/04_github_actions_cicd.md §3.3`, `docs/devops/04_github_actions_cicd.md §3.5`, `docs/devops/02_bootstrap_tfstate.md §3 bước 7`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-17](01_devops.md#ad-ops-17) (bắt đầu), [AD-OPS-24](01_devops.md#ad-ops-24) (bắt đầu)

Mọi việc sau đi qua PR và issue; board, review và lịch sử duyệt là bằng chứng rubric 5.4. Environment prod và stream cần có trước khi tf-apply, de-deploy, stream-toggle chạy được.

**Việc cần làm**

1. Tạo GitHub Project (Todo / Doing / Review / Done), label infra, de-batch, de-stream, bi, ds, drift
2. Tạo milestone G0…G5
3. Thay P1_HANDLE…P4_HANDLE trong .github/CODEOWNERS bằng username thật, thêm dòng cho /src/mapreduce/, /local/hadoop/, /src/serving_func/ theo docs/devops/00 §4
4. Bật branch protection cho main: bắt buộc PR, 1 approve, review của code owner, up-to-date, cấm force-push và xóa nhánh
5. Đặt status check bắt buộc: pr-checks / terraform, python, bundle; pr-lint / title, commits, body
6. Tạo environment dev, prod (1 required reviewer), stream (không reviewer, chỉ nhánh main)

**Đầu ra**

- GitHub Project board, label, milestone G0–G5
- .github/CODEOWNERS có username thật (PR chore(ci))
- Branch protection rule cho main
- Environment dev, prod, stream trên GitHub

**Xong khi**

- [ ] Push thẳng lên main bị từ chối
- [ ] PR đụng infra/** tự yêu cầu review của P1
- [ ] `gh api repos/{owner}/{repo}/environments` liệt kê dev, prod, stream; prod có required reviewer
- [ ] Board có cột và milestone đủ G0–G5

### AD-OPS-03

**Spike region được phép, quota vCPU và quyền trong tenant trường**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#5](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/5)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §2`, `docs/devops/01_chuan_bi_va_spike.md §3.1–3.3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-04](01_devops.md#ad-ops-04) (bắt đầu), [AD-OPS-06](01_devops.md#ad-ops-06) (bắt đầu)

Tài khoản Students giới hạn region, quota và quyền tenant mà tài liệu Microsoft không công bố đủ. Phải biết trước khi viết Terraform, nếu không sẽ phải sửa kiến trúc.

**Việc cần làm**

1. Thu thập tenant ID, subscription Sub A–D, object ID 4 thành viên vào *.auto.tfvars cục bộ (không commit)
2. Chạy lệnh policy region ở 01 §3.1 trên cả 4 subscription, lấy giao với region có Databricks, Event Hubs, SQL free offer
3. Chạy `az vm list-usage` cho region ứng viên của Sub A, ghi tổng vCPU và vCPU theo họ máy
4. Thử tạo UAMI, federated credential, App Registration, nhóm Entra trong rg-spike
5. Nhờ P3 thử gán Contributor Sub B cho UAMI thử
6. Ghi kết quả các dòng 3.1–3.3 vào spike_results.md

**Đầu ra**

- rg-spike với UAMI id-spike (tạm)
- docs/devops/spike_results.md dòng 3.1, 3.2, 3.3 có kết quả

**Xong khi**

- [ ] Mỗi dòng 3.1–3.3 ghi Đạt hoặc Không đạt kèm phương án dự phòng
- [ ] Đã có ứng viên region Sub A và một region khác cho Sub B
- [ ] Không có subscription ID, object ID, email trong file commit

### AD-OPS-04

**Spike Databricks nền: workspace, Unity Catalog, serverless warehouse, cluster single-node**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#6](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/6)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §3.4`, `docs/devops/05_databricks_platform.md §2`
- **Bắt đầu sau:** [AD-OPS-03](01_devops.md#ad-ops-03) [#5](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/5)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-05](01_devops.md#ad-ops-05) (bắt đầu), [AD-BI-01](04_power_bi.md#ad-bi-01) (hoàn tất)

Quyết định external location hay managed storage, nhóm account hay grant theo user, serving cho Power BI đều phụ thuộc phép thử này. Kết quả quyết định nhánh của stack 30-databricks.

**Việc cần làm**

1. Tạo tay một workspace Premium ở region đã chọn (rg-spike)
2. Kiểm UC tự gán metastore, ai là account admin / metastore admin
3. Thử tạo storage credential từ Access Connector
4. Tạo serverless SQL warehouse, chạy SELECT 1
5. Chạy notebook serverless; tạo cluster single-node DS3_v2 DBR 15.4 LTS, đo thời gian khởi động
6. Ghi ngày hết trial Premium; kiểm workspace có automatic identity management không
7. Ghi kết quả vào spike_results.md

**Đầu ra**

- docs/devops/spike_results.md các dòng 3.4 phần nền (workspace, metastore, admin, storage credential, serverless warehouse, serverless jobs, cluster, trial)

**Xong khi**

- [ ] Các dòng nêu trên có kết quả Đạt / Không đạt kèm dự phòng
- [ ] Đã biết chọn phương án nhóm nào ở 05 bước 2 (account group, nhóm Entra, hay grant theo user)

### AD-OPS-05

**Spike Databricks nâng cao: Lakeflow single-node, Kafka, policy local[n], for_each_task, Model Serving**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §3.4`, `docs/ds/05_mlflow_serving_bao_cao.md §3`
- **Bắt đầu sau:** [AD-OPS-04](01_devops.md#ad-ops-04) [#6](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/6)
- **Hoàn tất còn cần:** [AD-OPS-06](01_devops.md#ad-ops-06) [#8](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/8)
- **Mở khóa:** [AD-OPS-07](01_devops.md#ad-ops-07) (bắt đầu), [AD-DE-08](02_de_batch.md#ad-de-08) (hoàn tất), [AD-ST-08](03_de_stream.md#ad-st-08) (hoàn tất), [AD-ST-10](03_de_stream.md#ad-st-10) (hoàn tất)

Pipeline DE (LDP Advanced classic), streaming qua Kafka, thực nghiệm speedup DS và API model đều dựa vào các tính năng này. Không đạt thì phải chọn dự phòng trước khi DE/DS viết code.

**Việc cần làm**

1. Tạo pipeline LDP Advanced num_workers 0 với 1 bảng Auto Loader, 1 AUTO CDC, 1 expectation; thử thêm serverless pipeline
2. Thử append_flow đọc Kafka từ Event Hubs thử, đổi tên flow và chạy lại
3. Thử ForEachBatch sink POST tới URL thử
4. Tạo policy thử allowlist spark.master local[1]/local[2], kiểm sc.defaultParallelism
5. Tạo job 2 task dùng for_each_task đọc task value, concurrency 1
6. Đăng ký model pyfunc nhỏ vào UC, tạo endpoint scale_to_zero, gọi invocations
7. Ghi kết quả vào spike_results.md

**Đầu ra**

- docs/devops/spike_results.md các dòng 3.4: LDP, serverless pipeline, Kafka + đổi flow, ForEachBatch, policy DS, for_each_task, Model Serving

**Xong khi**

- [ ] Mỗi dòng có Đạt / Không đạt kèm dự phòng đã chọn
- [ ] Đã chốt edition pipeline DE và phương án API model (Model Serving + Function hay Function tự tra luật)

### AD-OPS-06

**Spike Azure SQL free offer, CDC, Event Hubs Kafka, ADF và Azure Functions**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#8](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/8)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §3.5`
- **Bắt đầu sau:** [AD-OPS-03](01_devops.md#ad-ops-03) [#5](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/5)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-05](01_devops.md#ad-ops-05) (hoàn tất), [AD-OPS-07](01_devops.md#ad-ops-07) (bắt đầu), [AD-DE-04](02_de_batch.md#ad-de-04) (hoàn tất)

Nguồn SQL CDC, Kafka endpoint ở Sub B, ADF gọi Databricks Job và Function Flex Consumption đều có thể bị chặn trên Students. Kết quả quyết định nguồn SQL và gói Function.

**Việc cần làm**

1. Tạo SQL DB free offer, bật CDC (sp_cdc_enable_db, sp_cdc_enable_table), sửa 1 dòng, đọc fn_cdc_get_all_changes
2. Nhờ P3 tạo Event Hubs Standard ở Sub B, gửi 1 message qua kcat vào :9093
3. Kiểm ADF có activity Databricks Job; thử Copy HTTP Zenodo ZIP → ADLS với ZipDeflate
4. Thử tạo Function App Flex Consumption Python 3.11 ở region Sub A
5. Ghi kết quả 3.5 vào spike_results.md

**Đầu ra**

- docs/devops/spike_results.md các dòng 3.5

**Xong khi**

- [ ] Mỗi dòng 3.5 có Đạt / Không đạt kèm dự phòng
- [ ] Đã biết nguồn SQL dùng CDC theo LSN hay watermark updated_at

### AD-OPS-07

**Chốt quyết định G0, dọn tài nguyên spike và mở cổng G0**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G0**
- Issue: [#9](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/9)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §4`, `docs/devops/spike_results.md §3`, `docs/devops/00_tong_quan.md §3`
- **Bắt đầu sau:** [AD-OPS-05](01_devops.md#ad-ops-05) [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7), [AD-OPS-06](01_devops.md#ad-ops-06) [#8](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/8)
- **Hoàn tất còn cần:** [AD-BI-01](04_power_bi.md#ad-bi-01) [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74)
- **Mở khóa:** [AD-OPS-08](01_devops.md#ad-ops-08) (bắt đầu), [AD-RP-02](06_bao_cao_demo.md#ad-rp-02) (hoàn tất)

Cổng G0 mở khóa DE viết code local và DS đọc thuật toán. Terraform phải dựng lại từ đầu nên tài nguyên tạo tay khi spike phải xóa.

**Việc cần làm**

1. Điền bảng Quyết định G0 (region Sub A/B/C, auth CI, UC, serving Power BI, compute, pipeline DE, nguồn SQL, nơi chạy DS, API model)
2. Cập nhật infra/envs/*.tfvars với region đã chốt (không có ID)
3. Ghi lựa chọn sandbox cùng hay khác region với Sub A
4. P1 tự xóa rg-spike và workspace thử trên Portal (agent không chạy lệnh xóa)
5. Đánh dấu ô 'Đã xóa rg-spike' và đóng milestone G0

**Đầu ra**

- docs/devops/spike_results.md mục 2 và 3 đủ, ô xóa rg-spike được đánh dấu
- infra/envs/10-core.tfvars, 20-stream.tfvars, 90-sandbox-p4.tfvars có region đã chốt
- Milestone G0 đóng

**Xong khi**

- [ ] Bảng Quyết định G0 không còn ô trống
- [ ] `az group list --query "[?name=='rg-spike']"` trả về rỗng
- [ ] PR cập nhật spike_results và envs đã merge

### AD-OPS-08

**Apply bootstrap, chuyển state lên blob và cấu hình OIDC cho GitHub Actions**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#10](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/10)
- Tài liệu: `docs/devops/02_bootstrap_tfstate.md §3`, `docs/devops/02_bootstrap_tfstate.md §6`, `infra/AGENTS.md`
- **Bắt đầu sau:** [AD-OPS-07](01_devops.md#ad-ops-07) [#9](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/9)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-09](01_devops.md#ad-ops-09) (bắt đầu), [AD-OPS-11](01_devops.md#ad-ops-11) (bắt đầu), [AD-OPS-29](01_devops.md#ad-ops-29) (bắt đầu)

Mọi stack cần state chung có khóa và CI cần đăng nhập Azure không secret. Đây là ngoại lệ duy nhất P1 apply từ máy.

**Việc cần làm**

1. Rà infra/bootstrap theo 02 §3 (storage tắt shared key, versioning, lock, UAMI id-github-ci, 5 federated credential main/pr/prod/dev/stream, rbac.tf có điều kiện)
2. P1 apply bootstrap bằng local state, ghi output tfstate_account và ci_client_id
3. Thêm backend azurerm, migrate state lên key bootstrap.tfstate, xóa state local
4. Điền storage_account_name thật vào infra/backend.hcl
5. Nhờ P3 gán Contributor Sub B cho principal của id-github-ci bằng lệnh ở 02 §3 bước 5
6. Đặt GitHub Variables AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_A, AZURE_SUBSCRIPTION_B, TFSTATE_ACCOUNT, TF_VAR_member_object_ids
7. Chạy workflow whoami, một thành viên khác chạy smoke init, rồi xóa .github/workflows/whoami.yml

**Đầu ra**

- rg-tfstate, sttfstate<sfx>, container tfstate, lock-tfstate, UAMI id-github-ci trên Sub A
- Role Contributor của id-github-ci trên Sub B
- infra/backend.hcl có tên storage thật
- GitHub Variables đủ theo 04 §4
- PR xóa whoami.yml

**Xong khi**

- [ ] Blob bootstrap.tfstate có trong container tfstate
- [ ] Workflow whoami in đúng Sub A
- [ ] Thành viên khác P1 chạy `terraform init -backend-config=../backend.hcl -backend-config="key=smoke.tfstate"` thành công
- [ ] Portal báo shared key của sttfstate bị tắt
- [ ] `az role assignment list --assignee <uami> --scope /subscriptions/<subB>` có Contributor

### AD-OPS-09

**Dựng ADLS Gen2 với 7 container và lifecycle landing sang cool (10-core bước 3.2)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#11](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/11)
- Tài liệu: `docs/devops/03_terraform_azure.md §2`, `docs/devops/03_terraform_azure.md §3 bước 3.1–3.2`
- **Bắt đầu sau:** [AD-OPS-08](01_devops.md#ad-ops-08) [#10](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/10)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-10](01_devops.md#ad-ops-10) (bắt đầu)

ADLS là nơi lưu mọi tầng dữ liệu và container ds cho phần DS. Lifecycle là bằng chứng rubric 3.4.

**Việc cần làm**

1. Apply bước 3.1 (RG rg-instacart-core) lần đầu từ máy P1
2. Viết module lakehouse_storage (wrapper AVM storage, pin version, enable_telemetry = false) hoặc gọi AVM trực tiếp
3. Tạo 7 container landing, bronze, silver, gold, checkpoints, mlflow, ds; HNS bật, shared key tắt
4. Thêm azurerm_storage_management_policy landing-to-cool sau 30 ngày
5. Sửa checklist main.tf từ '6 container' thành 7 và đánh dấu [x] 3.2

**Đầu ra**

- infra/modules/lakehouse_storage/*.tf
- infra/stacks/10-core: module lake, storage management policy
- Storage account ADLS trên Sub A

**Xong khi**

- [ ] `az storage fs list --account-name <st> --auth-mode login` thấy đủ 7 container
- [ ] `terraform plan` lần hai báo No changes
- [ ] Lifecycle rule landing-to-cool hiển thị trên Portal

### AD-OPS-10

**Dựng Key Vault, Access Connector và danh tính id-func-recommend (10-core bước 3.3–3.4)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12)
- Tài liệu: `docs/devops/03_terraform_azure.md §3 bước 3.3–3.4`
- **Bắt đầu sau:** [AD-OPS-09](01_devops.md#ad-ops-09) [#11](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/11)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-12](01_devops.md#ad-ops-12) (bắt đầu), [AD-OPS-13](01_devops.md#ad-ops-13) (bắt đầu), [AD-OPS-14](01_devops.md#ad-ops-14) (bắt đầu), [AD-OPS-18](01_devops.md#ad-ops-18) (bắt đầu), [AD-OPS-19](01_devops.md#ad-ops-19) (bắt đầu), [AD-OPS-24](01_devops.md#ad-ops-24) (bắt đầu)

Key Vault giữ mọi bí mật; Access Connector là danh tính để Unity Catalog đọc ADLS. id-func-recommend tạo sẵn để 30-databricks thêm vào workspace trước khi có 50-serving.

**Việc cần làm**

1. Module AVM keyvault RBAC mode, purge_protection false, soft delete 7 ngày (PR 1)
2. azurerm_databricks_access_connector dbac-instacart + Storage Blob Data Contributor trên lake (PR 2)
3. azurerm_user_assigned_identity id-func-recommend
4. Thêm output key_vault_id, key_vault_uri, access_connector_id, func_identity_id, func_identity_client_id
5. Đánh dấu [x] 3.3, 3.4

**Đầu ra**

- Key Vault, dbac-instacart, id-func-recommend trên rg-instacart-core
- Output mới trong infra/stacks/10-core/outputs.tf

**Xong khi**

- [ ] `az keyvault show` báo enableRbacAuthorization = true
- [ ] `az role assignment list --assignee <principal dbac>` có Storage Blob Data Contributor trên lake
- [ ] `terraform output func_identity_client_id` có giá trị
- [ ] Plan lần hai No changes

### AD-OPS-11

**Dựng Databricks workspace Premium (10-core bước 3.5)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#13](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/13)
- Tài liệu: `docs/devops/03_terraform_azure.md §3 bước 3.5`
- **Bắt đầu sau:** [AD-OPS-08](01_devops.md#ad-ops-08) [#10](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/10)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-13](01_devops.md#ad-ops-13) (bắt đầu), [AD-OPS-18](01_devops.md#ad-ops-18) (bắt đầu), [AD-OPS-19](01_devops.md#ad-ops-19) (bắt đầu)

Unity Catalog, SQL warehouse và secret scope cần Premium. Provider databricks của 30-databricks cần URL workspace từ output này.

**Việc cần làm**

1. Module AVM databricks workspace dbw-instacart, sku premium, managed RG rg-instacart-dbw-managed, không VNet injection
2. Đặt custom_parameters khớp kết quả spike
3. Thêm output databricks_workspace_url, databricks_workspace_id
4. Đánh dấu [x] 3.5

**Đầu ra**

- Workspace dbw-instacart trên Sub A
- Output workspace URL/ID

**Xong khi**

- [ ] Mở được https://<workspace_url>, thấy catalog mặc định nếu spike UC đạt
- [ ] Plan lần hai No changes

### AD-OPS-12

**Dựng Azure SQL retail_db free offer, firewall và secret mật khẩu (10-core bước 3.6)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14)
- Tài liệu: `docs/devops/03_terraform_azure.md §3 bước 3.6`, `docs/de/01_ingest_landing.md §3`
- **Bắt đầu sau:** [AD-OPS-10](01_devops.md#ad-ops-10) [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-15](01_devops.md#ad-ops-15) (bắt đầu), [AD-OPS-22](01_devops.md#ad-ops-22) (hoàn tất), [AD-OPS-25](01_devops.md#ad-ops-25) (bắt đầu), [AD-DE-04](02_de_batch.md#ad-de-04) (hoàn tất)

retail_db là nguồn CDC cho product_prices và promotions. Mật khẩu chỉ nằm trong Key Vault.

**Việc cần làm**

1. random_password + module AVM sql-server (hoặc azurerm_mssql_database nếu AVM thiếu free offer), DB retail_db GP_S_Gen5_2
2. Bật free offer (use_free_limit, AutoPause); nếu provider chưa hỗ trợ thì bật bằng az CLI rồi import
3. Firewall allow-azure và member_ips (biến không default, giá trị qua *.auto.tfvars / GitHub Variable)
4. azurerm_key_vault_secret sql-admin-password
5. Thêm output sql_server_fqdn; đánh dấu [x] 3.6

**Đầu ra**

- SQL server + DB retail_db free offer
- Secret sql-admin-password trong Key Vault

**Xong khi**

- [ ] sqlcmd hoặc Azure Data Studio kết nối retail_db từ IP thành viên
- [ ] Portal hiện free offer đang áp dụng
- [ ] Plan lần hai No changes

### AD-OPS-13

**Dựng Data Factory với managed identity và quyền (10-core bước 3.7)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#15](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/15)
- Tài liệu: `docs/devops/03_terraform_azure.md §3 bước 3.7`
- **Bắt đầu sau:** [AD-OPS-10](01_devops.md#ad-ops-10) [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12), [AD-OPS-11](01_devops.md#ad-ops-11) [#13](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/13)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-15](01_devops.md#ad-ops-15) (bắt đầu), [AD-OPS-25](01_devops.md#ad-ops-25) (bắt đầu)

ADF copy nguồn ngoài vào landing và gọi Databricks Job (rubric 3.2). Stack này chỉ tạo factory và quyền; pipeline ở 40-orchestration.

**Việc cần làm**

1. Module AVM datafactory, system-assigned identity
2. Role: Storage Blob Data Contributor trên lake, Key Vault Secrets User trên KV, Contributor trên workspace
3. Kiểm role nằm trong ci_assignable_role_ids của bootstrap
4. Thêm output adf_id; đánh dấu [x] 3.7

**Đầu ra**

- Data Factory trên rg-instacart-core với 3 role assignment

**Xong khi**

- [ ] `az role assignment list --assignee <adf principal>` có đủ 3 role
- [ ] ADF Studio mở được factory
- [ ] Plan lần hai No changes

### AD-OPS-14

**Cấp RBAC cho 4 thành viên và đặt budget cảnh báo chi phí (10-core bước 3.8–3.9)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#16](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/16)
- Tài liệu: `docs/devops/03_terraform_azure.md §3 bước 3.8–3.9`
- **Bắt đầu sau:** [AD-OPS-10](01_devops.md#ad-ops-10) [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-15](01_devops.md#ad-ops-15) (bắt đầu), [AD-OPS-28](01_devops.md#ad-ops-28) (bắt đầu)

Rubric 5.4 cần nhiều người cùng làm trên một resource và thông tin chi phí. Budget báo sớm trước khi hết 100 USD credit.

**Việc cần làm**

1. azurerm_role_assignment members: Contributor RG, Storage Blob Data Contributor lake, Key Vault Secrets User KV; Reader Sub A
2. P1 Key Vault Secrets Officer; UAMI CI Key Vault Secrets Officer (ghi SAS từ 20-stream)
3. Action group ag-instacart-team từ member_emails (*.auto.tfvars, không commit)
4. azurerm_consumption_budget_subscription 100 USD, ngưỡng 25/50/75/90
5. Đánh dấu [x] 3.8, 3.9

**Đầu ra**

- Role assignment cho 4 thành viên và UAMI CI
- Action group và budget budget-sub-a

**Xong khi**

- [ ] P2–P4 thấy RG và đọc được container lake bằng Storage Explorer
- [ ] Email test của action group đến cả 4 người
- [ ] Portal Cost Management hiện budget-sub-a với 4 ngưỡng

### AD-OPS-15

**Nghiệm thu stack 10-core và hoàn thiện output bước 3.10**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#17](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/17)
- Tài liệu: `docs/devops/03_terraform_azure.md §3 bước 3.10 và checklist 10-core`
- **Bắt đầu sau:** [AD-OPS-12](01_devops.md#ad-ops-12) [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14), [AD-OPS-13](01_devops.md#ad-ops-13) [#15](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/15), [AD-OPS-14](01_devops.md#ad-ops-14) [#16](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/16)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-16](01_devops.md#ad-ops-16) (hoàn tất), [AD-OPS-17](01_devops.md#ad-ops-17) (bắt đầu)

Các stack 20, 30, 40, 50 đọc output của 10-core qua remote state. Checklist đạt thì apply chuyển sang CI.

**Việc cần làm**

1. Bổ sung đủ output 3.10 (location, rg_name, lake_account_name, lake_dfs_endpoint, …)
2. Chạy plan hai lần liên tiếp để xác nhận ổn định
3. Đi hết checklist 10-core (container, shared key, đăng nhập workspace P2–P4, sqlcmd, email)
4. Chạy trivy config trên infra, sửa lỗi HIGH/CRITICAL còn lại

**Đầu ra**

- infra/stacks/10-core/outputs.tf đủ 3.10
- Checklist 10-core trong main.tf đánh dấu [x] hết

**Xong khi**

- [ ] `terraform plan` báo No changes
- [ ] `terraform output` liệt kê đủ output của 3.10
- [ ] Trivy không còn lỗi HIGH/CRITICAL ở 10-core

### AD-OPS-16

**Bổ sung job java, Infracost và chặn trivy trong pr-checks**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#18](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/18)
- Tài liệu: `docs/devops/04_github_actions_cicd.md §3.1`, `docs/devops/04_github_actions_cicd.md §4`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-OPS-15](01_devops.md#ad-ops-15) [#17](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/17)
- **Mở khóa:** Không có.

pr-checks hiện thiếu job java (MapReduce ở G5) và cost (Infracost, bằng chứng chi phí rubric 5.4); trivy chưa chặn merge.

**Việc cần làm**

1. Thêm job java dùng dorny/paths-filter, chỉ build mvn -B verify khi đụng src/mapreduce/**, luôn chạy để làm required check
2. Thêm job cost (infracost diff, comment lên PR), lưu INFRACOST_API_KEY vào Secrets
3. Đổi trivy exit-code thành 1 sau khi 10-core sạch lỗi
4. Thêm java vào danh sách required check trong branch protection
5. Cập nhật docs/devops/04 §4 nếu thêm biến/secret

**Đầu ra**

- .github/workflows/pr-checks.yml có job java, cost; trivy chặn
- Secret INFRACOST_API_KEY

**Xong khi**

- [ ] PR thử đụng infra/** có comment Infracost
- [ ] PR không đụng src/mapreduce/** vẫn có check java xanh
- [ ] PR cố ý thêm lỗi trivy HIGH bị báo đỏ

### AD-OPS-17

**Chạy thử trọn chuỗi CI hạ tầng bằng PR đổi tag ở 10-core**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#19](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/19)
- Tài liệu: `docs/devops/04_github_actions_cicd.md §1`, `docs/devops/04_github_actions_cicd.md §3.2–3.3`, `docs/devops/04_github_actions_cicd.md §5`
- **Bắt đầu sau:** [AD-OPS-02](01_devops.md#ad-ops-02) [#4](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/4), [AD-OPS-15](01_devops.md#ad-ops-15) [#17](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/17)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-23](01_devops.md#ad-ops-23) (bắt đầu), [AD-OPS-25](01_devops.md#ad-ops-25) (hoàn tất)

Từ đây mọi thay đổi hạ tầng chỉ đi qua PR → tf-plan → merge → tf-apply có duyệt. Lịch sử Deployments là bằng chứng 5.4.

**Việc cần làm**

1. Mở PR thêm tag ci_test = '1' vào 10-core
2. Kiểm pr-checks xanh, tf-plan comment đúng '1 to change'
3. P1 approve, merge; duyệt tf-apply trong environment prod
4. Sửa workflow nếu thiếu TF_VAR_* (member_object_ids, member_ips, member_emails)
5. PR gỡ tag ci_test

**Đầu ra**

- Comment plan trên PR thử
- Run tf-apply thành công có người duyệt trong tab Deployments

**Xong khi**

- [ ] tf-apply chạy xong sau khi được duyệt
- [ ] Lần plan sau của 10-core báo No changes
- [ ] Thử push thẳng lên main bị từ chối

### AD-OPS-18

**Thêm service principal CI, SP Function và user theo vai trò vào workspace (30-databricks bước 2)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20)
- Tài liệu: `docs/devops/05_databricks_platform.md §2`, `docs/devops/05_databricks_platform.md §3 bước 2`
- **Bắt đầu sau:** [AD-OPS-10](01_devops.md#ad-ops-10) [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12), [AD-OPS-11](01_devops.md#ad-ops-11) [#13](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/13)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-20](01_devops.md#ad-ops-20) (bắt đầu), [AD-OPS-21](01_devops.md#ad-ops-21) (bắt đầu), [AD-OPS-22](01_devops.md#ad-ops-22) (bắt đầu), [AD-DS-23](05_data_science.md#ad-ds-23) (bắt đầu)

UAMI CI phải là service principal trong workspace để deploy bundle và chạy job prod; SP Function cần có sẵn cho G5. Quyền UC cấp theo user hoặc nhóm tùy kết quả spike.

**Việc cần làm**

1. databricks_service_principal ci (application_id = client id id-github-ci) và func (sp-func-recommend, không entitlement)
2. Biến roles (de, ds, bi) không default; đặt qua *.auto.tfvars và GitHub Variable TF_VAR_roles
3. databricks_user theo roles, hoặc nhóm account/Entra nếu spike cho phép
4. Thêm TF_VAR_roles vào tf-plan, _tf-stack, drift-check; ghi vào docs/devops/04 §4
5. Đánh dấu [x] bước 2

**Đầu ra**

- sp-github-ci, sp-func-recommend và user thành viên trong workspace

**Xong khi**

- [ ] `databricks service-principals list` có sp-github-ci và sp-func-recommend
- [ ] 4 thành viên đăng nhập workspace được
- [ ] Plan lần hai No changes

### AD-OPS-19

**Tạo storage credential, external location, catalog instacart, 8 schema và volume (30-databricks bước 3–4)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21)
- Tài liệu: `docs/devops/05_databricks_platform.md §3 bước 3–4`
- **Bắt đầu sau:** [AD-OPS-10](01_devops.md#ad-ops-10) [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12), [AD-OPS-11](01_devops.md#ad-ops-11) [#13](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/13)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-20](01_devops.md#ad-ops-20) (bắt đầu), [AD-DE-08](02_de_batch.md#ad-de-08) (hoàn tất), [AD-DE-20](02_de_batch.md#ad-de-20) (hoàn tất), [AD-DE-21](02_de_batch.md#ad-de-21) (hoàn tất)

Catalog và volume raw là điều kiện của G1: Auto Loader đọc /Volumes/instacart/landing_meta/raw/. Schema ds_data, ml và volume exports, inbox phục vụ DS.

**Việc cần làm**

1. databricks_storage_credential cred-instacart-lake từ Access Connector
2. 7 external location el-<container>
3. Catalog instacart; 8 schema landing_meta, bronze, silver, gold, ml, ds_data, instacart_sandbox, ops với storage_root riêng
4. Volume raw, checkpoints, ds_data.exports, ml.inbox; kiểm đường dẫn không chồng storage_root
5. Nếu spike không cho tạo storage credential: đi nhánh managed storage và ghi lại
6. Đánh dấu [x] bước 3, 4

**Đầu ra**

- Storage credential, 7 external location, catalog instacart, 8 schema, 4 volume

**Xong khi**

- [ ] `SHOW SCHEMAS IN instacart` liệt kê đủ 8 schema
- [ ] `DESCRIBE EXTERNAL LOCATION el-bronze` trả về đúng URL
- [ ] `LIST '/Volumes/instacart/landing_meta/raw/'` chạy được
- [ ] Plan lần hai No changes

### AD-OPS-20

**Cấp quyền Unity Catalog theo vai trò (30-databricks bước 5)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22)
- Tài liệu: `docs/devops/05_databricks_platform.md §3 bước 5`
- **Bắt đầu sau:** [AD-OPS-18](01_devops.md#ad-ops-18) [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20), [AD-OPS-19](01_devops.md#ad-ops-19) [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-23](01_devops.md#ad-ops-23) (bắt đầu), [AD-OPS-30](01_devops.md#ad-ops-30) (bắt đầu), [AD-DE-08](02_de_batch.md#ad-de-08) (hoàn tất), [AD-DE-16](02_de_batch.md#ad-de-16) (hoàn tất), [AD-DE-18](02_de_batch.md#ad-de-18) (hoàn tất), [AD-DE-20](02_de_batch.md#ad-de-20) (hoàn tất), [AD-DE-21](02_de_batch.md#ad-de-21) (hoàn tất), [AD-ST-03](03_de_stream.md#ad-st-03) (hoàn tất), [AD-BI-02](04_power_bi.md#ad-bi-02) (hoàn tất), [AD-DS-22](05_data_science.md#ad-ds-22) (hoàn tất)

Rubric 4 yêu cầu ràng buộc ai xem dữ liệu nào ở tầng dữ liệu. databricks_grants ghi đè grant không khai báo nên mỗi securable chỉ một khối.

**Việc cần làm**

1. Một khối databricks_grants cho catalog, mỗi schema và mỗi volume theo bảng ở 05 bước 5
2. SP CI: USE CATALOG, CREATE SCHEMA, ALL PRIVILEGES các schema dữ liệu
3. DE CREATE SCHEMA cho schema dev <user>_*
4. Đánh dấu [x] bước 5

**Đầu ra**

- Các khối databricks_grants trong infra/stacks/30-databricks

**Xong khi**

- [ ] P2 (de) tạo được bảng trong bronze
- [ ] P3 (bi) đọc bronze bị PERMISSION_DENIED, đọc gold được
- [ ] DS đọc được ds_data, ghi được ml
- [ ] Plan lần hai No changes

### AD-OPS-21

**Tạo cluster policy single-node, pipeline và ds-benchmark, chặn tạo cluster tự do (30-databricks bước 6)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23)
- Tài liệu: `docs/devops/05_databricks_platform.md §3 bước 6`, `docs/de/02_bronze_ldp.md §2.1`
- **Bắt đầu sau:** [AD-OPS-18](01_devops.md#ad-ops-18) [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-23](01_devops.md#ad-ops-23) (bắt đầu), [AD-OPS-30](01_devops.md#ad-ops-30) (bắt đầu), [AD-DE-08](02_de_batch.md#ad-de-08) (hoàn tất), [AD-ST-03](03_de_stream.md#ad-st-03) (hoàn tất)

Quota 4 vCPU buộc cưỡng chế single-node và tự tắt; policy cố định giúp benchmark rubric 3.1 lặp lại được. Bundle tra policy theo tên.

**Việc cần làm**

1. databricks_cluster_policy instacart-single-node (DS3_v2, DBR 15.4, local[*], autoterminate ≤ 20, tag owner bắt buộc)
2. Policy loại dlt instacart-pipeline-single-node theo de/02 §2.1
3. Policy instacart-ds-benchmark cho chọn local[1]/[2]/[4]/[*]
4. databricks_permissions CAN_USE cho thành viên và SP CI (ds-benchmark: vai ds + SP CI)
5. databricks_entitlements tắt allow_cluster_create của nhóm users
6. Đánh dấu [x] bước 6

**Đầu ra**

- 3 cluster policy và quyền dùng

**Xong khi**

- [ ] Thành viên không tạo được cluster ngoài policy
- [ ] Cluster theo policy tự tắt sau tối đa 20 phút
- [ ] Cluster theo instacart-ds-benchmark với local[2] trả sc.defaultParallelism = 2

### AD-OPS-22

**Tạo secret scope Key Vault, serverless SQL warehouse và output cho DAB, Power BI (30-databricks bước 7–9)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24)
- Tài liệu: `docs/devops/05_databricks_platform.md §3 bước 7–9`, `infra/AGENTS.md`
- **Bắt đầu sau:** [AD-OPS-18](01_devops.md#ad-ops-18) [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20)
- **Hoàn tất còn cần:** [AD-OPS-12](01_devops.md#ad-ops-12) [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14)
- **Mở khóa:** [AD-OPS-23](01_devops.md#ad-ops-23) (bắt đầu), [AD-DE-05](02_de_batch.md#ad-de-05) (hoàn tất), [AD-DE-16](02_de_batch.md#ad-de-16) (hoàn tất), [AD-DE-18](02_de_batch.md#ad-de-18) (hoàn tất), [AD-ST-03](03_de_stream.md#ad-st-03) (hoàn tất), [AD-BI-02](04_power_bi.md#ad-bi-02) (hoàn tất), [AD-DS-12](05_data_science.md#ad-ds-12) (hoàn tất)

Notebook đọc bí mật qua scope kv; Power BI cần host và HTTP path của warehouse. Secret scope Key Vault cần token người dùng nên P1 apply từ máy.

**Việc cần làm**

1. databricks_secret_scope kv dựa trên Key Vault, lifecycle ignore_changes = all; P1 apply từ máy local
2. Gán Key Vault Secrets User cho ứng dụng AzureDatabricks (bổ sung ở 10-core nếu thiếu)
3. databricks_secret_acl READ cho de, ds, SP CI
4. databricks_sql_endpoint wh-instacart-bi serverless 2X-Small, auto stop 10 phút, tag owner; permissions CAN_USE
5. Output catalog, cluster_policy_id, ci_sp_application_id, warehouse_id, warehouse_http_path
6. Đặt GitHub Variable DATABRICKS_HOST; đánh dấu [x] bước 7–9

**Đầu ra**

- Secret scope kv, warehouse wh-instacart-bi, output 30-databricks
- GitHub Variable DATABRICKS_HOST

**Xong khi**

- [ ] `dbutils.secrets.get('kv','sql-admin-password')` trả [REDACTED]
- [ ] SELECT 1 chạy được trên wh-instacart-bi
- [ ] `terraform output warehouse_http_path` có giá trị

### AD-OPS-23

**Nghiệm thu G1: chạy 30-databricks qua CI và thử de-deploy dev/prod**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G1**
- Issue: [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25)
- Tài liệu: `docs/devops/05_databricks_platform.md §5`, `docs/devops/04_github_actions_cicd.md §3.4`, `docs/devops/04_github_actions_cicd.md §5`
- **Bắt đầu sau:** [AD-OPS-17](01_devops.md#ad-ops-17) [#19](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/19), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22), [AD-OPS-21](01_devops.md#ad-ops-21) [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24)
- **Hoàn tất còn cần:** [AD-DE-18](02_de_batch.md#ad-de-18) [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54)
- **Mở khóa:** [AD-OPS-26](01_devops.md#ad-ops-26) (bắt đầu), [AD-OPS-27](01_devops.md#ad-ops-27) (bắt đầu), [AD-OPS-31](01_devops.md#ad-ops-31) (bắt đầu), [AD-DE-19](02_de_batch.md#ad-de-19) (hoàn tất)

Đạt checklist 05 §5 thì mở G1 để DE chạy ADF → landing → bronze trên cloud. Bundle validate và de-deploy cần workspace thật.

**Việc cần làm**

1. Chạy plan 30-databricks qua tf-plan trên một PR, xác nhận No changes
2. Kiểm pr-checks / bundle validate xanh khi đã có DATABRICKS_HOST
3. PR thử sửa job trong bundles/: job [dev …] xuất hiện, đóng PR thì biến mất
4. Merge thử để de-deploy prod chạy, job chạy bằng sp-github-ci
5. Đi hết checklist 05 §5, đóng milestone G1

**Đầu ra**

- Checklist 05 §5 đạt, checklist 30-databricks trong main.tf đánh dấu [x] hết
- Run de-deploy dev, dev-cleanup, prod thành công

**Xong khi**

- [ ] `databricks bundle deploy -t dev` từ máy P2 tạo được job [dev p2] instacart-batch-medallion
- [ ] Plan 30-databricks báo No changes
- [ ] Milestone G1 đóng

### AD-OPS-24

**Dựng stack 20-stream (Event Hubs, SAS, secret) và nghiệm thu stream-toggle**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G2**
- Issue: [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26)
- Tài liệu: `docs/devops/03_terraform_azure.md §4`, `docs/devops/04_github_actions_cicd.md §3.5`
- **Bắt đầu sau:** [AD-OPS-02](01_devops.md#ad-ops-02) [#4](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/4), [AD-OPS-10](01_devops.md#ad-ops-10) [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-33](01_devops.md#ad-ops-33) (bắt đầu), [AD-ST-02](03_de_stream.md#ad-st-02) (hoàn tất), [AD-ST-03](03_de_stream.md#ad-st-03) (hoàn tất), [AD-ST-08](03_de_stream.md#ad-st-08) (hoàn tất), [AD-ST-09](03_de_stream.md#ad-st-09) (hoàn tất)

Event Hubs ở Sub B, cố ý khác region Sub A để đo độ trễ rubric 3.3; bật tắt theo buổi để tiết kiệm. P3 giữ Sub B, P1 duyệt PR.

**Việc cần làm**

1. Module AVM eventhub namespace Standard capacity 1, hub orders-stream 2 partition, retention 1 ngày, consumer group dbx-bronze
2. SAS dbx-listen (listen) và sim-send (send)
3. Secret eh-listen-connection-string, eh-send-connection-string, eh-bootstrap-server vào Key Vault Sub A qua provider alias core
4. Chạy stream-toggle up rồi down; kiểm lịch đêm chạy down
5. Đánh dấu [x] checklist 20-stream

**Đầu ra**

- infra/stacks/20-stream đủ resource theo checklist
- Run stream-toggle up/down thành công

**Xong khi**

- [ ] kcat gửi được 1 message vào :9093 bằng SAS send
- [ ] Secret eh-* xuất hiện trong Key Vault Sub A sau up, mất sau down
- [ ] destroy rồi apply lại xong trong 5 phút
- [ ] Run theo lịch 0 17 * * * kết thúc với namespace bị xóa

### AD-OPS-25

**Khai báo linked service và dataset ADF trong stack 40-orchestration**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G3**
- Issue: [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27)
- Tài liệu: `docs/devops/06_de_den_dashboard.md §1`
- **Bắt đầu sau:** [AD-OPS-12](01_devops.md#ad-ops-12) [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14), [AD-OPS-13](01_devops.md#ad-ops-13) [#15](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/15)
- **Hoàn tất còn cần:** [AD-OPS-17](01_devops.md#ad-ops-17) [#19](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/19)
- **Mở khóa:** [AD-OPS-26](01_devops.md#ad-ops-26) (bắt đầu), [AD-OPS-27](01_devops.md#ad-ops-27) (bắt đầu), [AD-DE-06](02_de_batch.md#ad-de-06) (hoàn tất), [AD-DE-07](02_de_batch.md#ad-de-07) (hoàn tất), [AD-DE-19](02_de_batch.md#ad-de-19) (hoàn tất)

ADF quản lý bằng Terraform để PR thấy được thay đổi và drift-check bắt được sửa tay trong Studio. Linked service là nền cho pipeline của P2.

**Việc cần làm**

1. Linked service ls_adls, ls_kv, ls_sql_retail (mật khẩu qua ls_kv), ls_zenodo, ls_off_api, ls_dbx (MSI)
2. Dataset CSV, Parquet, JSON, SQL tham số hóa ingest_date
3. Thêm envs/40-orchestration.tfvars nếu cần biến mới
4. Apply qua tf-apply

**Đầu ra**

- 6 linked service và các dataset trên factory

**Xong khi**

- [ ] Test connection của từng linked service trong ADF Studio thành công
- [ ] Plan 40-orchestration báo No changes

### AD-OPS-26

**Đưa pipeline và trigger ADF vào 40-orchestration, bật lịch hằng ngày khi pl_master đạt**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G3**
- Issue: [#28](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/28)
- Tài liệu: `docs/devops/06_de_den_dashboard.md §1`, `docs/devops/06_de_den_dashboard.md §3`
- **Bắt đầu sau:** [AD-OPS-23](01_devops.md#ad-ops-23) [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25), [AD-OPS-25](01_devops.md#ad-ops-25) [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27)
- **Hoàn tất còn cần:** [AD-DE-06](02_de_batch.md#ad-de-06) [#42](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/42), [AD-DE-07](02_de_batch.md#ad-de-07) [#43](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/43), [AD-DE-19](02_de_batch.md#ad-de-19) [#55](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/55), [AD-DE-18](02_de_batch.md#ad-de-18) [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54)
- **Mở khóa:** [AD-OPS-33](01_devops.md#ad-ops-33) (hoàn tất)

pl_master gọi job Databricks theo job ID nên phải apply sau lần deploy bundle prod đầu tiên. Lịch ADF → Databricks Job là bằng chứng ETL tự động rubric 3.2.

**Việc cần làm**

1. azurerm_data_factory_pipeline với activities_json = file(adf/pipelines/<tên>.json)
2. Data source databricks_job lấy job ID theo tên job prod
3. Trigger schedule (02:00 hằng ngày cho pl_master) với activated = var.triggers_on, mặc định false
4. Kiểm tf-apply chạy 40-orchestration sau 30-databricks
5. Khi pl_master chạy tay đạt: PR đặt triggers_on = true

**Đầu ra**

- 4 pipeline và trigger trên factory
- PR bật triggers_on = true

**Xong khi**

- [ ] ADF Studio thấy 4 pipeline giống JSON trong repo
- [ ] Trigger hằng ngày ở trạng thái Started sau PR bật
- [ ] Có ít nhất một lần chạy pl_master theo lịch thành công trong Monitor

### AD-OPS-27

**Kiểm thử drift-check bằng một thay đổi tay có chủ đích**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G3**
- Issue: [#29](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/29)
- Tài liệu: `docs/devops/04_github_actions_cicd.md §3.6`
- **Bắt đầu sau:** [AD-OPS-23](01_devops.md#ad-ops-23) [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25), [AD-OPS-25](01_devops.md#ad-ops-25) [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-33](01_devops.md#ad-ops-33) (bắt đầu)

drift-check giữ nguyên tắc không sửa tay trên Portal; matrix 10/30/40 chỉ plan được khi cả ba stack đã có state.

**Việc cần làm**

1. Chạy drift-check bằng workflow_dispatch khi không có drift, xác nhận không mở issue
2. Sửa tay một tag trên RG core, chạy lại, xác nhận issue Drift có label drift
3. Gỡ thay đổi tay, đóng issue, ghi lại làm bằng chứng
4. Kiểm drift-check có đủ TF_VAR_* mà các stack cần

**Đầu ra**

- Issue Drift: 10-core (…) có label drift (đã đóng)

**Xong khi**

- [ ] Run drift-check không có drift kết thúc xanh và không tạo issue
- [ ] Run có drift tạo đúng một issue cho đúng stack

### AD-OPS-28

**Lập bảng theo dõi chi phí theo tag và quota vCPU hằng tuần**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G4**
- Issue: [#30](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/30)
- Tài liệu: `docs/devops/00_tong_quan.md §1`, `docs/devops/02_bootstrap_tfstate.md §4`, `docs/devops/06_de_den_dashboard.md §6.4`
- **Bắt đầu sau:** [AD-OPS-14](01_devops.md#ad-ops-14) [#16](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/16)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-33](01_devops.md#ad-ops-33) (bắt đầu)

Checklist G4 cần bảng chi phí theo owner/stack và ảnh quota vCPU cho rubric 5.4. Theo dõi định kỳ giúp không hết 100 USD giữa chừng.

**Việc cần làm**

1. Tạo view Cost Management lưu sẵn, nhóm theo tag owner và stack cho Sub A và Sub B
2. Mỗi tuần ghi chi phí lũy kế, dự báo và cảnh báo budget đã nhận
3. Chụp `az vm list-usage` của region Sub A sau khi dựng xong
4. Đề xuất cắt giảm khi chạm 50% (tắt stream, rút ngắn auto stop)

**Đầu ra**

- docs/devops/chi_phi_quota.md (bảng chi phí theo tuần, theo owner/stack, quota vCPU)
- Ảnh chụp Cost Management và quota trong docs/devops/evidence/

**Xong khi**

- [ ] Bảng có số liệu ít nhất 3 tuần liên tiếp
- [ ] Có ảnh chi phí theo tag owner/stack và ảnh quota vCPU

### AD-OPS-29

**Dựng workspace sandbox 90-sandbox cho DS ở Sub C**

- Vai dự kiến: **P4 · DS**
- Cổng: **G3**
- Issue: [#31](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/31)
- Tài liệu: `docs/devops/03_terraform_azure.md §5`, `docs/ds/04_thuc_nghiem_danh_gia.md §5.2`
- **Bắt đầu sau:** [AD-OPS-08](01_devops.md#ad-ops-08) [#10](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/10)
- **Hoàn tất còn cần:** [AD-DE-21](02_de_batch.md#ad-de-21) [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57)
- **Mở khóa:** [AD-OPS-30](01_devops.md#ad-ops-30) (hoàn tất)

Quota tính theo subscription nên thực nghiệm DS chạy ở Sub C để không giành 4 vCPU của Sub A. Chỉ đọc chung dữ liệu khi cùng region (cùng metastore).

**Việc cần làm**

1. Hoàn thiện module dbx_workspace (workspace + access connector + storage nhỏ), validate
2. Điền infra/envs/90-sandbox-p4.tfvars theo region đã chốt
3. P4 apply từ máy với key sandbox-p4.tfstate
4. Kiểm workspace gắn cùng metastore với Sub A và đọc được instacart.silver.baskets
5. Nếu khác region: ghi vào spike_results là DS chạy ở Sub A ngoài giờ batch

**Đầu ra**

- Workspace dbw-instacart-sbx-p4 trên Sub C
- Ghi chú lựa chọn nơi chạy DS trong spike_results.md

**Xong khi**

- [ ] Mở được workspace sandbox
- [ ] Cùng region: `SELECT count(*) FROM instacart.ds_data.transactions` chạy được từ sandbox
- [ ] Plan lần hai No changes

### AD-OPS-30

**Mở rộng khung bundle và quyền cho các job DS**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G3**
- Issue: [#32](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/32)
- Tài liệu: `docs/devops/05_databricks_platform.md §4`, `docs/devops/06_de_den_dashboard.md §7`
- **Bắt đầu sau:** [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22), [AD-OPS-21](01_devops.md#ad-ops-21) [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23)
- **Hoàn tất còn cần:** [AD-OPS-29](01_devops.md#ad-ops-29) [#31](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/31)
- **Mở khóa:** [AD-DS-12](05_data_science.md#ad-ds-12) (hoàn tất)

Job ds-benchmark dùng policy instacart-ds-benchmark và endpoint cần SP Function; khung bundle phải có biến trước khi P4 viết jobs_ds.yml, mlflow_ds.yml, serving_ds.yml.

**Việc cần làm**

1. Thêm biến ds_policy_id (lookup instacart-ds-benchmark) và func_sp_application_id vào bundles/databricks.yml
2. CI truyền func_sp_application_id từ output 10-core qua BUNDLE_VAR
3. Thêm target sandbox chỉ khi Sub C cùng region
4. Kiểm thành viên vai ds đọc ds_data.exports, ghi ml.inbox bằng AzCopy / volume

**Đầu ra**

- bundles/databricks.yml có biến DS (và target sandbox nếu dùng)
- de-deploy truyền BUNDLE_VAR_func_sp_application_id

**Xong khi**

- [ ] `databricks bundle validate -t dev` xanh với biến mới
- [ ] P4 ghi được file vào /Volumes/instacart/ml/inbox/

### AD-OPS-31

**Dựng stack 50-serving: Function App Flex Consumption cho API gợi ý**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G5**
- Issue: [#33](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/33)
- Tài liệu: `docs/devops/03_terraform_azure.md §5.1`, `docs/ds/05_mlflow_serving_bao_cao.md §3`
- **Bắt đầu sau:** [AD-OPS-23](01_devops.md#ad-ops-23) [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25)
- **Hoàn tất còn cần:** [AD-DS-23](05_data_science.md#ad-ds-23) [#108](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/108)
- **Mở khóa:** [AD-OPS-32](01_devops.md#ad-ops-32) (bắt đầu), [AD-DS-24](05_data_science.md#ad-ds-24) (hoàn tất)

Function là thành phần FaaS (rubric 1.4) để demo gọi API không giữ token Databricks. Tùy chọn ở G5.

**Việc cần làm**

1. Thêm output lake_account_id, func_identity_principal_id vào 10-core
2. Thêm Website Contributor vào ci_assignable_role_ids của bootstrap, P1 apply tay
3. Tạo infra/stacks/50-serving: storage gói code tắt shared key, service plan FC1, azurerm_function_app_flex_consumption gắn id-func-recommend
4. Role Storage Blob Data Reader trên container ds (phương án B), Website Contributor cho UAMI CI
5. Thêm 50-serving vào matrix pr-checks, filter tf-plan, chuỗi tf-apply, envs/50-serving.tfvars
6. Không được Flex thì dùng Y1 + azurerm_linux_function_app, ghi lý do

**Đầu ra**

- infra/stacks/50-serving/*.tf, infra/envs/50-serving.tfvars
- Function App func-instacart-recommend trên Sub A

**Xong khi**

- [ ] Plan 50-serving qua tf-plan có comment trên PR, apply qua tf-apply
- [ ] `az functionapp show -n func-instacart-recommend` trả về trạng thái Running
- [ ] Plan lần hai No changes

### AD-OPS-32

**Viết workflow func-deploy đưa code Function lên Azure**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G5**
- Issue: [#34](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/34)
- Tài liệu: `docs/devops/04_github_actions_cicd.md §3.8`, `docs/ds/05_mlflow_serving_bao_cao.md §3.2`
- **Bắt đầu sau:** [AD-OPS-31](01_devops.md#ad-ops-31) [#33](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/33)
- **Hoàn tất còn cần:** [AD-DS-24](05_data_science.md#ad-ds-24) [#109](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/109)
- **Mở khóa:** Không có.

Code Function không đi qua Terraform; workflow deploy khi push main đụng src/serving_func/**.

**Việc cần làm**

1. Tạo .github/workflows/func-deploy.yml: OIDC bằng UAMI CI, Azure/functions-action@v1
2. Bước kiểm sau deploy: POST /api/recommend với giỏ [24852], function key lấy từ Key Vault lúc chạy
3. Ghi workflow vào docs/devops/04 nếu có thay đổi so với §3.8

**Đầu ra**

- .github/workflows/func-deploy.yml
- Run func-deploy thành công

**Xong khi**

- [ ] Push đụng src/serving_func/** kích hoạt deploy thành công
- [ ] Bước kiểm sau deploy nhận mã 200

### AD-OPS-33

**Tổng hợp bằng chứng hạ tầng cho rubric 1.4, 3.2, 3.4 và 5.4**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G4**
- Issue: [#35](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/35)
- Tài liệu: `docs/devops/00_tong_quan.md §1`, `docs/devops/00_tong_quan.md §5`, `docs/devops/04_github_actions_cicd.md §2`
- **Bắt đầu sau:** [AD-OPS-24](01_devops.md#ad-ops-24) [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26), [AD-OPS-27](01_devops.md#ad-ops-27) [#29](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/29), [AD-OPS-28](01_devops.md#ad-ops-28) [#30](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/30)
- **Hoàn tất còn cần:** [AD-OPS-26](01_devops.md#ad-ops-26) [#28](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/28)
- **Mở khóa:** [AD-OPS-34](01_devops.md#ad-ops-34) (bắt đầu), [AD-RP-03](06_bao_cao_demo.md#ad-rp-03) (hoàn tất), [AD-RP-05](06_bao_cao_demo.md#ad-rp-05) (bắt đầu)

Báo cáo cần bằng chứng làm việc nhóm trên cloud, ETL tự động và tối ưu lưu trữ. Phần hạ tầng do P1 gom.

**Việc cần làm**

1. Bảng phân loại IaaS/PaaS/FaaS/SaaS theo thành phần đã dựng thật
2. Ảnh board GitHub Projects, Insights → Contributors, lịch sử PR và tab Deployments (duyệt prod)
3. Ảnh trigger ADF và run Databricks Job theo lịch (3.2)
4. Ảnh lifecycle landing → cool và dung lượng theo container (3.4)
5. Ảnh email budget, issue drift, comment tf-plan và Infracost
6. Tổng hợp chi phí từ bảng theo dõi

**Đầu ra**

- docs/devops/bang_chung_ha_tang.md với ảnh trong docs/devops/evidence/

**Xong khi**

- [ ] Mỗi tiêu chí 1.4, 3.2, 3.4, 5.4 có ít nhất một ảnh hoặc liên kết kiểm được
- [ ] Không ảnh nào lộ subscription ID, object ID, email

### AD-OPS-34

**Hủy hạ tầng cuối kỳ theo thứ tự ngược và chốt báo cáo chi phí**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G5**
- Issue: [#36](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/36)
- Tài liệu: `docs/devops/03_terraform_azure.md §6`, `AGENTS.md (Lệnh không được chạy)`
- **Bắt đầu sau:** [AD-OPS-33](01_devops.md#ad-ops-33) [#35](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/35)
- **Hoàn tất còn cần:** [AD-RP-09](06_bao_cao_demo.md#ad-rp-09) [#121](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/121), [AD-RP-10](06_bao_cao_demo.md#ad-rp-10) [#122](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/122)
- **Mở khóa:** Không có.

Dừng tiêu credit sau khi nộp. Destroy phải đi ngược thứ tự để provider databricks không lỗi khi mất workspace.

**Việc cần làm**

1. Xuất bảng gold, ml cần giữ và ảnh bằng chứng trước khi hủy
2. Tắt trigger ADF, tắt stream-toggle schedule
3. P1 (và P3, P4 với Sub B, C) hủy theo thứ tự 50-serving → 90-sandbox → 40 → 20 → 30 → 10 bằng tay hoặc workflow có duyệt
4. Giữ bootstrap đến khi xác nhận không còn tài nguyên, rồi gỡ lock và hủy
5. Ghi chi phí cuối cùng vào bảng chi phí

**Đầu ra**

- Các RG rg-instacart-* đã bị xóa
- Bảng chi phí có dòng chốt cuối kỳ

**Xong khi**

- [ ] `az group list --query "[?starts_with(name,'rg-instacart')]"` trả rỗng trên Sub A, B, C
- [ ] Cost Management không còn phát sinh chi phí mới sau 48 giờ
