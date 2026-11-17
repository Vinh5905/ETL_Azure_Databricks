# Phân việc toàn dự án

Toàn bộ việc còn lại của dự án được tách thành **120 task**. Làm xong hết các task này là xong dự án: hạ tầng, pipeline batch và stream, Power BI, phần DS, báo cáo và demo.

Mỗi task là một issue trên GitHub và một thẻ trên GitHub Project [ETL Instacart Azure Databricks](https://github.com/users/Vinh5905/projects/8). Tiêu đề issue bắt đầu bằng mã task, ví dụ `AD-DE-02 — …`.

## Các file phân việc

| Mã | File | Số task |
|---|---|---:|
| `AD-OPS-xx` | [DevOps / nền tảng](01_devops.md) | 34 |
| `AD-DE-xx` | [DE batch](02_de_batch.md) | 27 |
| `AD-ST-xx` | [DE streaming](03_de_stream.md) | 10 |
| `AD-BI-xx` | [Power BI](04_power_bi.md) | 12 |
| `AD-DS-xx` | [Data Science](05_data_science.md) | 27 |
| `AD-RP-xx` | [Báo cáo, bằng chứng và demo](06_bao_cao_demo.md) | 10 |

## Quy ước

- **Vai:** task ghi vai dự kiến P1–P4 theo [phân vai](../00_tong_quan_du_an.md#7-phân-vai). Field `Owner` trên Project đang để theo vai; khi có người thật thì đổi `Owner` và `Assignees`.
- **Bắt đầu sau** (gate bắt đầu): task kia phải Done thì mới bắt đầu phần triển khai. Trên issue, quan hệ này được gắn bằng **Blocked by** của GitHub.
- **Hoàn tất còn cần** (gate hoàn tất): được chuẩn bị và làm trước, nhưng chỉ nghiệm thu bản cuối khi task kia Done.
- **Chuẩn bị ngay:** không có gate bắt đầu, làm được ngay, song song với task khác.
- **Mở khóa:** các task nhận đầu ra của task này.
- Mỗi task một nhánh `<type>/<vai>-<việc>` và PR có `Closes #<issue>`, theo [CONTRIBUTING](../../CONTRIBUTING.md).

## Trạng thái trên Project

Mỗi thẻ chỉ có một field tiến độ là `Status`, gồm 6 cột. Chỉ có In Progress là kéo tay. Các cột còn lại do script tự đặt theo phụ thuộc và PR:

| Status | Nghĩa | Ai đổi |
|---|---|---|
| Blocked | Còn task ở gate bắt đầu chưa Done | Script |
| Ready | Mọi task ở gate bắt đầu đã Done, nhận làm được ngay | Script |
| In Progress | Người được giao đang làm, chưa mở PR hoặc PR còn là draft | Người làm kéo thẻ khi bắt đầu; script đặt khi có PR draft |
| Review | PR đã mở và chờ review, hoặc đã push commit mới sau góp ý, hoặc đã approve chờ merge | Script |
| Needs Update | Reviewer đã comment hoặc yêu cầu sửa trên PR, sau đó chưa có commit mới | Script |
| Done | PR merge, issue đóng dạng completed | Script (và workflow có sẵn của Project) |

Ghi chú:
- PR phải ghi `Closes #<issue>` thì script mới nhận ra PR đó thuộc task nào.
- Script bỏ qua comment của bot (ví dụ comment plan của `tf-plan`) và comment của chính tác giả PR.
- PR bị đóng mà không merge thì thẻ quay về In Progress.

Field `Waiting on` ghi các task liên quan chưa Done:
- `Start:` là các task đang chặn.
- `Finish:` là các task cần cho gate hoàn tất.
- `None` là không còn chờ task nào.

Workflow `sync-task-board` tự chạy `scripts/sync_task_board.py --apply` để cập nhật `Status` và `Waiting on` cho cả board. Workflow chạy khi:
- Issue bị đóng hoặc mở lại.
- PR được mở, push commit, chuyển draft, hoặc bị đóng.
- Có review hoặc comment mới.
- Cứ 30 phút một lần.

Workflow cần secret `PROJECT_TOKEN`: PAT classic, chỉ cần scope `project` vì repo public. Nếu chuyển repo sang private thì thêm scope `repo`.

Muốn thấy kết quả ngay thì chạy tay `python3 scripts/sync_task_board.py --apply`, hoặc bấm *Run workflow*.

## Bảng tổng

| Mã | Việc | Vai | Cổng | Phụ thuộc | Mở khóa | Issue |
|---|---|---|---|---|---|---|
| [AD-OPS-01](01_devops.md#ad-ops-01) | Cài bộ công cụ cho cả 4 máy và ghi bảng công cụ vào spike_results | P1 | G0 | Chuẩn bị ngay | Không | [#3](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/3) |
| [AD-OPS-02](01_devops.md#ad-ops-02) | Thiết lập quản trị repo: board, label, milestone, CODEOWNERS, branch protection, environment | P1 | G0 | Chuẩn bị ngay | AD-OPS-17 (bắt đầu), AD-OPS-24 (bắt đầu) | [#4](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/4) |
| [AD-OPS-03](01_devops.md#ad-ops-03) | Spike region được phép, quota vCPU và quyền trong tenant trường | P1 | G0 | Chuẩn bị ngay | AD-OPS-04 (bắt đầu), AD-OPS-06 (bắt đầu) | [#5](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/5) |
| [AD-OPS-04](01_devops.md#ad-ops-04) | Spike Databricks nền: workspace, Unity Catalog, serverless warehouse, cluster single-node | P1 | G0 | Bắt đầu: AD-OPS-03 | AD-OPS-05 (bắt đầu), AD-BI-01 (hoàn tất) | [#6](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/6) |
| [AD-OPS-05](01_devops.md#ad-ops-05) | Spike Databricks nâng cao: Lakeflow single-node, Kafka, policy local[n], for_each_task, Model Serving | P1 | G0 | Bắt đầu: AD-OPS-04; Hoàn tất: AD-OPS-06 | AD-OPS-07 (bắt đầu), AD-DE-08 (hoàn tất), AD-ST-08 (hoàn tất), AD-ST-10 (hoàn tất) | [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7) |
| [AD-OPS-06](01_devops.md#ad-ops-06) | Spike Azure SQL free offer, CDC, Event Hubs Kafka, ADF và Azure Functions | P1 | G0 | Bắt đầu: AD-OPS-03 | AD-OPS-05 (hoàn tất), AD-OPS-07 (bắt đầu), AD-DE-04 (hoàn tất) | [#8](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/8) |
| [AD-OPS-07](01_devops.md#ad-ops-07) | Chốt quyết định G0, dọn tài nguyên spike và mở cổng G0 | P1 | G0 | Bắt đầu: AD-OPS-05, AD-OPS-06; Hoàn tất: AD-BI-01 | AD-OPS-08 (bắt đầu), AD-RP-02 (hoàn tất) | [#9](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/9) |
| [AD-OPS-08](01_devops.md#ad-ops-08) | Apply bootstrap, chuyển state lên blob và cấu hình OIDC cho GitHub Actions | P1 | G1 | Bắt đầu: AD-OPS-07 | AD-OPS-09 (bắt đầu), AD-OPS-11 (bắt đầu), AD-OPS-29 (bắt đầu) | [#10](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/10) |
| [AD-OPS-09](01_devops.md#ad-ops-09) | Dựng ADLS Gen2 với 7 container và lifecycle landing sang cool (10-core bước 3.2) | P1 | G1 | Bắt đầu: AD-OPS-08 | AD-OPS-10 (bắt đầu) | [#11](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/11) |
| [AD-OPS-10](01_devops.md#ad-ops-10) | Dựng Key Vault, Access Connector và danh tính id-func-recommend (10-core bước 3.3–3.4) | P1 | G1 | Bắt đầu: AD-OPS-09 | AD-OPS-12 (bắt đầu), AD-OPS-13 (bắt đầu), AD-OPS-14 (bắt đầu), AD-OPS-18 (bắt đầu), AD-OPS-19 (bắt đầu), AD-OPS-24 (bắt đầu) | [#12](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/12) |
| [AD-OPS-11](01_devops.md#ad-ops-11) | Dựng Databricks workspace Premium (10-core bước 3.5) | P1 | G1 | Bắt đầu: AD-OPS-08 | AD-OPS-13 (bắt đầu), AD-OPS-18 (bắt đầu), AD-OPS-19 (bắt đầu) | [#13](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/13) |
| [AD-OPS-12](01_devops.md#ad-ops-12) | Dựng Azure SQL retail_db free offer, firewall và secret mật khẩu (10-core bước 3.6) | P1 | G1 | Bắt đầu: AD-OPS-10 | AD-OPS-15 (bắt đầu), AD-OPS-22 (hoàn tất), AD-OPS-25 (bắt đầu), AD-DE-04 (hoàn tất) | [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14) |
| [AD-OPS-13](01_devops.md#ad-ops-13) | Dựng Data Factory với managed identity và quyền (10-core bước 3.7) | P1 | G1 | Bắt đầu: AD-OPS-10, AD-OPS-11 | AD-OPS-15 (bắt đầu), AD-OPS-25 (bắt đầu) | [#15](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/15) |
| [AD-OPS-14](01_devops.md#ad-ops-14) | Cấp RBAC cho 4 thành viên và đặt budget cảnh báo chi phí (10-core bước 3.8–3.9) | P1 | G1 | Bắt đầu: AD-OPS-10 | AD-OPS-15 (bắt đầu), AD-OPS-28 (bắt đầu) | [#16](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/16) |
| [AD-OPS-15](01_devops.md#ad-ops-15) | Nghiệm thu stack 10-core và hoàn thiện output bước 3.10 | P1 | G1 | Bắt đầu: AD-OPS-12, AD-OPS-13, AD-OPS-14 | AD-OPS-16 (hoàn tất), AD-OPS-17 (bắt đầu) | [#17](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/17) |
| [AD-OPS-16](01_devops.md#ad-ops-16) | Bổ sung job java, Infracost và chặn trivy trong pr-checks | P1 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-15 | Không | [#18](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/18) |
| [AD-OPS-17](01_devops.md#ad-ops-17) | Chạy thử trọn chuỗi CI hạ tầng bằng PR đổi tag ở 10-core | P1 | G1 | Bắt đầu: AD-OPS-02, AD-OPS-15 | AD-OPS-23 (bắt đầu), AD-OPS-25 (hoàn tất) | [#19](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/19) |
| [AD-OPS-18](01_devops.md#ad-ops-18) | Thêm service principal CI, SP Function và user theo vai trò vào workspace (30-databricks bước 2) | P1 | G1 | Bắt đầu: AD-OPS-10, AD-OPS-11 | AD-OPS-20 (bắt đầu), AD-OPS-21 (bắt đầu), AD-OPS-22 (bắt đầu), AD-DS-23 (bắt đầu) | [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20) |
| [AD-OPS-19](01_devops.md#ad-ops-19) | Tạo storage credential, external location, catalog instacart, 8 schema và volume (30-databricks bước 3–4) | P1 | G1 | Bắt đầu: AD-OPS-10, AD-OPS-11 | AD-OPS-20 (bắt đầu), AD-DE-08 (hoàn tất), AD-DE-20 (hoàn tất), AD-DE-21 (hoàn tất) | [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21) |
| [AD-OPS-20](01_devops.md#ad-ops-20) | Cấp quyền Unity Catalog theo vai trò (30-databricks bước 5) | P1 | G1 | Bắt đầu: AD-OPS-18, AD-OPS-19 | AD-OPS-23 (bắt đầu), AD-OPS-30 (bắt đầu), AD-DE-08 (hoàn tất), AD-DE-16 (hoàn tất), AD-DE-18 (hoàn tất), AD-DE-20 (hoàn tất), AD-DE-21 (hoàn tất), AD-ST-03 (hoàn tất), AD-BI-02 (hoàn tất), AD-DS-22 (hoàn tất) | [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22) |
| [AD-OPS-21](01_devops.md#ad-ops-21) | Tạo cluster policy single-node, pipeline và ds-benchmark, chặn tạo cluster tự do (30-databricks bước 6) | P1 | G1 | Bắt đầu: AD-OPS-18 | AD-OPS-23 (bắt đầu), AD-OPS-30 (bắt đầu), AD-DE-08 (hoàn tất), AD-ST-03 (hoàn tất) | [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23) |
| [AD-OPS-22](01_devops.md#ad-ops-22) | Tạo secret scope Key Vault, serverless SQL warehouse và output cho DAB, Power BI (30-databricks bước 7–9) | P1 | G1 | Bắt đầu: AD-OPS-18; Hoàn tất: AD-OPS-12 | AD-OPS-23 (bắt đầu), AD-DE-05 (hoàn tất), AD-DE-16 (hoàn tất), AD-DE-18 (hoàn tất), AD-ST-03 (hoàn tất), AD-BI-02 (hoàn tất), AD-DS-12 (hoàn tất) | [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24) |
| [AD-OPS-23](01_devops.md#ad-ops-23) | Nghiệm thu G1: chạy 30-databricks qua CI và thử de-deploy dev/prod | P1 | G1 | Bắt đầu: AD-OPS-17, AD-OPS-20, AD-OPS-21, AD-OPS-22; Hoàn tất: AD-DE-18 | AD-OPS-26 (bắt đầu), AD-OPS-27 (bắt đầu), AD-OPS-31 (bắt đầu), AD-DE-19 (hoàn tất) | [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25) |
| [AD-OPS-24](01_devops.md#ad-ops-24) | Dựng stack 20-stream (Event Hubs, SAS, secret) và nghiệm thu stream-toggle | P3 | G2 | Bắt đầu: AD-OPS-02, AD-OPS-10 | AD-OPS-33 (bắt đầu), AD-ST-02 (hoàn tất), AD-ST-03 (hoàn tất), AD-ST-08 (hoàn tất), AD-ST-09 (hoàn tất) | [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26) |
| [AD-OPS-25](01_devops.md#ad-ops-25) | Khai báo linked service và dataset ADF trong stack 40-orchestration | P1 | G3 | Bắt đầu: AD-OPS-12, AD-OPS-13; Hoàn tất: AD-OPS-17 | AD-OPS-26 (bắt đầu), AD-OPS-27 (bắt đầu), AD-DE-06 (hoàn tất), AD-DE-07 (hoàn tất), AD-DE-19 (hoàn tất) | [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27) |
| [AD-OPS-26](01_devops.md#ad-ops-26) | Đưa pipeline và trigger ADF vào 40-orchestration, bật lịch hằng ngày khi pl_master đạt | P1 | G3 | Bắt đầu: AD-OPS-23, AD-OPS-25; Hoàn tất: AD-DE-06, AD-DE-07, AD-DE-19, AD-DE-18 | AD-OPS-33 (hoàn tất) | [#28](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/28) |
| [AD-OPS-27](01_devops.md#ad-ops-27) | Kiểm thử drift-check bằng một thay đổi tay có chủ đích | P1 | G3 | Bắt đầu: AD-OPS-23, AD-OPS-25 | AD-OPS-33 (bắt đầu) | [#29](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/29) |
| [AD-OPS-28](01_devops.md#ad-ops-28) | Lập bảng theo dõi chi phí theo tag và quota vCPU hằng tuần | P1 | G4 | Bắt đầu: AD-OPS-14 | AD-OPS-33 (bắt đầu) | [#30](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/30) |
| [AD-OPS-29](01_devops.md#ad-ops-29) | Dựng workspace sandbox 90-sandbox cho DS ở Sub C | P4 | G3 | Bắt đầu: AD-OPS-08; Hoàn tất: AD-DE-21 | AD-OPS-30 (hoàn tất) | [#31](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/31) |
| [AD-OPS-30](01_devops.md#ad-ops-30) | Mở rộng khung bundle và quyền cho các job DS | P1 | G3 | Bắt đầu: AD-OPS-20, AD-OPS-21; Hoàn tất: AD-OPS-29 | AD-DS-12 (hoàn tất) | [#32](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/32) |
| [AD-OPS-31](01_devops.md#ad-ops-31) | Dựng stack 50-serving: Function App Flex Consumption cho API gợi ý | P1 | G5 | Bắt đầu: AD-OPS-23; Hoàn tất: AD-DS-23 | AD-OPS-32 (bắt đầu), AD-DS-24 (hoàn tất) | [#33](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/33) |
| [AD-OPS-32](01_devops.md#ad-ops-32) | Viết workflow func-deploy đưa code Function lên Azure | P1 | G5 | Bắt đầu: AD-OPS-31; Hoàn tất: AD-DS-24 | Không | [#34](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/34) |
| [AD-OPS-33](01_devops.md#ad-ops-33) | Tổng hợp bằng chứng hạ tầng cho rubric 1.4, 3.2, 3.4 và 5.4 | P1 | G4 | Bắt đầu: AD-OPS-24, AD-OPS-27, AD-OPS-28; Hoàn tất: AD-OPS-26 | AD-OPS-34 (bắt đầu), AD-RP-03 (hoàn tất), AD-RP-05 (bắt đầu) | [#35](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/35) |
| [AD-OPS-34](01_devops.md#ad-ops-34) | Hủy hạ tầng cuối kỳ theo thứ tự ngược và chốt báo cáo chi phí | P1 | G5 | Bắt đầu: AD-OPS-33; Hoàn tất: AD-RP-09, AD-RP-10 | Không | [#36](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/36) |
| [AD-DE-01](02_de_batch.md#ad-de-01) | Chuyển khung src sang cấu trúc đích và tạo dữ liệu mẫu 1% | P2 | G0 | Chuẩn bị ngay | AD-DE-02 (hoàn tất), AD-DE-03 (hoàn tất) | [#37](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/37) |
| [AD-DE-02](02_de_batch.md#ad-de-02) | Viết hàm thuần sim_time và chuẩn hóa tên kèm test | P2 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DE-01 | AD-DE-12 (hoàn tất), AD-DE-13 (hoàn tất), AD-ST-01 (hoàn tất) | [#38](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/38) |
| [AD-DE-03](02_de_batch.md#ad-de-03) | Viết khuôn quarantine và bộ luật chất lượng dùng chung | P2 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DE-01 | AD-DE-11 (hoàn tất), AD-DE-12 (hoàn tất), AD-ST-05 (hoàn tất) | [#39](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/39) |
| [AD-DE-04](02_de_batch.md#ad-de-04) | Viết script SQL tạo bảng nguồn, bật CDC và bảng điều khiển etl | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-12, AD-OPS-06 | AD-DE-05 (hoàn tất), AD-DE-07 (bắt đầu) | [#40](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/40) |
| [AD-DE-05](02_de_batch.md#ad-de-05) | Viết seed_retail_db theo đợt và job chạy seed | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-DE-04, AD-OPS-22 | AD-DE-07 (hoàn tất) | [#41](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/41) |
| [AD-DE-06](02_de_batch.md#ad-de-06) | Dựng ADF pl_ingest_instacart chép CSV Zenodo vào landing | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-25 | AD-OPS-26 (hoàn tất), AD-DE-09 (hoàn tất) | [#42](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/42) |
| [AD-DE-07](02_de_batch.md#ad-de-07) | Dựng ADF pl_ingest_retail_db đọc CDC theo LSN | P2 | G1 | Bắt đầu: AD-DE-04; Hoàn tất: AD-DE-05, AD-OPS-25 | AD-OPS-26 (hoàn tất), AD-DE-09 (hoàn tất), AD-DE-19 (bắt đầu) | [#43](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/43) |
| [AD-DE-08](02_de_batch.md#ad-de-08) | Khai báo pipeline instacart_batch trong DAB với schema dev riêng | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-19, AD-OPS-21, AD-OPS-20, AD-OPS-05 | AD-DE-09 (hoàn tất), AD-DE-16 (hoàn tất), AD-DE-18 (bắt đầu), AD-ST-03 (hoàn tất) | [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44) |
| [AD-DE-09](02_de_batch.md#ad-de-09) | Viết bronze.py nạp landing vào bronze bằng Auto Loader | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-DE-06, AD-DE-07, AD-DE-08 | AD-DE-10 (hoàn tất), AD-DE-11 (hoàn tất), AD-DE-12 (hoàn tất) | [#45](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/45) |
| [AD-DE-10](02_de_batch.md#ad-de-10) | Viết crawler Open Food Facts và tải ảnh theo lô | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-09 | AD-DE-13 (hoàn tất), AD-DE-19 (hoàn tất) | [#46](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/46) |
| [AD-DE-11](02_de_batch.md#ad-de-11) | Dựng silver SCD2 lịch sử giá và SCD1 khuyến mãi | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-03, AD-DE-09 | AD-DE-14 (hoàn tất), AD-DE-22 (hoàn tất) | [#47](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/47) |
| [AD-DE-12](02_de_batch.md#ad-de-12) | Dựng silver đơn hàng: order_time_sim, orders_batch, order_items_prior | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-02, AD-DE-03, AD-DE-09 | AD-DE-15 (hoàn tất), AD-DE-16 (hoàn tất), AD-DE-26 (hoàn tất) | [#48](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/48) |
| [AD-DE-13](02_de_batch.md#ad-de-13) | Làm phẳng OFF và fuzzy match sang sản phẩm Instacart | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-02, AD-DE-10 | AD-DE-14 (hoàn tất) | [#49](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/49) |
| [AD-DE-14](02_de_batch.md#ad-de-14) | Dựng silver.dim_product không nhân dòng | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-11, AD-DE-13 | AD-DE-17 (hoàn tất), AD-DE-22 (hoàn tất), AD-DE-27 (hoàn tất) | [#50](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/50) |
| [AD-DE-15](02_de_batch.md#ad-de-15) | Gộp batch với stream ở silver và dựng baskets | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-12, AD-ST-05 | AD-DE-17 (hoàn tất), AD-DE-18 (hoàn tất), AD-DE-20 (hoàn tất), AD-DE-22 (hoàn tất), AD-DE-24 (hoàn tất) | [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51) |
| [AD-DE-16](02_de_batch.md#ad-de-16) | Viết collect_dq ghi kết quả expectations vào ops.dq_results | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-08, AD-DE-12, AD-OPS-22, AD-OPS-20 | AD-DE-18 (hoàn tất), AD-BI-07 (hoàn tất) | [#52](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/52) |
| [AD-DE-17](02_de_batch.md#ad-de-17) | Viết reconcile đối soát nguồn, bronze, silver vào ops.reconciliation | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-14, AD-DE-15, AD-ST-07 | AD-DE-18 (hoàn tất), AD-DE-21 (bắt đầu), AD-BI-07 (hoàn tất), AD-DS-17 (hoàn tất) | [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53) |
| [AD-DE-18](02_de_batch.md#ad-de-18) | Hoàn thiện job instacart-batch-medallion trong DAB | P2 | G3 | Bắt đầu: AD-DE-08; Hoàn tất: AD-DE-15, AD-DE-16, AD-DE-17, AD-OPS-22, AD-OPS-20 | AD-OPS-23 (hoàn tất), AD-OPS-26 (hoàn tất), AD-DE-19 (hoàn tất), AD-DE-25 (hoàn tất), AD-DE-27 (bắt đầu) | [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54) |
| [AD-DE-19](02_de_batch.md#ad-de-19) | Dựng ADF pl_ingest_off và pl_master điều phối hằng ngày | P2 | G3 | Bắt đầu: AD-DE-07; Hoàn tất: AD-DE-10, AD-DE-18, AD-OPS-25, AD-OPS-23 | AD-OPS-26 (hoàn tất), AD-DE-27 (bắt đầu) | [#55](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/55) |
| [AD-DE-20](02_de_batch.md#ad-de-20) | Viết SQL ds-prepare: snapshot, item_index, transactions, holdout_eval | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-15, AD-OPS-19, AD-OPS-20 | AD-DE-21 (bắt đầu) | [#56](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/56) |
| [AD-DE-21](02_de_batch.md#ad-de-21) | Xuất file txt cho Hadoop, dựng job ds-prepare và chạy v1 | P2 | G3 | Bắt đầu: AD-DE-17, AD-DE-20; Hoàn tất: AD-OPS-19, AD-OPS-20 | AD-OPS-29 (hoàn tất), AD-DS-07 (hoàn tất), AD-DS-11 (bắt đầu) | [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57) |
| [AD-DE-22](02_de_batch.md#ad-de-22) | Dựng gold.fact_order_lines với giá theo thời điểm | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-11, AD-DE-14, AD-DE-15 | AD-DE-23 (hoàn tất) | [#58](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/58) |
| [AD-DE-23](02_de_batch.md#ad-de-23) | Dựng gold tổng hợp, dim và bảng phân quyền bi_user_access | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-22 | AD-DE-24 (hoàn tất), AD-BI-02 (hoàn tất), AD-BI-06 (hoàn tất) | [#59](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/59) |
| [AD-DE-24](02_de_batch.md#ad-de-24) | Dựng gold.product_pair_counts cho trang chi tiết sản phẩm | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-15, AD-DE-23 | AD-BI-03 (hoàn tất) | [#60](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/60) |
| [AD-DE-25](02_de_batch.md#ad-de-25) | Viết refresh_bi và ghi ops.pipeline_runs | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-18, AD-BI-09, AD-BI-01 | Không | [#61](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/61) |
| [AD-DE-26](02_de_batch.md#ad-de-26) | Chạy benchmark CSV, Parquet, Delta, Delta OPTIMIZE | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-12 | AD-BI-12 (hoàn tất), AD-RP-03 (bắt đầu) | [#62](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/62) |
| [AD-DE-27](02_de_batch.md#ad-de-27) | Viết runbook chạy lại và diễn tập 4 kịch bản lỗi | P2 | G4 | Bắt đầu: AD-DE-18, AD-DE-19; Hoàn tất: AD-DE-14, AD-ST-07 | AD-RP-03 (hoàn tất) | [#63](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/63) |
| [AD-ST-01](03_de_stream.md#ad-st-01) | Viết simulator dựng sự kiện đơn hàng từ tập train | P3 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DE-02 | AD-ST-02 (bắt đầu) | [#64](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/64) |
| [AD-ST-02](03_de_stream.md#ad-st-02) | Gửi sự kiện thử vào Event Hubs bằng giao thức Kafka | P3 | G2 | Bắt đầu: AD-ST-01; Hoàn tất: AD-OPS-24 | AD-ST-04 (hoàn tất) | [#65](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/65) |
| [AD-ST-03](03_de_stream.md#ad-st-03) | Khai báo pipeline instacart_stream và job instacart-stream-15m trong DAB | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-DE-08, AD-OPS-21, AD-OPS-20, AD-OPS-22, AD-OPS-24 | AD-ST-04 (hoàn tất) | [#66](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/66) |
| [AD-ST-04](03_de_stream.md#ad-st-04) | Viết bronze_events đọc Kafka vào bronze.order_events | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-ST-02, AD-ST-03 | AD-ST-05 (hoàn tất), AD-ST-08 (bắt đầu) | [#67](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67) |
| [AD-ST-05](03_de_stream.md#ad-st-05) | Dựng silver stream: parse, kiểm chất lượng, AUTO CDC theo order_id | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-DE-03, AD-ST-04 | AD-DE-15 (hoàn tất), AD-ST-06 (hoàn tất), AD-ST-07 (bắt đầu) | [#68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68) |
| [AD-ST-06](03_de_stream.md#ad-st-06) | Dựng gold stream và bảng độ trễ ops.streaming_latency | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-ST-05 | AD-ST-07 (hoàn tất), AD-ST-09 (bắt đầu), AD-ST-10 (bắt đầu), AD-BI-07 (hoàn tất) | [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69) |
| [AD-ST-07](03_de_stream.md#ad-st-07) | Phát đủ tập train, phát lại và đối soát phần stream | P3 | G3 | Bắt đầu: AD-ST-05; Hoàn tất: AD-ST-06 | AD-DE-17 (hoàn tất), AD-DE-27 (hoàn tất) | [#70](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/70) |
| [AD-ST-08](03_de_stream.md#ad-st-08) | Kiểm đổi eh_generation khi Event Hubs được tạo lại | P3 | G2 | Bắt đầu: AD-ST-04; Hoàn tất: AD-OPS-24, AD-OPS-05 | Không | [#71](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/71) |
| [AD-ST-09](03_de_stream.md#ad-st-09) | Đo độ trễ Event Hubs khác region và cùng region | P3 | G4 | Bắt đầu: AD-ST-06; Hoàn tất: AD-OPS-24 | AD-BI-07 (hoàn tất), AD-RP-03 (bắt đầu) | [#72](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/72) |
| [AD-ST-10](03_de_stream.md#ad-st-10) | Đẩy KPI stream lên push semantic model của Power BI | P3 | G4 | Bắt đầu: AD-ST-06; Hoàn tất: AD-BI-08, AD-OPS-05 | AD-BI-12 (bắt đầu) | [#73](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/73) |
| [AD-BI-01](04_power_bi.md#ad-bi-01) | Spike Power BI: giấy phép Pro, workspace instacart-bi, connector, push model, APR, quyền API | P3 | G0 | Chuẩn bị ngay; Hoàn tất: AD-OPS-04 | AD-OPS-07 (hoàn tất), AD-DE-25 (hoàn tất), AD-BI-02 (hoàn tất), AD-BI-08 (hoàn tất) | [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74) |
| [AD-BI-02](04_power_bi.md#ad-bi-02) | Kết nối Power BI với warehouse và tạo dự án PBIP | P3 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-23, AD-OPS-22, AD-OPS-20, AD-BI-01 | AD-BI-03 (bắt đầu) | [#75](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/75) |
| [AD-BI-03](04_power_bi.md#ad-bi-03) | Dựng star schema và measure DAX | P3 | G4 | Bắt đầu: AD-BI-02; Hoàn tất: AD-DE-24 | AD-BI-04 (bắt đầu), AD-BI-05 (bắt đầu), AD-BI-06 (bắt đầu), AD-BI-07 (bắt đầu), AD-BI-11 (bắt đầu) | [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76) |
| [AD-BI-04](04_power_bi.md#ad-bi-04) | Dựng trang 1 Tổng quan và trang 3 thời gian mua | P3 | G4 | Bắt đầu: AD-BI-03 | AD-BI-05 (hoàn tất), AD-BI-08 (hoàn tất), AD-BI-09 (bắt đầu) | [#77](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77) |
| [AD-BI-05](04_power_bi.md#ad-bi-05) | Dựng trang 2 chi tiết sản phẩm, trang 4 danh mục và điều hướng | P3 | G4 | Bắt đầu: AD-BI-03; Hoàn tất: AD-BI-04 | AD-BI-09 (bắt đầu), AD-BI-10 (bắt đầu) | [#78](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/78) |
| [AD-BI-06](04_power_bi.md#ad-bi-06) | Cài RLS DeptManager và Executive từ bi_user_access | P3 | G4 | Bắt đầu: AD-BI-03; Hoàn tất: AD-DE-23 | AD-BI-09 (bắt đầu) | [#79](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/79) |
| [AD-BI-07](04_power_bi.md#ad-bi-07) | Dựng trang 5 vận hành và real-time | P3 | G4 | Bắt đầu: AD-BI-03; Hoàn tất: AD-DE-16, AD-DE-17, AD-ST-06, AD-ST-09 | AD-BI-09 (hoàn tất), AD-BI-12 (bắt đầu) | [#80](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/80) |
| [AD-BI-08](04_power_bi.md#ad-bi-08) | Tạo push semantic model và dashboard Instacart Live | P3 | G4 | Chuẩn bị ngay; Hoàn tất: AD-BI-04, AD-BI-01 | AD-ST-10 (hoàn tất), AD-BI-12 (bắt đầu) | [#81](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/81) |
| [AD-BI-09](04_power_bi.md#ad-bi-09) | Publish báo cáo, tạo App và lịch refresh | P3 | G4 | Bắt đầu: AD-BI-04, AD-BI-05, AD-BI-06; Hoàn tất: AD-BI-07 | AD-DE-25 (hoàn tất), AD-BI-10 (hoàn tất), AD-BI-12 (bắt đầu), AD-DS-25 (bắt đầu) | [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82) |
| [AD-BI-10](04_power_bi.md#ad-bi-10) | Dựng mobile layout cho các trang và chụp ảnh trên điện thoại | P3 | G4 | Bắt đầu: AD-BI-05; Hoàn tất: AD-BI-09 | AD-BI-12 (bắt đầu) | [#83](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/83) |
| [AD-BI-11](04_power_bi.md#ad-bi-11) | Mở rộng semantic model cho view DS (khung trang 6–7) | P3 | G5 | Bắt đầu: AD-BI-03; Hoàn tất: AD-DS-22 | AD-DS-25 (bắt đầu) | [#84](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/84) |
| [AD-BI-12](04_power_bi.md#ad-bi-12) | Nghiệm thu checklist G4 và diễn tập demo 95/5 | P3 | G4 | Bắt đầu: AD-BI-07, AD-BI-08, AD-BI-09, AD-BI-10, AD-ST-10; Hoàn tất: AD-DE-26 | AD-RP-04 (bắt đầu), AD-RP-08 (bắt đầu) | [#85](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/85) |
| [AD-DS-01](05_data_science.md#ad-ds-01) | Cài apriori_gen, DApriori và DATID trên RDD kèm test vét cạn | P4 | G0 | Chuẩn bị ngay | AD-DS-02 (hoàn tất), AD-DS-10 (bắt đầu) | [#86](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/86) |
| [AD-DS-02](05_data_science.md#ad-ds-02) | Cài sinh luật gen_rules và bọc FP-Growth MLlib, test so khớp với MLlib | P4 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DS-01 | AD-DS-10 (bắt đầu) | [#87](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/87) |
| [AD-DS-03](05_data_science.md#ad-ds-03) | Viết hàm đọc dữ liệu thực nghiệm data.py và bộ đo measure.py có test local | P4 | G0 | Chuẩn bị ngay | AD-DS-10 (bắt đầu) | [#88](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/88) |
| [AD-DS-04](05_data_science.md#ad-ds-04) | Dựng cụm Hadoop HDFS + YARN + Spark on YARN bằng Docker ở local | P4 | G0 | Chuẩn bị ngay | AD-DS-06 (bắt đầu), AD-DS-07 (bắt đầu), AD-DS-17 (bắt đầu) | [#89](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/89) |
| [AD-DS-05](05_data_science.md#ad-ds-05) | Viết chương trình MapReduce Java ItemCount và PairCount kèm test MiniMRYarnCluster | P4 | G0 | Chuẩn bị ngay | AD-DS-06 (hoàn tất) | [#90](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/90) |
| [AD-DS-06](05_data_science.md#ad-ds-06) | Viết đếm cặp Spark on YARN bản DataFrame và RDD, đối chiếu với MapReduce | P4 | G0 | Bắt đầu: AD-DS-04; Hoàn tất: AD-DS-05 | AD-DS-08 (bắt đầu) | [#91](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/91) |
| [AD-DS-07](05_data_science.md#ad-ds-07) | Viết script tải dữ liệu export về máy và nạp lên HDFS local | P4 | G5 | Bắt đầu: AD-DS-04; Hoàn tất: AD-DE-21 | AD-DS-15 (bắt đầu) | [#92](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/92) |
| [AD-DS-08](05_data_science.md#ad-ds-08) | Viết bench.py chạy lưới E6 và script upload kết quả lên ds/inbox | P4 | G5 | Bắt đầu: AD-DS-06 | AD-DS-12 (hoàn tất), AD-DS-15 (bắt đầu) | [#93](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/93) |
| [AD-DS-09](05_data_science.md#ad-ds-09) | Định nghĩa lưới experiments.yml và viết plan_grid.py bung tổ hợp theo run_key | P4 | G5 | Chuẩn bị ngay | AD-DS-10 (bắt đầu), AD-DS-11 (hoàn tất) | [#94](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/94) |
| [AD-DS-10](05_data_science.md#ad-ds-10) | Viết run_experiment.py chạy một tổ hợp, ghi ml.experiment_runs và MLflow | P4 | G5 | Bắt đầu: AD-DS-01, AD-DS-02, AD-DS-03, AD-DS-09 | AD-DS-12 (hoàn tất) | [#95](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/95) |
| [AD-DS-11](05_data_science.md#ad-ds-11) | Khám phá dữ liệu ds_data v1 và chốt lưới MinSupport theo cấp item | P4 | G3 | Bắt đầu: AD-DE-21; Hoàn tất: AD-DS-09 | AD-DS-13 (bắt đầu) | [#96](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/96) |
| [AD-DS-12](05_data_science.md#ad-ds-12) | Khai báo job ds-benchmark trong DAB và viết ingest_local.sql | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-08, AD-DS-10, AD-OPS-30, AD-OPS-22 | AD-DS-13 (bắt đầu), AD-DS-15 (hoàn tất) | [#97](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/97) |
| [AD-DS-13](05_data_science.md#ad-ds-13) | Chạy spike DS-1 và thực nghiệm E1 kiểm đúng đắn trên dữ liệu thật | P4 | G5 | Bắt đầu: AD-DS-11, AD-DS-12 | AD-DS-14 (bắt đầu), AD-DS-20 (bắt đầu) | [#98](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/98) |
| [AD-DS-14](05_data_science.md#ad-ds-14) | Chạy toàn bộ lưới E2–E5 trên Databricks | P4 | G5 | Bắt đầu: AD-DS-13 | AD-DS-22 (hoàn tất), AD-DS-26 (bắt đầu) | [#99](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/99) |
| [AD-DS-15](05_data_science.md#ad-ds-15) | Chạy lưới E6 MapReduce và Spark on YARN ở local, nạp kết quả lên ml.experiment_runs | P4 | G5 | Bắt đầu: AD-DS-07, AD-DS-08; Hoàn tất: AD-DS-12 | AD-DS-16 (bắt đầu), AD-DS-22 (hoàn tất), AD-DS-26 (bắt đầu) | [#100](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/100) |
| [AD-DS-16](05_data_science.md#ad-ds-16) | Cài AprioriMR lặp k ≤ 3 trên MapReduce và DApriori trên YARN (mở rộng) | P4 | G5 | Bắt đầu: AD-DS-15 | Không | [#101](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/101) |
| [AD-DS-17](05_data_science.md#ad-ds-17) | Dựng demo pipeline HDFS + Spark dựng giỏ prior từ CSV gốc (tùy chọn) | P4 | G5 | Bắt đầu: AD-DS-04; Hoàn tất: AD-DE-17 | Không | [#102](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/102) |
| [AD-DS-18](05_data_science.md#ad-ds-18) | Viết đánh giá luật trên holdout evaluate_holdout.py và baseline phổ biến | P4 | G5 | Chuẩn bị ngay | AD-DS-19 (hoàn tất), AD-DS-20 (bắt đầu) | [#103](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/103) |
| [AD-DS-19](05_data_science.md#ad-ds-19) | Viết model pyfunc BasketRules gợi ý món từ giỏ hàng kèm test | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-18 | AD-DS-21 (bắt đầu) | [#104](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/104) |
| [AD-DS-20](05_data_science.md#ad-ds-20) | Chạy thực nghiệm E7 và đánh giá luật trên holdout val/test | P4 | G5 | Bắt đầu: AD-DS-13, AD-DS-18 | AD-DS-21 (bắt đầu), AD-DS-26 (bắt đầu) | [#105](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/105) |
| [AD-DS-21](05_data_science.md#ad-ds-21) | Chọn champion, đăng ký model basket_rules vào Unity Catalog và khai báo job ds-champion | P4 | G5 | Bắt đầu: AD-DS-19, AD-DS-20 | AD-DS-22 (hoàn tất), AD-DS-23 (bắt đầu), AD-DS-24 (hoàn tất), AD-DS-26 (hoàn tất) | [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106) |
| [AD-DS-22](05_data_science.md#ad-ds-22) | Viết view gold cho DS: association_rules, recommendations, experiment_metrics, holdout_metrics | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-14, AD-DS-15, AD-DS-21, AD-OPS-20 | AD-BI-11 (hoàn tất), AD-DS-25 (bắt đầu) | [#107](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/107) |
| [AD-DS-23](05_data_science.md#ad-ds-23) | Khai báo Model Serving endpoint instacart-basket-rules trong DAB và kiểm API | P4 | G5 | Bắt đầu: AD-DS-21, AD-OPS-18 | AD-OPS-31 (hoàn tất), AD-DS-24 (hoàn tất), AD-DS-27 (hoàn tất) | [#108](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/108) |
| [AD-DS-24](05_data_science.md#ad-ds-24) | Viết Azure Function recommend gọi endpoint, có phương án B tự tra luật | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-21, AD-DS-23, AD-OPS-31 | AD-OPS-32 (hoàn tất), AD-DS-27 (hoàn tất) | [#109](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/109) |
| [AD-DS-25](05_data_science.md#ad-ds-25) | Làm Power BI trang 6 luật kết hợp và trang 7 thực nghiệm thuật toán | P4 | G5 | Bắt đầu: AD-DS-22, AD-BI-11, AD-BI-09 | AD-DS-27 (bắt đầu) | [#110](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/110) |
| [AD-DS-26](05_data_science.md#ad-ds-26) | Phân tích kết quả thực nghiệm và viết phần DS của báo cáo, đối chiếu bài báo | P4 | G5 | Bắt đầu: AD-DS-14, AD-DS-15, AD-DS-20; Hoàn tất: AD-DS-21 | AD-DS-27 (bắt đầu), AD-RP-06 (bắt đầu) | [#111](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/111) |
| [AD-DS-27](05_data_science.md#ad-ds-27) | Đối chiếu checklist G5 và chốt bằng chứng phần DS | P4 | G5 | Bắt đầu: AD-DS-25, AD-DS-26; Hoàn tất: AD-DS-23, AD-DS-24 | AD-RP-06 (hoàn tất), AD-RP-08 (bắt đầu) | [#112](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/112) |
| [AD-RP-01](06_bao_cao_demo.md#ad-rp-01) | Lập khung báo cáo cuối kỳ và bảng truy vết rubric → bằng chứng | P3 | G0 | Chuẩn bị ngay | AD-RP-02 (bắt đầu), AD-RP-07 (bắt đầu) | [#113](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/113) |
| [AD-RP-02](06_bao_cao_demo.md#ad-rp-02) | Viết chương mở đầu, dữ liệu và kiến trúc của báo cáo | P3 | G1 | Bắt đầu: AD-RP-01; Hoàn tất: AD-OPS-07 | AD-RP-06 (bắt đầu) | [#114](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/114) |
| [AD-RP-03](06_bao_cao_demo.md#ad-rp-03) | Viết chương ETL batch, streaming và bằng chứng rubric mục 3 | P2 | G4 | Bắt đầu: AD-DE-26, AD-ST-09; Hoàn tất: AD-DE-27, AD-OPS-33 | AD-RP-06 (bắt đầu) | [#115](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/115) |
| [AD-RP-04](06_bao_cao_demo.md#ad-rp-04) | Viết chương trực quan dữ liệu (rubric mục 4) với ảnh dashboard | P3 | G4 | Bắt đầu: AD-BI-12 | AD-RP-06 (bắt đầu) | [#116](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/116) |
| [AD-RP-05](06_bao_cao_demo.md#ad-rp-05) | Viết chương quản lý dự án trên cloud (rubric mục 5.4) | P1 | G4 | Bắt đầu: AD-OPS-33 | AD-RP-06 (bắt đầu) | [#117](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/117) |
| [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) | Ghép báo cáo hoàn chỉnh, thống nhất hình bảng và trích dẫn | P3 | G5 | Bắt đầu: AD-RP-02, AD-RP-03, AD-RP-04, AD-RP-05, AD-DS-26; Hoàn tất: AD-DS-27 | AD-RP-07 (hoàn tất), AD-RP-10 (bắt đầu) | [#118](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/118) |
| [AD-RP-07](06_bao_cao_demo.md#ad-rp-07) | Soạn slide thuyết trình | P2 | G5 | Bắt đầu: AD-RP-01; Hoàn tất: AD-RP-06 | AD-RP-09 (bắt đầu) | [#119](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/119) |
| [AD-RP-08](06_bao_cao_demo.md#ad-rp-08) | Viết kịch bản demo đầu cuối và quay video dự phòng | P1 | G5 | Bắt đầu: AD-BI-12, AD-DS-27 | AD-RP-09 (bắt đầu) | [#120](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/120) |
| [AD-RP-09](06_bao_cao_demo.md#ad-rp-09) | Diễn tập thuyết trình và demo toàn nhóm, sửa lỗi phát hiện | P2 | G5 | Bắt đầu: AD-RP-07, AD-RP-08 | AD-OPS-34 (hoàn tất), AD-RP-10 (bắt đầu) | [#121](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/121) |
| [AD-RP-10](06_bao_cao_demo.md#ad-rp-10) | Đóng gói nộp bài: release, hướng dẫn tái lập, kiểm link | P1 | G5 | Bắt đầu: AD-RP-06, AD-RP-09 | AD-OPS-34 (hoàn tất) | [#122](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/122) |
