# ETL Azure Databricks

## Thành viên

| STT | Họ và tên | Mã số sinh viên |
|---:|---|---:|
| 1 | Phạm Hoàng Vinh | 23521793 |
| 2 | Trần Minh Hoài Tâm | 23521394 |
| 3 | Nguyễn Minh Tuấn | 23521720 |
| 4 | Nguyễn Quốc Trung | 23521680 |

**Số lượng thành viên:** 4

## Đề tài tạm thời

**Xây dựng quy trình ETL dữ liệu trên Azure Databricks.**

Project tập trung vào việc xây dựng quy trình trích xuất, biến đổi và nạp dữ liệu (ETL) trên nền tảng Microsoft Azure, sử dụng Azure Databricks làm môi trường xử lý dữ liệu.

## Mục tiêu dự kiến

- Thu thập dữ liệu từ các nguồn đầu vào phù hợp.
- Làm sạch, chuẩn hóa và biến đổi dữ liệu bằng các bước ETL.
- Lưu trữ dữ liệu theo từng lớp để thuận tiện cho việc quản lý và khai thác.
- Tạo nền tảng dữ liệu phục vụ phân tích và trực quan hóa.
- Đánh giá tính đúng đắn, hiệu năng và khả năng mở rộng của quy trình.

## Công nghệ dự kiến

- Microsoft Azure
- Azure Databricks
- Apache Spark / PySpark
- Azure Data Lake Storage
- SQL

## Quy trình tổng quát

```text
Nguồn dữ liệu → Trích xuất → Làm sạch & biến đổi → Lưu trữ → Phân tích
```

## Trạng thái

Đề tài đang ở giai đoạn đề xuất và sẽ được cập nhật thêm khi nhóm thống nhất phạm vi, nguồn dữ liệu và yêu cầu triển khai.

## Cấu trúc repo

| Thư mục | Nội dung | Kế hoạch |
|---|---|---|
| `docs/` | Kiến trúc, data dictionary, kế hoạch DevOps, DE, DS | [tổng quan dự án](docs/00_tong_quan_du_an.md) · [devops](docs/devops/00_tong_quan.md) · [de](docs/de/00_tong_quan_de.md) · [ds](docs/ds/00_tong_quan_ds.md) · [phân việc](docs/tasks/README.md) |
| `infra/` | Terraform: `bootstrap/`, `modules/`, `stacks/` (10-core, 20-stream, 30-databricks, 40-orchestration, 90-sandbox), `envs/` | [infra/README.md](infra/README.md) |
| `.github/` | Workflow CI/CD, CODEOWNERS, PR template | docs/devops/04 |
| `bundles/` | Databricks Asset Bundles (job, task) | docs/devops/05, mục 4 |
| `src/` | Code PySpark: `common/`, `batch/`, `streaming/`, `ml/` (DApriori, DATID, FP-Growth, thực nghiệm); `mapreduce/` (Java) | docs/de/00 §6, docs/ds/00 §8 |
| `local/hadoop/` | Cụm Hadoop HDFS + YARN + Spark bằng Docker, so sánh MapReduce và Spark | docs/ds/03 |
| `tests/` | pytest với PySpark local | — |
| `adf/` | JSON pipeline ADF, được stack 40-orchestration publish | docs/devops/06, mục 1 |
| `powerbi/` | Dự án Power BI dạng PBIP | docs/devops/06, mục 5 |

Thiết lập máy (Python 3.11 qua uv):

```bash
uv sync && uv run pytest -q
```

```bash
brew install pre-commit tflint && pre-commit install
```

## Ghi chú

Nội dung README sẽ tiếp tục được bổ sung với các thông tin như kiến trúc hệ thống, hướng dẫn cài đặt, cách chạy pipeline, cấu trúc thư mục và kết quả thực nghiệm.
