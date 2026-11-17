# Phân việc · Báo cáo, bằng chứng và demo (`AD-RP-xx`)

10 task. Quy ước, trạng thái và bảng tổng ở [README](README.md). Vai ghi theo P1–P4, người thật được gán trên GitHub Project.

| Mã | Việc | Vai | Cổng | Phụ thuộc | Issue |
|---|---|---|---|---|---|
| [AD-RP-01](#ad-rp-01) | Lập khung báo cáo cuối kỳ và bảng truy vết rubric → bằng chứng | P3 | G0 | Chuẩn bị ngay | [#113](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/113) |
| [AD-RP-02](#ad-rp-02) | Viết chương mở đầu, dữ liệu và kiến trúc của báo cáo | P3 | G1 | Bắt đầu: AD-RP-01; Hoàn tất: AD-OPS-07 | [#114](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/114) |
| [AD-RP-03](#ad-rp-03) | Viết chương ETL batch, streaming và bằng chứng rubric mục 3 | P2 | G4 | Bắt đầu: AD-DE-26, AD-ST-09; Hoàn tất: AD-DE-27, AD-OPS-33 | [#115](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/115) |
| [AD-RP-04](#ad-rp-04) | Viết chương trực quan dữ liệu (rubric mục 4) với ảnh dashboard | P3 | G4 | Bắt đầu: AD-BI-12 | [#116](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/116) |
| [AD-RP-05](#ad-rp-05) | Viết chương quản lý dự án trên cloud (rubric mục 5.4) | P1 | G4 | Bắt đầu: AD-OPS-33 | [#117](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/117) |
| [AD-RP-06](#ad-rp-06) | Ghép báo cáo hoàn chỉnh, thống nhất hình bảng và trích dẫn | P3 | G5 | Bắt đầu: AD-RP-02, AD-RP-03, AD-RP-04, AD-RP-05, AD-DS-26; Hoàn tất: AD-DS-27 | [#118](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/118) |
| [AD-RP-07](#ad-rp-07) | Soạn slide thuyết trình | P2 | G5 | Bắt đầu: AD-RP-01; Hoàn tất: AD-RP-06 | [#119](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/119) |
| [AD-RP-08](#ad-rp-08) | Viết kịch bản demo đầu cuối và quay video dự phòng | P1 | G5 | Bắt đầu: AD-BI-12, AD-DS-27 | [#120](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/120) |
| [AD-RP-09](#ad-rp-09) | Diễn tập thuyết trình và demo toàn nhóm, sửa lỗi phát hiện | P2 | G5 | Bắt đầu: AD-RP-07, AD-RP-08 | [#121](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/121) |
| [AD-RP-10](#ad-rp-10) | Đóng gói nộp bài: release, hướng dẫn tái lập, kiểm link | P1 | G5 | Bắt đầu: AD-RP-06, AD-RP-09 | [#122](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/122) |

### AD-RP-01

**Lập khung báo cáo cuối kỳ và bảng truy vết rubric → bằng chứng**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G0**
- Issue: [#113](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/113)
- Tài liệu: `docs/00_tong_quan_du_an.md §1.4, §6`, `docs/devops/00_tong_quan.md §1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-RP-02](06_bao_cao_demo.md#ad-rp-02) (bắt đầu), [AD-RP-07](06_bao_cao_demo.md#ad-rp-07) (bắt đầu)

Báo cáo được chấm theo rubric. Có khung và bảng truy vết sớm thì mỗi task biết phải để lại bằng chứng gì, cuối kỳ không phải đi tìm lại.

**Việc cần làm**

1. Liệt kê các mục rubric nhóm nhắm tới: 1.4, 3.1–3.4, 4 (phân tích + trực quan), 5.4.
2. Với từng mục, ghi task nào tạo ra bằng chứng, dạng bằng chứng (ảnh, bảng, số liệu, link PR) và nơi lưu.
3. Dựng mục lục báo cáo và slide; chốt quy ước đặt tên ảnh, bảng, nơi lưu ảnh.

**Đầu ra**

- docs/report/00_khung_bao_cao.md: mục lục báo cáo và slide
- docs/report/01_truy_vet_rubric.md: bảng mục rubric → task → bằng chứng → đường dẫn
- Thư mục docs/report/img/ cho ảnh bằng chứng

**Xong khi**

- [ ] Mọi mục rubric nhóm nhắm tới đều có ít nhất một task và một dạng bằng chứng
- [ ] Cả nhóm đã xem và đồng ý khung (ghi trong PR)

### AD-RP-02

**Viết chương mở đầu, dữ liệu và kiến trúc của báo cáo**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G1**
- Issue: [#114](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/114)
- Tài liệu: `docs/00_tong_quan_du_an.md`, `docs/architecture/*.png`, `docs/data/data_dictionary.md`
- **Bắt đầu sau:** [AD-RP-01](06_bao_cao_demo.md#ad-rp-01) [#113](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/113)
- **Hoàn tất còn cần:** [AD-OPS-07](01_devops.md#ad-ops-07) [#9](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/9)
- **Mở khóa:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) (bắt đầu)

Phần này dựa trên tài liệu đã có nên viết được sớm. Chỉ cần cập nhật lại theo quyết định G0 (region, lớp serving, phương án API).

**Việc cần làm**

1. Viết bài toán, đề tài, bài báo nền tảng và ý nghĩa (tóm từ tài liệu tổng quan).
2. Mô tả 3 nguồn dữ liệu, số dòng chính, sơ đồ schema nguồn.
3. Trình bày kiến trúc bằng hình 1–3, bảng thành phần theo lớp và bảng phân loại IaaS/PaaS/FaaS/SaaS (rubric 1.4).
4. Cập nhật theo các quyết định đã chốt ở G0.

**Đầu ra**

- docs/report/10_mo_dau_du_lieu_kien_truc.md (hoặc chương tương ứng trong file báo cáo chung)

**Xong khi**

- [ ] Chương có đủ: bài toán, dữ liệu, kiến trúc, phân loại dịch vụ cloud
- [ ] Mọi hình dùng bản PNG từ draw.io, có dòng File gốc
- [ ] Nội dung khớp quyết định G0 trong docs/devops/spike_results.md

### AD-RP-03

**Viết chương ETL batch, streaming và bằng chứng rubric mục 3**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#115](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/115)
- Tài liệu: `docs/de/00_tong_quan_de.md`, `docs/devops/06_de_den_dashboard.md §6.4`
- **Bắt đầu sau:** [AD-DE-26](02_de_batch.md#ad-de-26) [#62](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/62), [AD-ST-09](03_de_stream.md#ad-st-09) [#72](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/72)
- **Hoàn tất còn cần:** [AD-DE-27](02_de_batch.md#ad-de-27) [#63](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/63), [AD-OPS-33](01_devops.md#ad-ops-33) [#35](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/35)
- **Mở khóa:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) (bắt đầu)

Rubric mục 3 cần số đo thật: tốc độ đọc ghi, ETL tự động, độ trễ khác region, tối ưu lưu trữ. Chương này gom số đo từ các task DE, stream và DevOps.

**Việc cần làm**

1. Mô tả luồng batch (ADF → LDP → gold) và stream (simulator → Event Hubs → LDP), CDC/SCD2, expectations, chạy lại an toàn.
2. 3.1: bảng và biểu đồ benchmark CSV, Parquet, Delta, Delta OPTIMIZE từ ops.benchmark_results.
3. 3.2: ảnh lịch ADF, lần chạy job, ops.pipeline_runs.
4. 3.3: bảng độ trễ cùng region và khác region từ ops.streaming_latency, có p50/p95.
5. 3.4: lifecycle landing sang cool, OPTIMIZE, liquid clustering, VACUUM.
6. Tóm runbook chạy lại và kết quả diễn tập lỗi.

**Đầu ra**

- docs/report/20_etl_va_hieu_nang.md
- Ảnh và biểu đồ trong docs/report/img/

**Xong khi**

- [ ] Mỗi mục 3.1–3.4 có số liệu thật kèm truy vấn hoặc ảnh nguồn
- [ ] Số liệu khớp bảng ops.* tại thời điểm chụp (ghi ngày đo)

### AD-RP-04

**Viết chương trực quan dữ liệu (rubric mục 4) với ảnh dashboard**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#116](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/116)
- Tài liệu: `docs/de/07_power_bi.md`, `docs/devops/06_de_den_dashboard.md §5`
- **Bắt đầu sau:** [AD-BI-12](04_power_bi.md#ad-bi-12) [#85](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/85)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) (bắt đầu)

Rubric mục 4 là trọng tâm. Chương này trình bày dashboard theo đúng các ý rubric: SaaS, nhiều report và điều hướng, cập nhật tức thời, bảo mật/RLS, mobile.

**Việc cần làm**

1. Chụp từng trang 1–5 kèm câu hỏi kinh doanh mà trang trả lời và insight chính.
2. Trình bày điều hướng, RLS (DeptManager, Executive), chia sẻ qua App.
3. Trình bày tile real-time và cách đo độ trễ hiển thị.
4. Chụp bố cục mobile trên điện thoại.

**Đầu ra**

- docs/report/30_truc_quan.md
- Ảnh dashboard desktop và mobile trong docs/report/img/

**Xong khi**

- [ ] Đủ ảnh 5 trang desktop và mobile, ảnh RLS khi đăng nhập 2 vai khác nhau, ảnh tile real-time
- [ ] Mỗi trang có ít nhất một insight bằng số liệu

### AD-RP-05

**Viết chương quản lý dự án trên cloud (rubric mục 5.4)**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G4**
- Issue: [#117](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/117)
- Tài liệu: `docs/devops/04_github_actions_cicd.md`, `docs/devops/02_bootstrap_tfstate.md`
- **Bắt đầu sau:** [AD-OPS-33](01_devops.md#ad-ops-33) [#35](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/35)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) (bắt đầu)

Rubric 5.4 chấm cách nhiều người cùng làm trên một resource, dùng GitHub, kiểm soát chi phí và quota.

**Việc cần làm**

1. Mô tả phân vai, RBAC 4 thành viên, quy trình PR + review + CI, OIDC không giữ secret.
2. Đưa ảnh GitHub Project (board, Theo người), Insights → Contributors, lịch sử PR.
3. Đưa bảng chi phí theo tag owner/stack, budget alert, ảnh quota vCPU theo tuần.
4. Nêu các quyết định để vừa 4 vCPU và 100 USD (single-node, serverless warehouse, tắt Event Hubs ban đêm…).

**Đầu ra**

- docs/report/40_quan_ly_du_an.md
- Ảnh board, Insights, Cost Management, quota trong docs/report/img/

**Xong khi**

- [ ] Có số chi phí thật theo tag và ảnh quota, ghi ngày lấy số
- [ ] Có link tới ít nhất 3 PR tiêu biểu (infra, DE, DS) có review và CI

### AD-RP-06

**Ghép báo cáo hoàn chỉnh, thống nhất hình bảng và trích dẫn**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G5**
- Issue: [#118](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/118)
- Tài liệu: `docs/report/00_khung_bao_cao.md`
- **Bắt đầu sau:** [AD-RP-02](06_bao_cao_demo.md#ad-rp-02) [#114](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/114), [AD-RP-03](06_bao_cao_demo.md#ad-rp-03) [#115](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/115), [AD-RP-04](06_bao_cao_demo.md#ad-rp-04) [#116](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/116), [AD-RP-05](06_bao_cao_demo.md#ad-rp-05) [#117](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/117), [AD-DS-26](05_data_science.md#ad-ds-26) [#111](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/111)
- **Hoàn tất còn cần:** [AD-DS-27](05_data_science.md#ad-ds-27) [#112](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/112)
- **Mở khóa:** [AD-RP-07](06_bao_cao_demo.md#ad-rp-07) (hoàn tất), [AD-RP-10](06_bao_cao_demo.md#ad-rp-10) (bắt đầu)

Gom các chương do nhiều người viết thành một báo cáo thống nhất để nộp.

**Việc cần làm**

1. Ghép các chương theo khung; đánh số hình, bảng; thống nhất thuật ngữ.
2. Thêm kết luận, hạn chế, hướng phát triển.
3. Trích dẫn bài báo Fernandez-Basso et al. (2024) và các nguồn dữ liệu đúng quy cách.
4. Đối chiếu bảng truy vết rubric: mục nào thiếu bằng chứng thì báo task nguồn.
5. Xuất bản nộp (PDF hoặc định dạng giảng viên yêu cầu).

**Đầu ra**

- File báo cáo cuối (PDF) và nguồn trong docs/report/
- docs/report/01_truy_vet_rubric.md cập nhật trạng thái đủ/thiếu

**Xong khi**

- [ ] Bảng truy vết rubric không còn mục thiếu bằng chứng
- [ ] Cả 4 người đã đọc và duyệt bản cuối (ghi trong PR)

### AD-RP-07

**Soạn slide thuyết trình**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G5**
- Issue: [#119](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/119)
- Tài liệu: `docs/report/00_khung_bao_cao.md`
- **Bắt đầu sau:** [AD-RP-01](06_bao_cao_demo.md#ad-rp-01) [#113](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/113)
- **Hoàn tất còn cần:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) [#118](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/118)
- **Mở khóa:** [AD-RP-09](06_bao_cao_demo.md#ad-rp-09) (bắt đầu)

Slide làm song song với báo cáo, dùng lại hình và số liệu của báo cáo.

**Việc cần làm**

1. Dựng slide theo khung: bài toán, kiến trúc, ETL, dashboard, DS, quản lý dự án, kết luận.
2. Đặt chỗ cho demo trực tiếp và video dự phòng.
3. Chia phần nói cho từng người, căn thời lượng.

**Đầu ra**

- File slide trong docs/report/ (pptx hoặc pdf)

**Xong khi**

- [ ] Slide dùng số liệu khớp báo cáo bản cuối
- [ ] Có phân công người nói và thời lượng từng phần

### AD-RP-08

**Viết kịch bản demo đầu cuối và quay video dự phòng**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G5**
- Issue: [#120](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/120)
- Tài liệu: `docs/devops/06_de_den_dashboard.md §6.4`, `docs/ds/05_mlflow_serving_bao_cao.md`
- **Bắt đầu sau:** [AD-BI-12](04_power_bi.md#ad-bi-12) [#85](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/85), [AD-DS-27](05_data_science.md#ad-ds-27) [#112](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/112)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-RP-09](06_bao_cao_demo.md#ad-rp-09) (bắt đầu)

Demo trực tiếp trên cloud dễ trục trặc (quota, Event Hubs đã tắt, token hết hạn). Cần kịch bản chuẩn và video dự phòng.

**Việc cần làm**

1. Viết kịch bản: bật stream-toggle, chạy simulator, xem tile real-time; kích hoạt pl_master; mở dashboard và RLS; gọi API gợi ý.
2. Ghi checklist chuẩn bị trước buổi demo (bật tài nguyên, warm-up warehouse, endpoint, token).
3. Quay video dự phòng toàn bộ kịch bản.

**Đầu ra**

- docs/report/demo_runbook.md
- Video demo dự phòng (link lưu trữ ghi trong runbook)

**Xong khi**

- [ ] Chạy trọn kịch bản 1 lần không lỗi theo runbook
- [ ] Video phát được, thời lượng khớp phần demo trong slide

### AD-RP-09

**Diễn tập thuyết trình và demo toàn nhóm, sửa lỗi phát hiện**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G5**
- Issue: [#121](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/121)
- Tài liệu: `docs/report/demo_runbook.md`
- **Bắt đầu sau:** [AD-RP-07](06_bao_cao_demo.md#ad-rp-07) [#119](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/119), [AD-RP-08](06_bao_cao_demo.md#ad-rp-08) [#120](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/120)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-34](01_devops.md#ad-ops-34) (hoàn tất), [AD-RP-10](06_bao_cao_demo.md#ad-rp-10) (bắt đầu)

Diễn tập ít nhất một lần trước buổi bảo vệ để căn giờ và phát hiện lỗi.

**Việc cần làm**

1. Diễn tập đầy đủ: thuyết trình + demo theo runbook, bấm giờ.
2. Ghi lỗi và câu hỏi phản biện dự kiến; giao lại cho task nguồn nếu cần sửa.
3. Chốt bản slide và runbook cuối.

**Đầu ra**

- docs/report/dien_tap.md: thời lượng, lỗi, câu hỏi dự kiến và cách trả lời

**Xong khi**

- [ ] Tổng thời lượng nằm trong giới hạn
- [ ] Mọi lỗi phát hiện đã sửa hoặc có phương án dự phòng

### AD-RP-10

**Đóng gói nộp bài: release, hướng dẫn tái lập, kiểm link**

- Vai dự kiến: **P1 · DevOps**
- Cổng: **G5**
- Issue: [#122](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/122)
- Tài liệu: `README.md`, `CONTRIBUTING.md`
- **Bắt đầu sau:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) [#118](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/118), [AD-RP-09](06_bao_cao_demo.md#ad-rp-09) [#121](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/121)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-34](01_devops.md#ad-ops-34) (hoàn tất)

Bước cuối trước khi nộp và trước khi hủy hạ tầng.

**Việc cần làm**

1. Cập nhật README: hướng dẫn tái lập từ đầu theo thứ tự stack và bundle.
2. Chạy script kiểm link nội bộ toàn bộ docs/.
3. Tạo tag/release đánh dấu bản nộp, đính kèm báo cáo và slide.
4. Đóng các issue đã xong; issue nào bỏ thì ghi lý do.

**Đầu ra**

- Release trên GitHub kèm báo cáo, slide
- README cập nhật

**Xong khi**

- [ ] Không còn link hỏng trong docs/
- [ ] Release có đủ file nộp; board không còn task Todo/In Progress ngoài AD-OPS-34
