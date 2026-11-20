# Phân việc · Power BI (`AD-BI-xx`)

12 task. Quy ước, trạng thái và bảng tổng ở [README](README.md). Vai ghi theo P1–P4, người thật được gán trên GitHub Project.

| Mã | Việc | Vai | Cổng | Phụ thuộc | Issue |
|---|---|---|---|---|---|
| [AD-BI-01](#ad-bi-01) | Spike Power BI: giấy phép Pro, workspace instacart-bi, connector, push model, APR, quyền API | P3 | G0 | Chuẩn bị ngay; Hoàn tất: AD-OPS-04 | [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74) |
| [AD-BI-02](#ad-bi-02) | Kết nối Power BI với warehouse và tạo dự án PBIP | P3 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-23, AD-OPS-22, AD-OPS-20, AD-BI-01 | [#75](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/75) |
| [AD-BI-03](#ad-bi-03) | Dựng star schema và measure DAX | P3 | G4 | Bắt đầu: AD-BI-02; Hoàn tất: AD-DE-24 | [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76) |
| [AD-BI-04](#ad-bi-04) | Dựng trang 1 Tổng quan và trang 3 thời gian mua | P3 | G4 | Bắt đầu: AD-BI-03 | [#77](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77) |
| [AD-BI-05](#ad-bi-05) | Dựng trang 2 chi tiết sản phẩm, trang 4 danh mục và điều hướng | P3 | G4 | Bắt đầu: AD-BI-03; Hoàn tất: AD-BI-04 | [#78](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/78) |
| [AD-BI-06](#ad-bi-06) | Cài RLS DeptManager và Executive từ bi_user_access | P3 | G4 | Bắt đầu: AD-BI-03; Hoàn tất: AD-DE-23 | [#79](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/79) |
| [AD-BI-07](#ad-bi-07) | Dựng trang 5 vận hành và real-time | P3 | G4 | Bắt đầu: AD-BI-03; Hoàn tất: AD-DE-16, AD-DE-17, AD-ST-06, AD-ST-09 | [#80](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/80) |
| [AD-BI-08](#ad-bi-08) | Tạo push semantic model và dashboard Instacart Live | P3 | G4 | Chuẩn bị ngay; Hoàn tất: AD-BI-04, AD-BI-01 | [#81](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/81) |
| [AD-BI-09](#ad-bi-09) | Publish báo cáo, tạo App và lịch refresh | P3 | G4 | Bắt đầu: AD-BI-04, AD-BI-05, AD-BI-06; Hoàn tất: AD-BI-07 | [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82) |
| [AD-BI-10](#ad-bi-10) | Dựng mobile layout cho các trang và chụp ảnh trên điện thoại | P3 | G4 | Bắt đầu: AD-BI-05; Hoàn tất: AD-BI-09 | [#83](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/83) |
| [AD-BI-11](#ad-bi-11) | Mở rộng semantic model cho view DS (khung trang 6–7) | P3 | G5 | Bắt đầu: AD-BI-03; Hoàn tất: AD-DS-22 | [#84](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/84) |
| [AD-BI-12](#ad-bi-12) | Nghiệm thu checklist G4 và diễn tập demo 95/5 | P3 | G4 | Bắt đầu: AD-BI-07, AD-BI-08, AD-BI-09, AD-BI-10, AD-ST-10; Hoàn tất: AD-DE-26 | [#85](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/85) |

### AD-BI-01

**Spike Power BI: giấy phép Pro, workspace instacart-bi, connector, push model, APR, quyền API**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G0**
- Issue: [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74)
- Tài liệu: `docs/devops/01_chuan_bi_va_spike.md §3.6`, `docs/de/07_power_bi.md`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-OPS-04](01_devops.md#ad-ops-04) [#6](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/6)
- **Mở khóa:** [AD-OPS-07b](01_devops.md#ad-ops-07b) (bắt đầu), [AD-DE-25](02_de_batch.md#ad-de-25) (hoàn tất), [AD-BI-02](04_power_bi.md#ad-bi-02) (hoàn tất), [AD-BI-08](04_power_bi.md#ad-bi-08) (hoàn tất)

Phép thử Power BI thuộc cổng G0. Kết quả quyết định lớp serving cho Power BI, cách làm tile real-time và có tự động publish/refresh bằng API được không.

**Việc cần làm**

1. Kiểm giấy phép Pro của P3 (M365 trường hoặc trial 60 ngày). Kiểm giấy phép của P1, P2, P4 chuyển sang [AD-BI-02](#ad-bi-02).
2. Tạo workspace instacart-bi. Việc thêm 3 thành viên còn lại chuyển sang [AD-BI-02](#ad-bi-02).
3. Trong Power BI Desktop, nối connector Azure Databricks tới warehouse đã spike (host + HTTP path).
4. Tạo thử một streaming/push semantic model bằng API và đẩy vài dòng.
5. Bật auto page refresh trên một report DirectQuery, publish và ghi giới hạn thực tế.
6. Hỏi admin hoặc thử REST API Power BI bằng token của UAMI để biết tenant có cho service principal gọi API không.

**Đầu ra**

- Mục Power BI trong docs/devops/spike_results.md: kết quả từng phép thử, ảnh chụp, phương án dự phòng đã chọn
- Workspace Power BI instacart-bi (P3 là Admin)

**Xong khi**

- [ ] Đủ 6 phép thử ở devops/01 §3.6 có kết quả Đạt/Không đạt kèm bằng chứng
- [ ] Đã chốt lớp serving cho Power BI và cách làm tile real-time để ghi vào quyết định G0 (AD-OPS-07b)

### AD-BI-02

**Kết nối Power BI với warehouse và tạo dự án PBIP**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#75](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/75)
- Tài liệu: `docs/de/07_power_bi.md §1`, `docs/devops/06_de_den_dashboard.md §4, §5.1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-23](02_de_batch.md#ad-de-23) [#59](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/59), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22), [AD-BI-01](04_power_bi.md#ad-bi-01) [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74)
- **Mở khóa:** [AD-BI-03](04_power_bi.md#ad-bi-03) (bắt đầu)

Power BI đọc gold qua serverless SQL warehouse bằng Entra ID; nguồn report lưu dạng PBIP để review qua PR.

**Việc cần làm**

1. Get data → Azure Databricks với host và HTTP path của wh-instacart-bi
2. Xác thực Entra ID bằng tài khoản chỉ đọc gold/ops
3. Chọn bảng gold ở de/07 §2, không chọn fact_order_lines
4. Lưu powerbi/instacart.pbip, thêm .pbi/localSettings.json và cache.abf vào .gitignore
5. Nếu không có serverless: chọn dự phòng theo devops/06 §4
6. Kiểm giấy phép Pro hoặc trial của P1, P2, P4, ghi ngày hết hạn. Trial chỉ có 60 ngày nên bật sao cho còn hạn tới buổi demo
7. Thêm P1, P2, P4 vào instacart-bi với quyền Member theo devops/06 §5.1. Người xem dashboard nhận qua App ở AD-BI-09, không thêm vào workspace

**Đầu ra**

- powerbi/instacart.pbip (khung)
- .gitignore cập nhật
- Workspace Power BI instacart-bi có đủ 4 thành viên

**Xong khi**

- [ ] Desktop tải được dữ liệu gold qua warehouse
- [ ] Git không chứa cache.abf hay thông tin đăng nhập
- [ ] P1, P2, P4 là Member của instacart-bi và mở được report thử, ngày hết hạn giấy phép đã ghi

### AD-BI-03

**Dựng star schema và measure DAX**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76)
- Tài liệu: `docs/de/07_power_bi.md §2, §3`
- **Bắt đầu sau:** [AD-BI-02](04_power_bi.md#ad-bi-02) [#75](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/75)
- **Hoàn tất còn cần:** [AD-DE-24](02_de_batch.md#ad-de-24) [#60](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/60)
- **Mở khóa:** [AD-BI-04](04_power_bi.md#ad-bi-04) (bắt đầu), [AD-BI-05](04_power_bi.md#ad-bi-05) (bắt đầu), [AD-BI-06](04_power_bi.md#ad-bi-06) (bắt đầu), [AD-BI-07](04_power_bi.md#ad-bi-07) (bắt đầu), [AD-BI-11](04_power_bi.md#ad-bi-11) (bắt đầu)

Semantic model dùng chung cho mọi trang: chế độ Import/DirectQuery từng bảng, quan hệ một chiều, bảng _Measures.

**Việc cần làm**

1. Đặt chế độ từng bảng theo de/07 §2; mark dim_date là date table
2. Tạo quan hệ (kể cả product_pair_counts[p2] inactive)
3. Ẩn cột khóa phía nhiều và cột kỹ thuật (dup_cnt, n, off_match_score)
4. Tạo bảng _Measures với các measure ở de/07 §3
5. Đặt image_url là Data category = Image URL

**Đầu ra**

- Semantic model trong powerbi/ (PBIP)

**Xong khi**

- [ ] Tổng Orders = SELECT sum(orders) FROM gold.kpi_daily
- [ ] Pair Count as P2 trả số khác rỗng khi lọc theo p2
- [ ] Không có quan hệ hai chiều

### AD-BI-04

**Dựng trang 1 Tổng quan và trang 3 thời gian mua**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#77](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77)
- Tài liệu: `docs/de/07_power_bi.md §4`
- **Bắt đầu sau:** [AD-BI-03](04_power_bi.md#ad-bi-03) [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-BI-05](04_power_bi.md#ad-bi-05) (hoàn tất), [AD-BI-08](04_power_bi.md#ad-bi-08) (hoàn tất), [AD-BI-09](04_power_bi.md#ad-bi-09) (bắt đầu)

Hai trang KPI theo thời gian: thẻ KPI, xu hướng doanh thu giả lập, heatmap thứ × giờ dùng cột gốc.

**Việc cần làm**

1. Trang 1: 5 thẻ KPI, đường Revenue theo year_month, cột Orders theo source, slicer ngày
2. Ghi rõ doanh thu là giả lập trên trang
3. Trang 3: ma trận order_dow × order_hour_of_day, đường Orders theo ngày
4. Tạo 2 bookmark Theo giờ / Theo tuần và nút gán bookmark

**Đầu ra**

- Trang 1 và trang 3 trong PBIP

**Xong khi**

- [ ] Số trên thẻ Orders khớp gold.kpi_daily
- [ ] Hai nút bookmark chuyển đúng trạng thái

### AD-BI-05

**Dựng trang 2 chi tiết sản phẩm, trang 4 danh mục và điều hướng**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#78](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/78)
- Tài liệu: `docs/de/07_power_bi.md §4`
- **Bắt đầu sau:** [AD-BI-03](04_power_bi.md#ad-bi-03) [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76)
- **Hoàn tất còn cần:** [AD-BI-04](04_power_bi.md#ad-bi-04) [#77](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77)
- **Mở khóa:** [AD-BI-09](04_power_bi.md#ad-bi-09) (bắt đầu), [AD-BI-10](04_power_bi.md#ad-bi-10) (bắt đầu)

Trang 2 là đích drill-through (ảnh, giá, Nutri-Score, sản phẩm mua kèm); trang 4 lọc theo department/aisle; rubric cần ≥ 4 trang có điều hướng.

**Việc cần làm**

1. Trang 2: ảnh, thẻ giá/KM/Nutri-Score/NOVA, top 10 mua kèm theo lift, drill-through field product_id, nút Back
2. Trang 4: treemap Dept Revenue, bảng top sản phẩm, Promo Item Share, slicer department/aisle
3. Page navigator trên mọi trang, ẩn trang 2 khỏi navigator
4. Drill-through từ trang 1 và 4 sang trang 2; tooltip page (tùy chọn)

**Đầu ra**

- Trang 2 và trang 4 trong PBIP
- Thanh điều hướng chung

**Xong khi**

- [ ] Drill-through sang trang 2 và nút Back hoạt động
- [ ] Ảnh hiện cho sản phẩm matched
- [ ] Dept Orders chỉ dùng khi đã lọc một ngành, có nhãn 'có mặt ngành'

### AD-BI-06

**Cài RLS DeptManager và Executive từ bi_user_access**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#79](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/79)
- Tài liệu: `docs/de/07_power_bi.md §5`, `docs/de/05_gold_serving.md §2.3`
- **Bắt đầu sau:** [AD-BI-03](04_power_bi.md#ad-bi-03) [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76)
- **Hoàn tất còn cần:** [AD-DE-23](02_de_batch.md#ad-de-23) [#59](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/59)
- **Mở khóa:** [AD-BI-09](04_power_bi.md#ad-bi-09) (bắt đầu)

Bảo mật mức dòng theo email người xem; lọc trên dim_department và lan sang department_sales, product_360.

**Việc cần làm**

1. Nhập landing_meta.bi_access_seed bằng SQL editor (≥ 2 người, 2 department), không commit email
2. Tạo role DeptManager với biểu thức DAX ở de/07 §5 và role Executive không lọc
3. Ẩn bi_user_access, không tạo quan hệ
4. Kiểm bằng View as trong Desktop
5. Ghi rõ trang Tổng quan không bị RLS lọc

**Đầu ra**

- Role RLS trong semantic model
- Bảng landing_meta.bi_access_seed (nhập tay)

**Xong khi**

- [ ] gold.bi_user_access có ≥ 2 người ở 2 department
- [ ] View as người A chỉ thấy department được cấp ở trang 4

### AD-BI-07

**Dựng trang 5 vận hành và real-time**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#80](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/80)
- Tài liệu: `docs/de/07_power_bi.md §2, §4, §6b`
- **Bắt đầu sau:** [AD-BI-03](04_power_bi.md#ad-bi-03) [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76)
- **Hoàn tất còn cần:** [AD-DE-16](02_de_batch.md#ad-de-16) [#52](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/52), [AD-DE-17](02_de_batch.md#ad-de-17) [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53), [AD-ST-06](03_de_stream.md#ad-st-06) [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69), [AD-ST-09](03_de_stream.md#ad-st-09) [#72](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/72)
- **Mở khóa:** [AD-BI-09](04_power_bi.md#ad-bi-09) (hoàn tất), [AD-BI-12](04_power_bi.md#ad-bi-12) (bắt đầu)

Trang 5 cho thấy streaming, độ trễ khác region và chất lượng dữ liệu; DirectQuery cho bảng stream.

**Việc cần làm**

1. Thêm stream_kpi_1m, ops.streaming_latency (DirectQuery), ops.dq_results, ops.reconciliation (Import)
2. Thẻ Orders last 5 min, đường stream_kpi_1m, p50/p95 độ trễ theo eh_region
3. Bảng dq_results (tỉ lệ lỗi theo luật) và reconciliation
4. Bật auto page refresh 5 giây trong Desktop, ghi giới hạn khoảng 30 phút trên Service

**Đầu ra**

- Trang 5 trong PBIP

**Xong khi**

- [ ] Trang 5 cập nhật trong Desktop khi pipeline stream chạy
- [ ] Hiển thị độ trễ cùng và khác region

### AD-BI-08

**Tạo push semantic model và dashboard Instacart Live**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#81](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/81)
- Tài liệu: `docs/de/07_power_bi.md §6a`, `docs/devops/06_de_den_dashboard.md §5.3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-BI-04](04_power_bi.md#ad-bi-04) [#77](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77), [AD-BI-01](04_power_bi.md#ad-bi-01) [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74)
- **Mở khóa:** [AD-ST-10](03_de_stream.md#ad-st-10) (hoàn tất), [AD-BI-12](04_power_bi.md#ad-bi-12) (bắt đầu)

Tile real-time vài giây cần streaming semantic model kiểu API; push URL chứa khóa nên chỉ lưu ở Key Vault.

**Việc cần làm**

1. Tạo streaming semantic model instacart_rt (minute, orders, items), bật Historic data analysis
2. Lưu Push URL vào Key Vault tên pbi-push-url bằng az keyvault secret set, không dán vào code hay chat
3. Tạo dashboard Instacart Live: tile Card orders và Line theo minute
4. Ghim thêm visual trang 1 lên dashboard
5. Ghi tên secret để AD-ST-10 nối sink (cùng P3 làm)

**Đầu ra**

- Semantic model instacart_rt
- Dashboard Instacart Live
- Secret pbi-push-url trong Key Vault

**Xong khi**

- [ ] Gửi thử một dòng bằng POST thấy tile đổi số
- [ ] Dashboard có cả tile real-time và visual batch

### AD-BI-09

**Publish báo cáo, tạo App và lịch refresh**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82)
- Tài liệu: `docs/de/07_power_bi.md §7, §8`, `docs/devops/06_de_den_dashboard.md §5.1, §5.6`
- **Bắt đầu sau:** [AD-BI-04](04_power_bi.md#ad-bi-04) [#77](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77), [AD-BI-05](04_power_bi.md#ad-bi-05) [#78](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/78), [AD-BI-06](04_power_bi.md#ad-bi-06) [#79](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/79)
- **Hoàn tất còn cần:** [AD-BI-07](04_power_bi.md#ad-bi-07) [#80](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/80)
- **Mở khóa:** [AD-DE-25](02_de_batch.md#ad-de-25) (hoàn tất), [AD-BI-10](04_power_bi.md#ad-bi-10) (hoàn tất), [AD-BI-12](04_power_bi.md#ad-bi-12) (bắt đầu), [AD-DS-25](05_data_science.md#ad-ds-25) (bắt đầu)

Người xem nhận báo cáo qua App để RLS có hiệu lực; Import refresh hằng ngày sau pl_master.

**Việc cần làm**

1. Publish vào instacart-bi, semantic model tên instacart
2. Data source credentials OAuth2, scheduled refresh 04:00
3. Gán người vào role ở Semantic model → Security, Test as role
4. Tạo App Instacart Analytics, audience Quản lý ngành hàng (trang 1–4) và Ban điều hành (thêm trang 5)
5. Không dùng Publish to web; gửi workspaceId/datasetId cho P2 (refresh_bi)

**Đầu ra**

- App Instacart Analytics
- Lịch refresh trên Service
- Ảnh Test as role của 2 người

**Xong khi**

- [ ] Người A và người B thấy trang 4 khác nhau, Executive thấy hết (có ảnh)
- [ ] Scheduled refresh chạy thành công một lần
- [ ] Người xem không là thành viên workspace

### AD-BI-10

**Dựng mobile layout cho các trang và chụp ảnh trên điện thoại**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#83](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/83)
- Tài liệu: `docs/de/07_power_bi.md §8`, `docs/devops/06_de_den_dashboard.md §5.5`
- **Bắt đầu sau:** [AD-BI-05](04_power_bi.md#ad-bi-05) [#78](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/78)
- **Hoàn tất còn cần:** [AD-BI-09](04_power_bi.md#ad-bi-09) [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82)
- **Mở khóa:** [AD-BI-12](04_power_bi.md#ad-bi-12) (bắt đầu)

Rubric yêu cầu responsive mobile: mỗi trang có bố cục dọc riêng, kiểm trên app Power BI Mobile.

**Việc cần làm**

1. View → Mobile layout cho trang 1–4 (và 5): thẻ KPI lên trên, 1–2 biểu đồ chính
2. Page navigator dạng dọc, bỏ visual phụ
3. Mở App bằng tài khoản người xem trên điện thoại, chụp từng trang

**Đầu ra**

- Mobile layout trong PBIP
- Ảnh chụp trên điện thoại cho báo cáo

**Xong khi**

- [ ] Đủ 4 trang có mobile layout và ảnh chụp

### AD-BI-11

**Mở rộng semantic model cho view DS (khung trang 6–7)**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G5**
- Issue: [#84](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/84)
- Tài liệu: `docs/de/07_power_bi.md §2, §4`, `docs/ds/05_mlflow_serving_bao_cao.md §4, §5`
- **Bắt đầu sau:** [AD-BI-03](04_power_bi.md#ad-bi-03) [#76](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/76)
- **Hoàn tất còn cần:** [AD-DS-22](05_data_science.md#ad-ds-22) [#107](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/107)
- **Mở khóa:** [AD-DS-25](05_data_science.md#ad-ds-25) (bắt đầu)

Trang 6–7 do phần DS dựng; phần chung là thêm view DS vào model đúng chế độ và quan hệ để không phá RLS và model hiện có.

**Việc cần làm**

1. Thêm recommendations, association_rules, experiment_metrics, holdout_metrics (Import)
2. Quan hệ product_360 → recommendations[product_id], product_360 → association_rules[consequent_product_id]
3. Giữ association_rules không nối dim_department (RLS không lọc)
4. Tạo trang 6, 7 trống có navigator và thêm vào audience App
5. Thêm visual 'Gợi ý theo luật' trên trang 2

**Đầu ra**

- Semantic model có 4 view DS
- Khung trang 6, 7 trong PBIP

**Xong khi**

- [ ] Refresh model không lỗi
- [ ] RLS ở trang 4 vẫn đúng sau khi thêm bảng

### AD-BI-12

**Nghiệm thu checklist G4 và diễn tập demo 95/5**

- Vai dự kiến: **P3 · Stream/BI**
- Cổng: **G4**
- Issue: [#85](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/85)
- Tài liệu: `docs/de/07_power_bi.md §9`, `docs/devops/06_de_den_dashboard.md §6.4`
- **Bắt đầu sau:** [AD-BI-07](04_power_bi.md#ad-bi-07) [#80](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/80), [AD-BI-08](04_power_bi.md#ad-bi-08) [#81](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/81), [AD-BI-09](04_power_bi.md#ad-bi-09) [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82), [AD-BI-10](04_power_bi.md#ad-bi-10) [#83](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/83), [AD-ST-10](03_de_stream.md#ad-st-10) [#73](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/73)
- **Hoàn tất còn cần:** [AD-DE-26](02_de_batch.md#ad-de-26) [#62](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/62)
- **Mở khóa:** [AD-RP-04](06_bao_cao_demo.md#ad-rp-04) (bắt đầu), [AD-RP-08](06_bao_cao_demo.md#ad-rp-08) (bắt đầu)

Gom bằng chứng mở cổng G4: điều hướng, RLS, mobile, tile real-time, độ trễ, tổng khớp nguồn.

**Việc cần làm**

1. Đi lần lượt checklist de/07 §9
2. Diễn tập 95/5 (P3 vừa phát stream vừa theo dõi dashboard): phát nốt 5% với --rate 20, đo thời gian tile đổi
3. Kiểm tổng Orders trang 1 với SQL
4. Kiểm powerbi/ không có cache.abf hay thông tin đăng nhập
5. Gom ảnh vào thư mục bằng chứng báo cáo

**Đầu ra**

- Checklist G4 đã tick
- Bộ ảnh bằng chứng rubric mục 4

**Xong khi**

- [ ] Mọi mục de/07 §9 đạt
- [ ] Tile tăng trong ≤ 1 phút khi phát nốt 5%
