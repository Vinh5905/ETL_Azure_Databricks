# Power BI

Dự án Power BI lưu dạng **PBIP** (văn bản, review được qua PR). Không commit `.pbix`.
4 report, RLS, mobile layout, tile real-time: xem docs/devops/06, mục 5.

## Mở dự án Instacart

Mở [dự án Instacart](instacart/instacart.pbip) trong Power BI Desktop. Dự án liên kết báo cáo `instacart.Report`
với mô hình dữ liệu `instacart.SemanticModel` bằng đường dẫn tương đối, nên các tệp có
thể mở từ một bản checkout Git mới. Git bỏ qua `localSettings.json` và `cache.abf`.

## Dựng báo cáo bằng dữ liệu mẫu

Giữ `DbxServer` là `(sample)`, rồi chọn **Home → Refresh**. Power Query đọc
CSV UTF-8 đi kèm định nghĩa model, dùng chung kiểu cột với bảng Gold.
Mẫu gồm 61 ngày từ tháng 10 đến tháng 11/2026, 3 ngành, 9 sản phẩm và
2 vai quản lý ngành để kiểm tra biểu đồ và bộ lọc. Nguồn ngày, đơn, món,
cặp sản phẩm và bảng doanh số đều là dữ liệu giả lập.

## Nghiệm thu AD-BI-02

- Sau khi AD-DE-23 bàn giao Gold trên Sub D, mở **Connection details** của
  `wh-instacart-bi` trong workspace Databricks hiện hành để lấy **Server Hostname**
  và **HTTP Path**. Trong **Transform data → Manage parameters**, điền lần lượt
  `DbxServer`, `DbxHttpPath`, `Catalog` và `Schema`. Với prod hiện hành,
  catalog là `instacart` và schema là `gold`.
- Đăng nhập Microsoft Entra ID bằng tài khoản vai BI khi connector yêu cầu.
  Áp dụng thay đổi, Refresh và đối chiếu danh sách bảng với
  [hướng dẫn Power BI](../docs/de/07_power_bi.md#2-mô-hình-dữ-liệu-star-schema).
- Giữ **Auto date/time** tắt ở cấp dự án. Dùng `dim_date[date]` khi cấu hình
  bảng ngày sau khi nạp mô hình Gold.
- Trong Power BI Service, xác nhận P3 có Pro hoặc trial còn hạn đến buổi demo.
  Ghi hạn trial nếu có. Xác nhận P1, P2 và P4 mở báo cáo thử bằng Desktop hoặc
  nhận bản PDF. [Quy định giấy phép và chia sẻ](../docs/devops/06_de_den_dashboard.md#51-giấy-phép-và-phân-phối).
- Trước khi commit, rà soát tệp dự án để loại trừ thông tin đăng nhập và kiểm tra
  `localSettings.json`, `cache.abf` nằm ngoài các tệp Git theo dõi.

## Nghiệm thu trang Vận hành

Mẫu local dùng Import cho `stream_kpi_1m`, `streaming_latency`,
`dq_results` và `reconciliation`. Ba bảng vận hành đọc schema cùng tiền tố
với Gold. Ví dụ, `Schema=gold` dùng `ops`, còn `Schema=p3_gold` dùng `p3_ops`.

Khi có nguồn thật, cấu hình hai bảng `stream_kpi_1m` và `streaming_latency`
ở chế độ DirectQuery, giữ hai bảng chất lượng và đối soát ở Import. Sau đó
bật **Page refresh** trên trang Vận hành và kiểm trong Desktop, theo
[hướng dẫn cập nhật thời gian thực](../docs/de/07_power_bi.md#6-cập-nhật-gần-thời-gian-thực).
Biểu đồ độ trễ so p50/p95 của `ms_eh_to_bronze` theo `eh_region`.
