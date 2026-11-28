# Power BI

Dự án Power BI lưu dạng **PBIP** (văn bản, review được qua PR). Không commit `.pbix`.
4 report, RLS, mobile layout, tile real-time: xem docs/devops/06, mục 5.

## Mở dự án Instacart

Mở `instacart.pbip` trong Power BI Desktop. Dự án liên kết báo cáo `instacart.Report`
với mô hình dữ liệu `instacart.SemanticModel` bằng đường dẫn tương đối, nên các tệp có
thể mở từ một bản checkout Git mới. Git bỏ qua `localSettings.json` và `cache.abf`.

## Nghiệm thu AD-BI-02

- Sau khi AD-DE-23 bàn giao Gold trên Sub D, mở **Connection details** của
  `wh-instacart-bi` trong workspace Databricks hiện hành để lấy **Server Hostname**
  và **HTTP Path**. Dùng hai giá trị này trong connector **Azure Databricks** của
  Power BI Desktop và đăng nhập Microsoft Entra ID bằng tài khoản vai BI.
  Chọn bảng và chế độ tải theo [hướng dẫn Power BI](../docs/de/07_power_bi.md#2-mô-hình-dữ-liệu-star-schema).
  Bỏ chọn `fact_order_lines` trong Navigator. Lưu danh sách bảng tải thành công
  cùng kết quả đối chiếu với warehouse làm minh chứng.
- Giữ **Auto date/time** tắt ở cấp dự án. Dùng `dim_date[date]` khi cấu hình
  bảng ngày sau khi nạp mô hình Gold.
- Trong Power BI Service, dùng **Manage access** ở workspace `instacart-bi` để kiểm
  tra P3 là Admin và P1, P2, P4 là Member. Xác nhận từng người mở được báo cáo thử
  và ghi loại giấy phép Pro hoặc trial cùng ngày hết hạn nếu có. Trên PR chỉ ghi
  vai trò, trạng thái kiểm tra và thông tin giấy phép, giữ tài khoản Entra riêng.
- Trước khi commit, rà soát tệp dự án để loại trừ thông tin đăng nhập và kiểm tra
  `localSettings.json`, `cache.abf` nằm ngoài các tệp Git theo dõi.
