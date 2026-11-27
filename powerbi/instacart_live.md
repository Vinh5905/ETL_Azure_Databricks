# Instacart Live (AD-BI-08)

## Mô hình streaming

Đã tạo semantic model `instacart_rt` trong workspace `instacart-bi` ngày
27/11/2026. Trên Power BI Service, chọn **New item → Legacy → Streaming
dataset → API**. Bật **Historic data analysis** và kiểm tra các trường trước
khi tạo.

| Trường | Kiểu trên Power BI | Ý nghĩa |
|---|---|---|
| `minute` | DateTime | Đầu phút theo UTC |
| `orders` | Number | Số đơn của phút |
| `items` | Number | Số món của phút |

Power BI đã báo tạo schema thành công và liệt kê model trong workspace. Nếu
model đã tồn tại, đối chiếu schema hiện có trước khi tạo lại. [Cách tạo
streaming semantic model](https://learn.microsoft.com/power-bi/connect-data/service-real-time-streaming#create-a-streaming-semantic-model)

## Dashboard và tile

Đã tạo dashboard **Instacart Live** trong cùng workspace. Trong **Edit →
Add a tile → Custom Streaming Data**, chọn `instacart_rt` cho hai tile:

- **Orders – latest minute** dùng Card, trường `orders`.
- **Orders by minute – last 15 minutes** dùng Line chart, trục `minute` và
  giá trị `orders`, phạm vi 15 phút.

Cả hai tile đã xuất hiện trên dashboard. Visual Trang 1 cần ghim thêm sau
khi [AD-BI-04](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/77)
bàn giao. Chọn **Pin visual** từ báo cáo đã publish và ghim vào dashboard này.

## Kiểm tra POST và bàn giao cho ST10

Từ gốc repo, chạy bằng PowerShell 7 để xem payload mẫu:

```powershell
./powerbi/send_instacart_live_sample.ps1 -Orders 2 -Items 5
```

Khi nghiệm thu, thêm `-Send` và dán Push URL của đúng `instacart_rt` vào prompt
ẩn. Kết quả cần có mã HTTP thành công và Card hiển thị `2`. Ghi giờ UTC, mã HTTP
và giá trị tile. Nếu cần kiểm tra Line chart bằng hai điểm khác phút, chọn hai
phút gần thời điểm đo và dùng `-Minute` cùng số đơn phân biệt. Hai tile cần
khớp dữ liệu gửi. Chụp vùng tile và tránh màn hình thông tin tích hợp.

Người có quyền ghi secret trong Key Vault core lưu Push URL dưới tên
`pbi-push-url` bằng Azure Portal. Bàn giao **tên secret** và xác nhận scope
`kv` của Databricks có thể đọc nó. Push URL là thông tin xác thực, giữ ngoài
Git và chat. ST10 dùng model này sau khi xác minh quyền và kết nối.

**Trạng thái.** Cấu hình model và hai tile đã kiểm trên Service. Payload mẫu
đã kiểm local. Lượt POST thật, secret ở Key Vault và visual batch còn chờ.
