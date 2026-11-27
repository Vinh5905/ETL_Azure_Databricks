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
