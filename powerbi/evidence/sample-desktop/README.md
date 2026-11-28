# Nghiệm thu Power BI trên dữ liệu mẫu

Các ảnh được chụp từ bản sao dự án PBIP trên Power BI Desktop 26.11.
Nguồn là CSV giả lập đi kèm model, gồm 61 ngày, 3 ngành và 9 sản phẩm.
Desktop đã tải mẫu và DAX trả **200 đơn**, **696 món**.
`Pair Count as P2` trả **116** khi lọc sản phẩm mẫu 202.

| Trang | Desktop | Mobile trong Desktop |
| --- | --- | --- |
| Tổng quan | [Ảnh](desktop-overview.png) | [Ảnh](mobile-overview.png) |
| Chi tiết sản phẩm | [Ảnh](desktop-product.png) | [Ảnh](mobile-product.png) |
| Thời gian mua | [Theo giờ](desktop-shopping-time.png), [Theo tuần](desktop-weekly.png) | [Ảnh](mobile-shopping-time.png) |
| Danh mục | [Ảnh](desktop-category.png) | [Ảnh](mobile-category.png) |
| Vận hành | [Ảnh](desktop-operations.png) | [Ảnh](mobile-operations.png) |

Các số độ trễ là dữ liệu minh họa. Mẫu `eastasia` có p50/p95 là
341,5/405,85 ms và mẫu `koreacentral` có 581,5/645,85 ms.
Thẻ đơn trong 5 phút gần nhất để trống vì timestamp mẫu nằm ngoài
khoảng thời gian này lúc chụp.

Hai bookmark chuyển qua lại giữa ma trận thứ × giờ và biểu đồ tuần ISO.
Từ Danh mục, drill-through trên dòng sản phẩm 202 mở trang Chi tiết chỉ với
sản phẩm đã chọn và các cặp liên quan. Ảnh sản phẩm cần nguồn Gold thật.

Trong Desktop, **View as → DeptManager** với người dùng hiện tại không có
ánh xạ trong fixture trả trang Danh mục không có số doanh thu hay sản phẩm.
[Ảnh kiểm tra RLS](rls-no-mapping.png)

Đối chiếu Gold prod, quyền RLS của người dùng thật và ảnh trong ứng dụng
Power BI Mobile thực hiện khi có dữ liệu và báo cáo đã publish.
