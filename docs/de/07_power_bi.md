# 07 · Power BI: từ bảng gold đến dashboard hoàn chỉnh

**Đầu vào:**
- Các bảng gold đã đạt kiểm tra ([05](05_gold_serving.md)).
- Warehouse `wh-instacart-bi` (host và HTTP path lấy từ output `30-databricks`).
- Workspace `instacart-bi` và giấy phép Pro ([devops/06 §5.1](../devops/06_de_den_dashboard.md)).

**Đầu ra:**
- Báo cáo `powerbi/instacart.pbip` (lưu trong repo, dạng text).
- Semantic model có RLS, 4 trang (+1 trang vận hành) kèm điều hướng và mobile layout.
- Ở G5: thêm trang 6 (luật kết hợp) và trang 7 (thực nghiệm thuật toán) từ view DS ([ds/05 §5](../ds/05_mlflow_serving_bao_cao.md)).
- Dashboard có tile real-time; App "Instacart Analytics".

**Xong khi:** đạt checklist G4 ở mục 9 (trùng với [devops/06 §6.4](../devops/06_de_den_dashboard.md)).

> Tài liệu [devops/06 §5](../devops/06_de_den_dashboard.md) nói **vì sao** chọn cơ chế (giấy phép, giới hạn của Pro, cách phân phối). File này nói **làm từng bước thế nào**.

---

## 1. Kết nối

1. Power BI Desktop → *Get data* → **Azure Databricks**.
2. Điền thông tin kết nối:
   - **Server hostname:** `adb-<id>.<n>.azuredatabricks.net`.
   - **HTTP path:** `/sql/1.0/warehouses/<warehouse_id>`.
3. Chọn *Data connectivity mode* theo từng bảng (mục 2). Lần đầu chọn Import cho mọi bảng, sau đó đổi riêng bảng cần DirectQuery.
4. **Xác thực:** *Microsoft Entra ID* bằng tài khoản P3. Người đó chỉ có quyền đọc `gold` và `ops`, đúng nguyên tắc đặc quyền tối thiểu ([devops/05 bước 5](../devops/05_databricks_platform.md)).
5. Navigator → catalog `instacart` → chọn các bảng ở mục 2. **Không** chọn `fact_order_lines`.

Lưu bằng *File → Save as → Power BI Project (.pbip)* vào `powerbi/`. Thêm `powerbi/**/.pbi/localSettings.json` và `powerbi/**/cache.abf` vào `.gitignore`.

## 2. Mô hình dữ liệu (star schema)

| Bảng | Chế độ | Vai trò | Ghi chú |
|---|---|---|---|
| `dim_date` | Import | Dim | *Mark as date table* theo cột `date` |
| `dim_department` | Import | Dim, **nơi đặt RLS** | 21 dòng |
| `product_360` | Import | Dim sản phẩm + chỉ số | Cột `image_url` đặt *Data category = Image URL* |
| `kpi_daily` | Import | Fact | |
| `kpi_by_dow_hour` | Import | Fact | Không nối `dim_date` (không có ngày) |
| `department_sales` | Import | Fact | Có RLS qua `dim_department` |
| `product_pair_counts` | Import | Fact | |
| `bi_user_access` | Import | Bảng tra RLS | Ẩn khỏi report view, không tạo quan hệ |
| `stream_kpi_1m` | **DirectQuery** | Fact real-time | Trang 5 |
| `ops.streaming_latency` | **DirectQuery** | Vận hành | Trang 5 |
| `ops.dq_results`, `ops.reconciliation` | Import | Vận hành | Trang 5 |
| `recommendations` (view DS) | Import | Fact | Trang 2: "Gợi ý theo luật" cạnh bảng `product_pair_counts`. Có từ G5 |
| `association_rules` (view DS) | Import | Fact | Trang 6. Không nối `dim_department` nên RLS không lọc bảng này: luật là tri thức chung, không phải số bán theo ngành |
| `experiment_metrics`, `holdout_metrics` (view DS) | Import | Kết quả thực nghiệm | Trang 7. Bảng rời, không quan hệ |

**Quan hệ** (đều là một-nhiều, lọc một chiều):

```
dim_date[date]                  1 ─→ * kpi_daily[order_date]
dim_date[date]                  1 ─→ * department_sales[order_date]
dim_department[department_id]   1 ─→ * department_sales[department_id]
dim_department[department_id]   1 ─→ * product_360[department_id]
product_360[product_id]         1 ─→ * product_pair_counts[p1]      (active)
product_360[product_id]         1 ─→ * product_pair_counts[p2]      (inactive, dùng USERELATIONSHIP)
product_360[product_id]         1 ─→ * recommendations[product_id]                       (G5)
product_360[product_id]         1 ─→ * association_rules[consequent_product_id]          (G5, drill-through)
```

Ẩn mọi cột khóa ở phía nhiều. Ẩn các cột kỹ thuật (`dup_cnt`, `n`, `off_match_score`).

## 3. Measure DAX

Tạo bảng rỗng `_Measures` để chứa các measure:

```dax
Orders          = SUM ( kpi_daily[orders] )
Items           = SUM ( kpi_daily[items] )
Revenue (sim)   = SUM ( kpi_daily[revenue_sim] )
Avg Basket Size = DIVIDE ( [Items], [Orders] )
Reorder Rate    = DIVIDE ( SUMX ( kpi_daily, kpi_daily[reorder_rate] * kpi_daily[items] ), [Items] )

Revenue MoM % =
VAR cur  = [Revenue (sim)]
VAR prev = CALCULATE ( [Revenue (sim)], DATEADD ( dim_date[date], -1, MONTH ) )
RETURN DIVIDE ( cur - prev, prev )

-- Theo ngành hàng (department_sales)
Dept Revenue      = SUM ( department_sales[revenue_sim] )
Dept Items        = SUM ( department_sales[items] )
Dept Reorder Rate = DIVIDE ( SUM ( department_sales[reordered_items] ), [Dept Items] )
Promo Item Share  = DIVIDE ( SUM ( department_sales[promo_items] ), [Dept Items] )
Dept Orders (có mặt ngành) = SUM ( department_sales[orders] )

-- Cặp sản phẩm: xem theo p2 khi người dùng chọn sản phẩm ở vai trò p2
Pair Count as P2 =
CALCULATE ( SUM ( product_pair_counts[pair_count] ),
            USERELATIONSHIP ( product_360[product_id], product_pair_counts[p2] ) )

-- Real-time
Orders last 5 min =
CALCULATE ( SUM ( stream_kpi_1m[orders] ),
            stream_kpi_1m[minute] >= NOW () - TIME ( 0, 5, 0 ) )
```

**Bẫy cộng dồn sai:** `department_sales[orders]` là số đơn **có món** của aisle đó. Một đơn mua ở 3 aisle sẽ được đếm 3 lần khi cộng qua các aisle. Vì vậy:
- Tổng số đơn **luôn** lấy từ `kpi_daily`.
- `Dept Orders` chỉ dùng khi đã lọc về một aisle hoặc department, và đặt tên rõ là "có mặt ngành".

`NOW()` trong DirectQuery được tính ở giờ UTC của Service. `stream_kpi_1m[minute]` cũng là UTC, nên không lệch múi giờ.

## 4. Bốn trang và điều hướng

| Trang | Visual chính | Bộ lọc / tương tác |
|---|---|---|
| **1 · Tổng quan** | 5 thẻ KPI (Orders, Items, Revenue (sim), Avg Basket, Reorder Rate); đường Revenue theo `dim_date[year_month]`; cột Orders theo `source` | Slicer khoảng ngày (`dim_date`); nút sang trang 3, 4 |
| **2 · Chi tiết sản phẩm** (đích drill-through) | Ảnh (`image_url`), thẻ giá hiện hành / KM / Nutri-Score / NOVA; bảng top 10 sản phẩm hay mua kèm (`product_pair_counts` theo `lift`) | Drill-through field: `product_360[product_id]`; nút *Back* |
| **3 · Bộ lọc 1: thời gian mua** | Ma trận nhiệt `order_dow` × `order_hour_of_day` (`kpi_by_dow_hour`); đường Orders theo ngày | Slicer ngày; bookmark "Theo giờ" / "Theo tuần" |
| **4 · Bộ lọc 2: danh mục** | Treemap Dept Revenue theo department → aisle; bảng top sản phẩm (`product_360`); cột Promo Item Share | Slicer department / aisle; chuột phải → *Drill through* sang trang 2 |
| 5 · Vận hành & real-time | Thẻ `Orders last 5 min`; đường `stream_kpi_1m`; p50/p95 độ trễ theo `eh_region`; bảng `dq_results` (nhãn pipeline ưu tiên `pipeline_name`), `reconciliation` | Auto page refresh (mục 6) |
| 6 · Luật kết hợp (G5) | Bảng luật (vế trái → vế phải, support, confidence, lift); scatter support × confidence, cỡ điểm theo lift; thẻ số luật | Slicer `confidence`, `lift`, `antecedent_size`; drill-through sang trang 2 theo `consequent_product_id` |
| 7 · Thực nghiệm thuật toán (G5) | Thời gian theo MinSupp / quy mô / số item; speedup và efficiency theo core; MapReduce và Spark; HitRate@10 kèm baseline | Slicer `experiment`, `grain`, `platform`, `algo`; bookmark "Hiệu năng" / "Chất lượng luật". Danh sách biểu đồ ở [ds/04 §8](../ds/04_thuc_nghiem_danh_gia.md) |

**Điều hướng** (rubric yêu cầu ≥ 4 report và có điều hướng):
1. *Insert → Buttons → Navigator → Page navigator* ở thanh trên cùng mọi trang. Ẩn trang 2 khỏi navigator, vì trang này chỉ vào qua drill-through.
2. Trên trang 2, nút *Back* (*Insert → Buttons → Back*).
3. Bookmark: trên trang 3 tạo 2 bookmark (hiện ma trận giờ / hiện đường theo tuần). Hai nút gán *Action → Bookmark*.
4. Tooltip page (tùy chọn): rê chuột lên một sản phẩm ở trang 4 thì hiện ảnh và giá.

## 5. Bảo mật mức dòng (RLS)

*Modeling → Manage roles*:

| Role | Bảng lọc | Biểu thức DAX |
|---|---|---|
| `DeptManager` | `dim_department` | xem bên dưới |
| `Executive` | (không lọc) | — |

```dax
-- Role DeptManager, bộ lọc trên bảng dim_department
[department_id]
    IN CALCULATETABLE (
        VALUES ( bi_user_access[department_id] ),
        bi_user_access[user_email] = USERPRINCIPALNAME ()
    )
```

**Vì sao lọc trên `dim_department` mà không tạo quan hệ tới `bi_user_access`:**
- Bộ lọc đi theo quan hệ một chiều từ dim sang fact (`department_sales`, `product_360`), không cần bật lọc hai chiều.
- `bi_user_access` không nằm trong đường lọc của bất kỳ visual nào.

**Phạm vi:**
- `kpi_daily` và `kpi_by_dow_hour` không có cột department nên **không** bị RLS lọc. Ghi rõ trong báo cáo: trang Tổng quan là số toàn hệ thống.
- Nếu muốn lọc cả các trang đó, phải thêm `department_id` vào grain của hai bảng ở [05 §2.2](05_gold_serving.md).

**Kiểm tra:**
- Trong Desktop: *Modeling → View as → DeptManager + Other user = <email người A>*. Trang 4 chỉ còn department được cấp.
- Trên Service: *Semantic model → Security*, thêm người A, B vào `DeptManager` và giảng viên vào `Executive`, rồi *Test as role*. Chụp màn hình 2 người thấy 2 bộ số khác nhau.

**Lưu ý:** RLS chỉ áp với người có quyền **Viewer**. Thành viên Admin/Member/Contributor của workspace thấy hết. Vì vậy phải phân phối qua **App**, và người xem không được thêm vào workspace.

## 6. Cập nhật gần thời gian thực

Có ba lớp, chi tiết vì sao ở [devops/06 §5.3](../devops/06_de_den_dashboard.md):

**a. Push semantic model (tile vài giây):**
1. Workspace → *New → Streaming semantic model → API*. Tên `instacart_rt`.
2. Thêm các trường:
   - `minute` kiểu DateTime.
   - `orders`, `items` kiểu Number.
3. Bật *Historic data analysis*.
4. Copy **Push URL** rồi lưu vào Key Vault:
   ```bash
   az keyvault secret set --vault-name <kv> --name pbi-push-url --value '<url>'
   ```
   URL chứa khóa, không dán vào code hay chat.
5. Pipeline stream gửi dữ liệu vào ([04 §4.5](04_streaming.md)).
6. *New dashboard* `Instacart Live` → *Edit → Add a tile → Custom streaming data* → `instacart_rt` → Card `orders` và Line chart theo `minute`.
7. Ghim thêm các visual của trang 1 (*Pin visual*) để dashboard có cả số batch và số real-time.

**b. DirectQuery + auto page refresh (trang 5):**
- Trang 5 → *Format page → Page refresh* → bật, chọn 5 giây (chạy được trong Desktop khi trình chiếu).
- Trên Service (workspace Pro) giá trị tối thiểu bị giới hạn, khoảng 30 phút. Ghi rõ trong báo cáo.

**c. Import theo lịch:** các trang 1–4, xem mục 7.

## 7. Làm mới dữ liệu sau mỗi lần pipeline chạy

| Cách | Thiết lập | Khi dùng |
|---|---|---|
| **Scheduled refresh** (mặc định) | *Semantic model → Settings → Data source credentials* (OAuth2, tài khoản P3) → *Refresh* hằng ngày 04:00 | Luôn bật. `pl_master` chạy 02:00, xong trước 03:30 |
| Refresh qua API sau job | Task `refresh_bi` gọi `POST https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes` | Chỉ khi tenant cho phép service principal gọi API (spike 3.6) |

Nếu tenant chặn service principal thì `refresh_bi.py` chỉ ghi dòng "skipped" vào `ops.pipeline_runs`, còn scheduled refresh lo phần làm mới. Task `reconcile` đứng trước `refresh_bi` nên số sai không bao giờ được đẩy chủ động. Scheduled refresh thì vẫn chạy theo giờ, nên phải theo dõi `ops.reconciliation` ở trang 5.

## 8. Mobile layout, publish và chia sẻ

1. **Mobile layout:** với từng trang, chọn *View → Mobile layout*.
   - Kéo các thẻ KPI lên trên, xếp dọc, rồi đến 1–2 biểu đồ chính.
   - Bỏ visual phụ.
   - Page navigator dạng dọc.
2. **Publish:** *Home → Publish → instacart-bi*. Đặt tên semantic model `instacart`.
3. **App:** *Workspace → Create app → Instacart Analytics*.
   - Thêm audience *Quản lý ngành hàng* (thấy trang 1–4) và *Ban điều hành* (thấy cả trang 5). Từ G5, cả hai audience thấy thêm trang 6–7.
   - Thêm người xem theo email hoặc nhóm Entra.
4. **Không** dùng *Publish to web*, vì RLS sẽ mất ([devops/06 §5.1](../devops/06_de_den_dashboard.md)).
5. Mở app Power BI Mobile, đăng nhập bằng tài khoản người xem và chụp màn hình từng trang.

## 9. Checklist G4

- [ ] App có ≥ 4 trang. Page navigator, nút Back, bookmark và drill-through sang trang 2 đều hoạt động.
- [ ] Ảnh sản phẩm hiện trên trang 2 cho các sản phẩm `matched`.
- [ ] Người A (DeptManager, produce) và người B (DeptManager, dairy eggs) thấy trang 4 khác nhau; giảng viên (Executive) thấy hết. Có ảnh chụp.
- [ ] Mobile layout cho cả 4 trang, có ảnh chụp trên điện thoại.
- [ ] Demo 95/5: tile `Instacart Live` tăng trong ≤ 1 phút sau khi phát nốt; trang 5 cập nhật với auto page refresh trong Desktop.
- [ ] Trang 5 hiển thị độ trễ cùng / khác region từ `ops.streaming_latency`.
- [ ] Tổng Orders trên trang 1 = `SELECT sum(orders) FROM gold.kpi_daily` (khớp số liệu nguồn).
- [ ] File `powerbi/*.pbip` đã commit, không có `cache.abf` hay thông tin đăng nhập.

Trang 6–7 không thuộc G4; chúng được kiểm ở checklist G5 ([ds/05 §6](../ds/05_mlflow_serving_bao_cao.md)).
