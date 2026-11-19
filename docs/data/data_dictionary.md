# Từ điển dữ liệu và kế hoạch làm giàu dữ liệu

Tài liệu mô tả toàn bộ dữ liệu đầu vào của dự án: nguồn gốc, vai trò của từng bảng, ý nghĩa từng cột, dữ liệu mẫu, và cách làm giàu danh mục sản phẩm bằng Open Food Facts.

Schema dạng DBML để dán vào [dbdiagram.io](https://dbdiagram.io) nằm ở [source_data_schema_v1.dbml](source_data_schema_v1.dbml) (ảnh: [source_data_schema_v1.png](source_data_schema_v1.png)).

> Số dòng và dữ liệu mẫu của Instacart lấy theo bản Kaggle phổ biến. Khi tải bản Zenodo về, cần đối chiếu lại bằng `wc -l` và `head`. Dữ liệu mẫu của các bảng tự sinh chỉ mang tính minh họa.

## 1. Tổng quan: 4 loại dữ liệu


| Loại (theo yêu cầu Bài 08) | Nguồn                                                    | Bảng                                                                  | Định dạng                | Đường vào                                 |
| -------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------ | ----------------------------- | ----------------------------------------------- |
| Structured (file)              | Instacart 2017, tải từ Zenodo                           | `orders`, `order_products__prior`, `products`, `aisles`, `departments` | CSV                         | ADF Copy HTTP → `landing/instacart/`         |
| Structured (CSDL)              | Azure SQL, dữ liệu **tự sinh** bằng Python (có seed) | `product_prices`, `promotions`                                         | Bảng SQL → Parquet        | ADF Copy **CDC theo LSN** → `landing/retail_db/`   |
| Semi-structured + Unstructured | Open Food Facts REST API                                  | `off_products` (JSON), `product_images` (ảnh .jpg)                    | JSON, JPG                   | Crawler theo lô → `landing/off_api/`        |
| Streaming                      | Đơn `train` của Instacart, phát lại bằng simulator  | `order_events`                                                         | JSON qua Event Hubs (Kafka) | Structured Streaming → `bronze.order_events` |

### Dữ liệu nào là gốc, dữ liệu nào do nhóm tạo


| Dữ liệu                                       | Gốc hay tạo                           | Ghi chú                                                                                         |
| ------------------------------------------------- | ----------------------------------------- | -------------------------------------------------------------------------------------------------- |
| 6 file Instacart                                | **Gốc**                                | Instacart **tự chia** `prior` / `train` / `test`, nhóm không chia lại                        |
| `product_prices`, `promotions`                  | **Tự sinh**                            | Instacart không có giá. Doanh thu trên BI là giả lập                                      |
| `off_products`, ảnh                            | **Dữ liệu thật của nguồn ngoài**  | Nhóm chọn sản phẩm cần tra và ghép vào danh mục                                         |
| `order_events`                                  | **Đóng gói lại từ dữ liệu gốc** | Nội dung là đơn `train` gốc, chỉ thêm `event_time` giả lập và các trường kỹ thuật |
| `product_off_match`, `dim_product`, `baskets`… | **Dẫn xuất** trong ETL                | Xem mục 5                                                                                       |

### Vì sao Instacart có prior / train / test

Instacart lấy lịch sử của khoảng 206 nghìn khách, mỗi khách từ 4 đến 100 đơn:

- **prior**: mọi đơn **trước** đơn cuối cùng của mỗi khách, tức là lịch sử mua hàng.
- **Đơn cuối cùng** của mỗi khách được xếp vào một trong hai nhóm:
  - **train** (131.209 khách): có danh sách sản phẩm, nằm trong `order_products__train`.
  - **test** (75.000 khách): danh sách sản phẩm **bị giấu**, vì đây là đáp án của cuộc thi Kaggle.

Dự án dùng các nhóm như sau:

- `prior` đi **batch** và là dữ liệu train luật kết hợp.
- `train` đi **streaming** (đơn mới nhất của mỗi khách) và là tập **holdout** để đánh giá luật.
- `test` không có món hàng nên bị loại khỏi `baskets`. Header đơn vẫn giữ trong `silver.orders`, có gắn cờ.

## 2. Dữ liệu gốc Instacart

Quan hệ giữa các bảng: `departments` ← `products` → `aisles`; `products` ← `order_products__*` → `orders`.

### 2.1 `orders`: mỗi dòng là một đơn hàng

- **Quy mô:** 3.421.083 dòng, 206.209 khách.
- **Vai trò:** header của mọi đơn. Dùng cho KPI theo thứ/giờ, chu kỳ mua lại, và để gắn `user_id` cho giỏ hàng.


| Cột                     | Kiểu               | Ý nghĩa                                                                          |
| -------------------------- | --------------------- | ------------------------------------------------------------------------------------ |
| `order_id`               | int, PK             | Mã đơn, duy nhất                                                               |
| `user_id`                | int                 | Mã khách (đã ẩn danh)                                                         |
| `eval_set`               | string              | Nhóm của đơn: `prior` / `train` / `test`                                       |
| `order_number`           | int                 | Đơn thứ mấy của khách này (1, 2, 3…)                                       |
| `order_dow`              | int                 | Thứ trong tuần, 0–6. Instacart **không công bố** 0 là thứ mấy             |
| `order_hour_of_day`      | int                 | Giờ đặt đơn, 0–23                                                            |
| `days_since_prior_order` | float, null được | Số ngày kể từ đơn trước, chặn tối đa 30. **Rỗng ở đơn đầu tiên** |

```
order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order
2539329,1,prior,1,2,08,
2398795,1,prior,2,3,07,15.0
473747,1,prior,3,3,12,21.0
1187899,1,train,11,4,08,14.0
2168274,2,prior,1,2,11,
```

**Lưu ý:** không có cột ngày tháng. Phần streaming phải tự tạo `order_ts_sim` (xem mục 4).

### 2.2 `order_products__prior`: mỗi dòng là một món trong một đơn `prior`

- **Quy mô:** 32.434.489 dòng.
- **Vai trò:** nguồn chính để dựng giỏ hàng (`baskets`) và train luật kết hợp.


| Cột                | Kiểu                 | Ý nghĩa                                                |
| --------------------- | ----------------------- | ---------------------------------------------------------- |
| `order_id`          | int, FK → `orders`   | Món này thuộc đơn nào                              |
| `product_id`        | int, FK → `products` | Sản phẩm nào                                          |
| `add_to_cart_order` | int                   | Thứ tự bỏ vào giỏ                                   |
| `reordered`         | int (0/1)             | 1 nếu khách **từng mua** sản phẩm này trước đó |

```
order_id,product_id,add_to_cart_order,reordered
2,33120,1,1
2,28985,2,1
2,9327,3,0
2,45918,4,1
```

**Lưu ý:** không có cột số lượng. Mỗi sản phẩm chỉ xuất hiện một lần trong một đơn, phù hợp với bài toán giỏ hàng ("có mua hay không").

### 2.3 `order_products__train`: các món của đơn `train`

- **Quy mô:** 1.384.617 dòng, 131.209 đơn.
- **Cột:** giống hệt `order_products__prior`.
- **Vai trò:** **không nạp batch vào silver**. Simulator đóng gói thành sự kiện stream; sau khi qua stream, các món này trở thành `baskets` với `split = holdout`.

```
order_id,product_id,add_to_cart_order,reordered
1,49302,1,1
1,11109,2,1
1,10246,3,0
1,49683,4,0
```

### 2.4 `products`: danh mục sản phẩm

- **Quy mô:** 49.688 dòng.
- **Vai trò:** chuyển `product_id` thành tên dễ đọc trong luật kết hợp và BI. Đây là **bảng được làm giàu** bằng giá, khuyến mãi và Open Food Facts.


| Cột            | Kiểu                    | Ý nghĩa                                                                          |
| ----------------- | -------------------------- | ------------------------------------------------------------------------------------ |
| `product_id`    | int, PK                  | Mã sản phẩm                                                                     |
| `product_name`  | string                   | Tên sản phẩm (tiếng Anh, thị trường Mỹ). **Là khóa để ghép với OFF** |
| `aisle_id`      | int, FK → `aisles`      | Kệ hàng                                                                          |
| `department_id` | int, FK → `departments` | Ngành hàng                                                                       |

```
product_id,product_name,aisle_id,department_id
1,Chocolate Sandwich Cookies,61,19
2,All-Seasons Salt,104,13
3,Robust Golden Unsweetened Oolong Tea,94,7
24852,Banana,24,4
```

### 2.5 `aisles` (134 dòng) và `departments` (21 dòng)

- **Vai trò:** bảng tra cứu 2 cấp phân loại sản phẩm, với department là cấp cha. Dùng để gộp luật kết hợp theo nhóm hàng và làm KPI theo ngành hàng.


| Bảng         | Cột                               | Ý nghĩa                 |
| --------------- | ------------------------------------ | --------------------------- |
| `aisles`      | `aisle_id` (PK), `aisle`           | Mã và tên kệ hàng    |
| `departments` | `department_id` (PK), `department` | Mã và tên ngành hàng |

```
aisle_id,aisle                department_id,department
1,prepared soups salads       1,frozen
24,fresh fruits               4,produce
61,cookies cakes              7,beverages
94,tea                        19,snacks
```

## 3. Dữ liệu tự sinh trong Azure SQL

### Vì sao cần

Mục đích chính là **đáp ứng yêu cầu Bài 08 về nguồn CSDL quan hệ**. Instacart chỉ có file CSV tĩnh, nên không thể hiện được kỹ năng ingest từ cơ sở dữ liệu. Hai bảng này giúp thể hiện:

- **CDC từ CSDL:** bật Change Data Capture của SQL. ADF đọc nhật ký thay đổi (insert/update/delete) theo cửa sổ LSN, watermark lưu ở `etl.cdc_watermark` ([de/01 §3](../de/01_ingest_landing.md)). Dự phòng nếu không bật được CDC: watermark `updated_at` + soft delete.
- **Dữ liệu thay đổi theo thời gian:** nguồn chỉ giữ giá **hiện hành**; lịch sử giá được dựng ở silver bằng SCD Type 2 từ luồng CDC.
- **Join theo khoảng thời gian hiệu lực:** đơn hàng phải khớp với giá và khuyến mãi đang có hiệu lực tại thời điểm đặt (`order_ts_sim`).

Insight là lợi ích phụ: doanh thu giả lập theo ngành hàng, và câu hỏi "món đang khuyến mãi có hay được mua kèm không". Phần DS (FP-Growth, DApriori) **không phụ thuộc** vào hai bảng này.

`inventory` đã được **bỏ khỏi phạm vi**, vì ít giá trị nhất.

### Cách sinh

- Script Python cố định seed, để 4 thành viên sinh ra cùng một bộ dữ liệu.
- Giá gốc lấy theo phân phối riêng của từng department (ví dụ `produce` rẻ, `alcohol` đắt).
- Chạy theo **đợt** (`--wave`): đợt 0 insert toàn bộ (giá có hiệu lực từ `2015-01-01`); đợt 1–4 đổi giá khoảng 5% sản phẩm, thêm/gia hạn/**xóa** khuyến mãi, với `updated_at` = 01/03, 08/03, 15/03, 22/03/2026.
- Khuyến mãi áp cho khoảng 5–10% sản phẩm, mỗi đợt kéo dài 3–14 ngày.

### 3.1 `product_prices`: giá hiện hành (nguồn của CDC)

Bảng kiểu hệ thống bán hàng: **mỗi sản phẩm một dòng**, đổi giá là `UPDATE`. Lịch sử giá **không** lưu ở nguồn mà dựng ở `silver.product_prices_history` (SCD Type 2, cột `__START_AT` / `__END_AT`) từ luồng CDC ([de/03 §2](../de/03_silver_cdc_scd_dq.md)).


| Cột         | Kiểu                 | Ý nghĩa                                                            |
| -------------- | ----------------------- | ---------------------------------------------------------------------- |
| `product_id` | int, PK, FK → `products` | Sản phẩm                                                           |
| `unit_price` | decimal(10,2)         | Giá bán hiện hành (USD)                                         |
| `currency`   | char(3)               | Đơn vị tiền tệ, mặc định `USD`                               |
| `updated_at` | datetime2             | Thời điểm đổi giá theo lịch giả lập. Silver dùng làm `SEQUENCE BY` → khoảng hiệu lực |

```
-- Bảng nguồn sau đợt 3 (một dòng)
product_id,unit_price,currency,updated_at
24852,0.27,USD,2026-03-15 00:00:00

-- silver.product_prices_history (dựng từ CDC)
product_id,unit_price,__START_AT,__END_AT
24852,0.29,2015-01-01,2026-03-01
24852,0.25,2026-03-01,2026-03-15
24852,0.27,2026-03-15,NULL
```

### 3.2 `promotions`: chương trình khuyến mãi


| Cột           | Kiểu                 | Ý nghĩa                              |
| ---------------- | ----------------------- | ---------------------------------------- |
| `promo_id`     | varchar, PK           | Mã chương trình                    |
| `product_id`   | int, FK → `products` | Sản phẩm được khuyến mãi        |
| `promo_type`   | varchar               | `percent_off` / `bogo` (mua 1 tặng 1) |
| `discount_pct` | int                   | % giảm (với `percent_off`)           |
| `start_date`   | date                  | Ngày bắt đầu                       |
| `end_date`     | date                  | Ngày kết thúc                       |
| `updated_at`   | datetime              | Thời điểm sửa; CDC xếp thứ tự theo LSN, không theo cột này |

```
promo_id,product_id,promo_type,discount_pct,start_date,end_date,updated_at
P0001,24852,percent_off,15,2026-03-01,2026-03-07,2026-02-25 10:00:00
P0002,1,bogo,0,2026-03-10,2026-03-16,2026-03-05 09:00:00
```

## 4. Dữ liệu streaming: `order_events`

### Vì sao cần

Bài 08 yêu cầu luồng streaming qua Event Hubs. Đơn `train` là ứng viên tự nhiên vì nó là **đơn mới nhất** của mỗi khách: lịch sử đi batch, đơn mới đến dưới dạng stream.

### Cấu trúc sự kiện

Mỗi đơn là một sự kiện, gồm header từ `orders` và mảng `items` từ `order_products__train`.


| Trường                               | Nguồn                         | Ý nghĩa                                                                              |
| ---------------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| `event_id`                             | Tạo                           | `order-<order_id>`, cố định, dùng để khử trùng khi phát lại                  |
| `event_type`                           | Tạo                           | `order_placed`                                                                         |
| `schema_version`                       | Tạo                           | Phiên bản schema sự kiện                                                           |
| `order_id` … `days_since_prior_order` | Gốc (`orders`)                | Như mục 2.1                                                                          |
| `event_time`                           | Tạo                           | Thời điểm giả lập (= `order_ts_sim`), dùng cho watermark và cửa sổ thời gian |
| `replay_run_id`                        | Tạo                           | Mã lần phát lại, để phân biệt các lần demo                                   |
| `produced_at`, `producer_location`     | Tạo                           | Đồng hồ thật lúc gửi và nơi chạy simulator; dùng đo độ trễ ([de/04 §4.4](../de/04_streaming.md)) |
| `items[]`                              | Gốc (`order_products__train`) | `product_id`, `add_to_cart_order`, `reordered`                                         |

```json
{"event_id": "order-1187899", "event_type": "order_placed", "schema_version": 1,
 "order_id": 1187899, "user_id": 1, "order_number": 11, "order_dow": 4,
 "order_hour_of_day": 8, "days_since_prior_order": 14,
 "event_time": "2026-03-30T08:00:00Z", "replay_run_id": "run-03",
 "items": [{"product_id": 196, "add_to_cart_order": 1, "reordered": 1}]}
```

### Thời gian giả lập `order_ts_sim`

Dữ liệu gốc không có ngày tháng, nên thời gian được dựng như sau:

1. Đặt đơn cuối (`train` hoặc `test`) của mỗi khách vào ngày tất định
   `2026-03-01 + (user_id × 7919 mod 30)`, tại giờ `order_hour_of_day`, theo UTC.
2. Tính lùi các đơn `prior` bằng cách trừ dần `days_since_prior_order`. Giờ trong ngày lấy theo `order_hour_of_day`.

Hàm Python và SQL dùng chung ở `src/common/sim_time.py`; `days_back` chỉ cộng khoảng cách
của các đơn **sau** đơn đang xét, không cộng khoảng cách của chính nó.

Hệ quả cho BI:

- Biểu đồ **theo thứ/giờ luôn dùng cột gốc** (`order_dow`, `order_hour_of_day`).
- `order_ts_sim` chỉ dùng cho trục thời gian, cho phép join với giá và khuyến mãi, và cho watermark.

## 5. Làm giàu danh mục sản phẩm bằng Open Food Facts

### 5.1 Vì sao cần

- **Đáp ứng Bài 08:** đây là nguồn **REST API**, trả **JSON lồng nhau** (semi-structured) và **ảnh** (unstructured). Một nguồn phủ được 2 trong 4 loại dữ liệu.
- **Kỹ thuật DE thể hiện được:** gọi API có giới hạn tốc độ, crawl theo lô tăng dần, làm phẳng JSON, xử lý schema thay đổi, và ghép dữ liệu khi không có khóa chung (fuzzy matching).
- **Insight:** thêm thông tin dinh dưỡng, thương hiệu và mức độ chế biến cho sản phẩm. Từ đó trả lời được các câu hỏi như "giỏ hàng có xu hướng lành mạnh hay không" hay "luật kết hợp có hay nối đồ ăn vặt với đồ uống có đường không".

### 5.2 API đã kiểm tra

Đã gọi thử ngày 2026-11-15, cả hai endpoint trả HTTP 200.


| Endpoint                                                                       | Dùng khi                                                                     | Giới hạn (theo tài liệu OFF) |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ---------------------------------- |
| `GET https://search.openfoodfacts.org/search?q=<tên>&fields=...`              | Tìm theo **tên**. Bắt buộc phải dùng vì Instacart không có mã vạch | Khoảng 10 lần/phút            |
| `GET https://world.openfoodfacts.org/api/v2/product/<barcode>.json?fields=...` | Lấy chi tiết (thành phần, dinh dưỡng) khi đã có mã vạch            | Khoảng 100 lần/phút           |

Cần gửi `User-Agent` mô tả ứng dụng, và tôn trọng giới hạn tốc độ (gặp HTTP 429 thì chờ rồi thử lại).

Một kết quả thật khi tìm "Chocolate Sandwich Cookies" (`product_id = 1`):

```json
{"code": "0019320015403", "product_name": "Oreos", "brands": ["Nabisco"],
 "categories_tags": ["en:snacks", "en:biscuits", "en:chocolate-sandwich-cookies"],
 "countries_tags": ["en:france", "en:united-states"],
 "nutriscore_grade": "e", "nova_group": 4,
 "image_url": "https://images.openfoodfacts.org/images/products/001/932/001/5403/front_en.30.400.jpg"}
```

### 5.3 `off_products`: các trường giữ lại


| Trường                       | Kiểu        | Ý nghĩa                                                                      |
| -------------------------------- | -------------- | -------------------------------------------------------------------------------- |
| `code`                         | string, PK   | Mã vạch (EAN/UPC), khóa của OFF                                            |
| `product_name`                 | string       | Tên sản phẩm trên OFF                                                      |
| `brands`                       | array/string | Thương hiệu                                                                 |
| `categories_tags`              | array        | Danh mục phân cấp, ví dụ `en:snacks` → `en:biscuits`                     |
| `countries_tags`               | array        | Quốc gia có bán. Dùng để lọc `en:united-states`                         |
| `nutriscore_grade`             | string       | Điểm dinh dưỡng `a` (tốt) → `e` (kém), hoặc `unknown`                  |
| `nova_group`                   | int          | Mức độ chế biến: 1 (tự nhiên) → 4 (siêu chế biến)                   |
| `nutriments.energy-kcal_100g`  | float        | Năng lượng trên 100g                                                       |
| `nutriments.sugars_100g`       | float        | Đường trên 100g                                                            |
| `nutriments.fat_100g`          | float        | Chất béo trên 100g                                                          |
| `ingredients_text`             | string       | Thành phần (lấy từ endpoint chi tiết)                                     |
| `image_url`                    | string       | Link ảnh mặt trước, dùng để tải ảnh                                   |
| `_search_query`, `_product_id` | metadata     | Tên Instacart đã dùng để tìm và `product_id` gốc, do crawler ghi kèm |

### 5.4 `product_images`: ảnh sản phẩm (unstructured)

- Crawler tải ảnh từ `image_url` vào `landing/off_api/images/<code>.jpg`.
- Databricks đọc bằng `spark.read.format("binaryFile")`.


| Cột                        | Ý nghĩa                           |
| ----------------------------- | ------------------------------------- |
| `off_code`                  | Mã vạch OFF, FK → `off_products` |
| `path`                      | Đường dẫn file trên ADLS       |
| `length`                    | Kích thước (byte)                |
| `width`, `height`, `format` | Lấy từ metadata ảnh              |
| `modificationTime`          | Thời điểm file được ghi       |

### 5.5 Crawl theo lô tăng dần, không phải streaming

- **Không crawl hết** 49.688 sản phẩm: với khoảng 10 lần tìm/phút, việc đó mất khoảng 83 giờ.
- **Phạm vi:** top khoảng 2.000 sản phẩm phổ biến nhất (theo số lần xuất hiện trong `order_products__prior`), khoảng 3,5 giờ gọi API. Nhóm này chiếm phần lớn số món trong các giỏ hàng.
- **Mỗi lần chạy lấy một lô**, ví dụ 300 sản phẩm chưa tra. Mỗi lô ghi **một file JSON mới**: `<off_root>/products/batch=<plan_hash>/batch_<content_hash>.json`.
- **Bảng trạng thái `off_crawl_state`** ghi sản phẩm nào đã tra, kết quả thế nào. Lần chạy sau bỏ qua sản phẩm đã xong và thử lại những sản phẩm bị lỗi.
- `off_crawl_state.batch_id STRING` lưu hash kế hoạch của lô đã xử lý. `batch_file` trỏ JSONL đã công bố nếu có hit.
- **Auto Loader** tự nhặt file mới vào `bronze.off_products`, như mọi nguồn file khác.
- **Nơi chạy crawler:** chọn một trong hai.
  - Python trong một task của Databricks job: đơn giản, dễ viết logic chấm điểm.
  - Vòng `Until` + `Wait` trong ADF: đúng tinh thần "ADF ingest REST" của Bài 08 hơn.
- **Phương án dự phòng:** OFF có bản dump toàn bộ (JSONL/Parquet). Dùng bản này nhanh hơn, nhưng mất phần gọi API.

### 5.6 Cách join: ghép theo tên, qua bảng cầu nối

Instacart và OFF **không có khóa chung**, vì Instacart không có mã vạch. Vì vậy phải ghép theo tên và lưu kết quả vào bảng cầu nối `silver.product_off_match`.

![Ghép tên Instacart ↔ OFF và dựng silver.dim_product](product_enrichment_join.png)

File gốc để sửa: [product_enrichment_join.drawio](product_enrichment_join.drawio) (mở bằng app draw.io hoặc app.diagrams.net).

**Quy trình chấm điểm:**

1. Chuẩn hóa tên: chữ thường, bỏ dấu câu, bỏ các từ nhiễu như `organic`, `bag of`, đơn vị đo.
2. Lọc kết quả có `countries_tags` chứa `en:united-states`.
3. Tính độ giống tên bằng `rapidfuzz.token_set_ratio`, cộng điểm nếu thương hiệu xuất hiện trong tên Instacart.
4. Gắn nhãn:
   - `matched`: điểm ≥ 85.
   - `low_confidence`: điểm 70–84. Lưu lại nhưng không đưa vào BI.
   - `no_match`: điểm < 70, hoặc không có kết quả nào.

**Thử nghiệm trên 4 tên** cho thấy cần bước chấm điểm này:


| Tên Instacart                       | Kết quả OFF đầu tiên                  | Đánh giá              |
| -------------------------------------- | -------------------------------------------- | -------------------------- |
| Banana                               | Fresh Banana                               | Đúng                   |
| Robust Golden Unsweetened Oolong Tea | Oolong Tea (Pokka)                         | Gần đúng, khác hãng |
| Organic Strawberries                 | Yaourt fraise biologique (sữa chua Pháp) | Sai                      |
| Bag of Organic Bananas               | Plastic Bag                                | Sai                      |

#### Đo trên dev

Ngày 2026-11-19, trên lô dev gồm 305 sản phẩm đã crawl, có **279 sản phẩm** với ít nhất một ứng viên Mỹ.
Commit đo là `508a8a4`. [Update batch dev](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/55bda8da-ae3f-4a14-be01-028193fdf91d) hoàn tất với `enable_off=true`.

| Nhãn | Toàn bridge (49.688) | Tỷ lệ | Đã crawl và có ứng viên Mỹ (279) | Tỷ lệ |
|---|---:|---:|---:|---:|
| `matched` | 259 | 0,5213% | 259 | 92,8315% |
| `low_confidence` | 8 | 0,0161% | 8 | 2,8674% |
| `no_match` | 49.421 | 99,4626% | 12 | 4,3011% |

Bridge có **49.688 dòng / 49.688 khóa**. Số khóa bằng `bronze.products`.
`off_products_clean` có **1.525 dòng** và `quarantine_off_products` có **0 dòng**.

Bảng cầu nối giữ **một dòng cho mỗi `product_id` Instacart**, kể cả chưa crawl/chỉ có hit ngoài Mỹ:
những sản phẩm đó nhận `off_code=NULL`, `off_match_score=0`, `off_match_status=no_match`.
Ứng viên dưới 70 vẫn giữ mã/điểm với nhãn `no_match` để kiểm tra. Khi bằng điểm, chọn mã OFF tăng dần.
Ngưỡng 85/70 áp dụng trên điểm DOUBLE chưa làm tròn. Bonus 5 điểm chỉ dùng thương hiệu đầu tiên
xuất hiện nguyên cụm từ trong tên Instacart, chặn trên 100; tên chuẩn hóa rỗng luôn nhận 0.

Khi có dữ liệu thật, ghi **đủ ba nhãn** (`matched`, `low_confidence`, `no_match`, kể cả nhãn có 0 dòng)
và tỉ lệ với mẫu số là **toàn bộ bảng cầu nối** (49.688 sản phẩm nếu dùng release đầy đủ).
Báo riêng số sản phẩm đã crawl/đủ ứng viên Mỹ để phân biệt độ phủ crawler với chất lượng ghép;
không suy ra `no_match` nghĩa là đã tra nhưng không thấy. Lưu commit, pipeline/update ID,
nguồn bronze/phiên bản và thời điểm đo. Hướng dẫn lấy số và đối soát tại
[nghiệm thu OFF](../de/03d_nghiem_thu_off.md). Tỉ lệ thấp vẫn chấp nhận được nếu báo cáo trung thực.

### 5.7 Làm giàu được những gì


| Thông tin thêm vào `dim_product`           | Từ nguồn       | Dùng cho                                                                                |
| ----------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------ |
| `current_price`, doanh thu giả lập          | `product_prices` | KPI doanh thu theo ngành hàng (ghi rõ là giả lập)                                  |
| `on_promotion`, `discount_pct`                | `promotions`     | So sánh tỉ lệ mua kèm khi có và không có khuyến mãi                            |
| `brand`                                       | OFF              | Top thương hiệu; luật kết hợp ở cấp thương hiệu                               |
| `nutriscore_grade`, `nova_group`              | OFF              | Mức độ "lành mạnh" của giỏ hàng; luật nào nối đồ siêu chế biến với nhau |
| `energy_kcal_100g`, `sugars_100g`, `fat_100g` | OFF              | Profile dinh dưỡng trung bình theo ngành hàng / khung giờ                          |
| `off_categories`                              | OFF              | Phân loại chi tiết hơn aisle                                                         |
| `image_path`                                  | OFF (ảnh)       | Hiển thị ảnh sản phẩm trên dashboard / trang gợi ý                               |
| `off_match_status`, `off_match_score`         | Bảng cầu nối  | Đo tỉ lệ làm giàu thành công, lọc dữ liệu kém tin cậy                        |

### 5.8 Giấy phép

- Dữ liệu OFF theo **ODbL**, ảnh theo **CC-BY-SA**: phải ghi nguồn trong báo cáo.
- Dữ liệu Instacart chỉ dùng cho mục đích phi thương mại: cũng phải ghi nguồn.

## 6. Các bảng silver liên quan


| Bảng                      | Hình thành từ                                                    | Vai trò                                                                    |
| ---------------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `silver.orders`            | **View**: `orders_batch` (prior/test) ∪ `stream_orders` (train) | Header đơn chuẩn hóa, thêm `source` và `order_ts_sim`                 |
| `silver.order_items`       | **View**: `order_items_prior` ∪ `stream_order_items`                | Các món trong đơn, thêm `source`                                       |
| `silver.product_prices_history` | CDC của `product_prices` (`AUTO CDC`, SCD2)                | Lịch sử giá, dùng cho join theo thời điểm                         |
| `silver.product_off_match` | Kết quả chấm điểm ghép tên                                   | Bảng cầu nối Instacart ↔ OFF                                            |
| `silver.dim_product`       | `products` + `aisles` + `departments` + giá + khuyến mãi + OFF   | Danh mục sản phẩm đã làm giàu                                        |
| `silver.baskets`           | `order_items` gom theo đơn                                        | Đầu vào phần DS: `split = prior` để train, `holdout` để đánh giá |
| `silver.quarantine_*`      | Bản ghi vi phạm luật chất lượng dữ liệu (mỗi nguồn một bảng) | Lưu lỗi kèm lý do (`_dq_failed`), không xóa im lặng                  |

### 6.1 Hợp đồng `silver.dim_product` (AD-DE-14)

Một dòng mỗi `product_id` Instacart; release đầy đủ có 49.688 sản phẩm. Thiếu nguồn làm giàu
vẫn giữ sản phẩm nhờ LEFT JOIN. Cột chính:

| Nhóm | Cột / ý nghĩa |
|---|---|
| Danh mục | `product_id`, `product_name`, `aisle_id`, `aisle`, `department_id`, `department` |
| Giá | `current_price` DECIMAL(10,2), `currency` từ hàng SCD2 `__END_AT IS NULL`; thiếu thì NULL |
| Khuyến mãi | `discount_pct` lớn nhất trong các chương trình phủ `as_of_date`; `on_promotion` BOOL; không có thì NULL/false |
| Độ tin cậy OFF | `off_match_score`, `off_match_status` giữ cả matched/low_confidence/no_match; thiếu bridge thành 0/no_match |
| OFF tin cậy | `off_code`, `off_product_name`, `brand`, `off_categories`, `countries_tags`, `nutriscore_grade`, `nova_group`, `energy_kcal_100g`, `sugars_100g`, `fat_100g`, `image_url` — chỉ làm giàu khi matched |
| Ảnh | `image_path` từ metadata ảnh của barcode đã matched; thiếu/không matched thì NULL |
| Kỹ thuật | `dup_cnt` = 1 và `_join_sources_unique` = true khi update thành công; không dùng làm measure BI |

OFF ghép theo cặp product_id/off_code; cùng barcode không cho phép lấy payload của sản phẩm khác.
Giá là giá **hiện hành**, không phải giá lịch sử tại ngày đặt đơn; xem AD-DE-22 cho join theo thời điểm.
Ngày cấu hình chỉ quyết định khuyến mãi; khuyến mãi 0% vẫn có on_promotion=true.

Đã nghiệm thu trên dev với **49.688 dòng và 49.688 khóa** ở cả hai chế độ. Khi OFF tắt,
mọi thuộc tính OFF/ảnh là NULL, trạng thái là `no_match` và điểm bằng 0.
Số đo `has_price` và update ID:
[nghiệm thu dim_product](../de/03e_nghiem_thu_dim_product.md).

Guard của `silver.dim_product`: `dup_cnt` là số dòng sau join cho mỗi product_id,
`_join_sources_unique` kiểm trùng nguồn giá/OFF/bridge, `_catalog_sources_unique`
cảnh báo nhiều payload CSV trong nhóm được ưu tiên, và `_catalog_sources_unrescued` cảnh báo
khi phiên bản danh mục được chọn có rescue.

### 6.2 View hợp nhất và `silver.baskets` (AD-DE-15)

`silver.orders` có order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
days_since_prior_order, order_ts_sim, source và is_test. Batch giữ prior/test; stream đổi event_time
thành order_ts_sim và gắn train/stream/is_test=false. `silver.order_items` có order_id, product_id,
add_to_cart_order, reordered, source. Cả hai UNION ALL, không tự khử trùng hay tạo bản train batch.

Baskets có năm cột nghiệp vụ. `order_id INT` là khóa, `user_id INT` là mã khách,
`split` nhận `prior` hoặc `holdout`, `items ARRAY<INT>` chứa mã sản phẩm duy nhất theo thứ tự tăng dần,
và `n_items INT = size(items)` đếm số sản phẩm khác nhau. Header test không tạo giỏ.
`_items_valid` kiểm tính hợp lệ của mã sản phẩm, `_source_disjoint` kiểm mã đơn holdout không xuất hiện
ở batch, còn `dup_cnt` đếm giỏ theo mã đơn. Ba cột kỹ thuật phục vụ kiểm chất lượng.
Header holdout đang chờ món được cảnh báo riêng. Snapshot AD-DE-20 kiểm miền số trước khi cast về INT.

Mục tiêu full release sau khi phát đủ stream là **3.214.874 giỏ prior** và **131.209 giỏ holdout**.
Còn chờ AD-ST-05 (#68) để chạy hai view trên warehouse và đối soát số giỏ thật theo
[runbook nghiệm thu](../de/03f_nghiem_thu_baskets.md).

## 7. Dữ liệu cho Data Science

Chi tiết cách dựng ở [de/08](../de/08_du_lieu_cho_ds.md); cách dùng ở [ds/01](../ds/01_du_lieu_thuc_nghiem.md).

| Bảng | Hạt | Cột chính | Vai trò |
| --- | --- | --- | --- |
| `ds_data.data_versions` | phiên bản | `data_version`, `created_at`, `n_prior`, `n_holdout`, `source` | Theo dõi snapshot đã cố định đủ giỏ, danh mục và thứ tự holdout |
| `ds_data.baskets` | phiên bản × đơn | `data_version`, `order_id`, `split`, `items` (product_id) | Bản cố định của `silver.baskets`. `v1` không bao giờ bị ghi đè |
| `ds_data.ds_prepare_products` | phiên bản × sản phẩm | `product_id`, tên sản phẩm, khóa và tên aisle/department | Danh mục nội bộ cố định cho việc tính lại index và transactions |
| `ds_data.ds_prepare_holdout` | phiên bản × đơn holdout | `order_id`, `ordered_products` (product_id theo vị trí bỏ vào giỏ) | Đầu vào nội bộ để tính lại context/targets từ thứ tự ban đầu |
| `ds_data.item_index` | phiên bản × cấp × item | `grain` (`product`/`aisle`/`department`), `item_key`, `idx`, `freq_prior` | `idx` đánh từ 0 theo tần suất giảm dần trong `prior`; "top-N item" = `idx < N` |
| `ds_data.transactions` | phiên bản × cấp × đơn | `items` (mảng `idx` tăng dần), `n_items`, `scale_bucket` (0–99) | Đầu vào của DApriori, DATID, FP-Growth. `scale_bucket < p` lấy p% số giỏ, các tập con lồng nhau |
| `ds_data.holdout_eval` | phiên bản × đơn holdout | `eval_part` (`val`/`test`), `context`, `targets` | Đánh giá gợi ý: nửa đầu giỏ theo thứ tự bỏ vào làm ngữ cảnh, nửa sau làm đáp án |
| `ml.experiment_runs` | một lần chạy thực nghiệm | `run_key`, `experiment` (E1–E7), `platform`, `algo`, tham số, thời gian, bộ nhớ, số tập / số luật | Nguồn sự thật của báo cáo thực nghiệm. `platform` phân biệt Databricks, Spark on YARN, MapReduce |
| `ml.frequent_itemsets`, `ml.association_rules` | `run_key` × tập / luật | `items` / `antecedent`, `consequent`, `support`, `confidence`, `lift` | Chỉ lưu cho các lần chạy E7 (và lần được đánh dấu `persist`) |
| `ml.holdout_metrics` | `run_key` × `min_confidence` × `eval_part` × k | HitRate, Precision, Recall @k, coverage | Chọn champion; so với baseline phổ biến |
| `gold.association_rules`, `gold.recommendations`, `gold.experiment_metrics`, `gold.holdout_metrics` | View | Luật có tên sản phẩm; top 10 món mua kèm; trung vị + speedup | Power BI trang 2, 6, 7 |

## 8. Kết quả chất lượng `ops.dq_results` (AD-DE-16)

Bảng Delta thường gồm: `update_id STRING`, `pipeline STRING` (ưu tiên ID), `flow STRING`,
`dataset STRING`, `expectation STRING`, `passed BIGINT`, `failed BIGINT`, `at TIMESTAMP_LTZ`, đều NOT NULL.
`pipeline_name STRING` có thể NULL nếu event log không cung cấp tên và nằm ngoài khóa MERGE.
Power BI hiển thị tên này, fallback về `pipeline` khi thiếu.
Grain/khóa MERGE là **pipeline × update_id × flow × dataset × expectation**. Dataset bắt buộc vì
cùng flow có thể có một luật cùng tên trên nhiều view. Đếm lại cùng update/log không nhân dòng.

Passed/failed là tổng counter đã emit, không phải cardinality cuối của bảng silver. Thu cả micro-batch
RUNNING, loại event ID trùng; không dựng zero khi thiếu metrics. Một bản ghi có thể vi phạm nhiều luật,
view có thể được tính trong nhiều flow; SUM failed không phải số dòng quarantine. Khi fail dừng update,
xem ERROR/violation thay vì suy ra số đếm không được emit. `at` là max event timestamp, không phải lúc thu.

Đã nghiệm thu batch trên warehouse dev với **47 khóa expectation**. Hai lượt MERGE
giữ nguyên kết quả và đã đối chiếu bốn bảng quarantine. Còn chờ event log stream.
Hợp đồng, truy vấn điều tra và nghiệm thu:
[DE/06 §3](../de/06_van_hanh_chay_lai.md#3-event-log-và-opsdq_results),
[hướng dẫn collect_dq](../de/06b_nghiem_thu_dq.md).

## 9. Cấu hình và kết quả đối soát (AD-DE-17)

`ops.expected_counts` là bảng Delta thường, khóa logic profile/metric, có expected BIGINT không âm,
source và configured_at. Chín metric: orders, order_products_prior, order_products_train, products,
aisles, departments, orders_prior, orders_train, orders_test. Profile full seed tham chiếu không thay
cho xác minh Zenodo; mẫu phải dùng manifest nguồn, không lấy số actual bronze làm chuẩn.

`ops.reconciliation` có khóa logic **check_date DATE / check_name STRING**. Cột nghiệp vụ expected/actual
BIGINT, passed BOOL, checked_at TIMESTAMP_LTZ; metadata run_id/profile/stage/config_hash STRING để phân
biệt phạm vi và ngăn dùng lại check xanh cũ. Mọi cột NOT NULL, mỗi check lưu kết quả mới nhất trong ngày.
Tách 22 batch / 25 G3 / 3 G4, không ghi stage chưa chạy như thể đã đạt. Hàng failed vẫn được lưu khi
assert cuối làm task đỏ; repair cập nhật cùng khóa. Kết quả cùng run/hash mới đủ điều kiện mở cổng.

Đã đối soát full release trên warehouse dev. Hai lượt đều ghi **22 check batch xanh** và giữ nguyên số khóa.
Lượt thử sai kỳ vọng lưu được check đỏ. G3/G4 còn chờ đủ nguồn Silver và Gold.
Cách cấu hình, thời gian và statement ID:
[DE/06 §4](../de/06_van_hanh_chay_lai.md#4-đối-soát-tự-động-opsreconciliation),
[06c_nghiem_thu_reconciliation.md](../de/06c_nghiem_thu_reconciliation.md).
