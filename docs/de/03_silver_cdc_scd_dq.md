# 03 · Tầng silver: CDC → SCD, kiểm chất lượng, chuẩn hóa (bronze → silver)

**Đầu vào:**
- Bronze đã đạt đối soát ([02](02_bronze_ldp.md)).
- Phần stream (`silver.stream_orders`, `silver.stream_order_items`) do pipeline `instacart_stream` tạo ([04](04_streaming.md)).

**Đầu ra** (schema `silver`):

| Bảng | Kiểu | Kỹ thuật |
|---|---|---|
| `product_prices_history` | Streaming table | `AUTO CDC` → **SCD Type 2** |
| `promotions` | Streaming table | `AUTO CDC` → **SCD Type 1**, áp dụng delete |
| `order_time_sim` | Materialized view | Tính `order_ts_sim` tất định |
| `orders_batch` | Materialized view | Loại trùng, gắn thời gian |
| `order_items_prior` | Streaming table | `AUTO CDC` SCD1 theo khóa (`order_id`, `product_id`) |
| `quarantine_*` | Streaming table | Bản ghi vi phạm luật, kèm lý do |
| `off_products_clean`, `product_off_match` | Materialized view | Làm phẳng JSON, fuzzy match |
| `dim_product` | Materialized view | Danh mục đã làm giàu |
| `orders`, `order_items` | **View** UC | Gộp batch ∪ stream |
| `baskets` | Materialized view | Đầu vào DS |

**Xong khi:** đạt checklist G3 ở mục 9. Sau đó chạy job `ds-prepare` ([08](08_du_lieu_cho_ds.md)) rồi **DS bắt đầu**.

---

## 1. Ba loại bảng của LDP và khi nào dùng

| Loại | Dữ liệu được xử lý | Chạy lại | Dùng cho |
|---|---|---|---|
| **Streaming table** (`@dp.table` trả `readStream`, hoặc `create_streaming_table` + flow) | Chỉ dữ liệu **mới** kể từ lần trước (có checkpoint) | Dòng cũ không bị xử lý lại | Nguồn lớn và chỉ tăng; đích của `AUTO CDC` |
| **Materialized view** (`@dp.materialized_view`) | Kết quả của câu truy vấn trên **toàn bộ** nguồn | Tính lại. Trên classic là tính lại toàn bộ; serverless có thể tính tăng dần | Join, window, tổng hợp, bảng nhỏ và vừa |
| **View** tạm (`@dp.temporary_view`) | Không lưu, chỉ là bước trung gian trong pipeline | — | Nguồn của `AUTO CDC`, nơi đặt expectations |

## 2. CDC → SCD Type 2: lịch sử giá

### 2.1 SCD Type 1 và Type 2

Lấy ví dụ sản phẩm 24852 (Banana), đổi giá hai lần:

```
Luồng CDC (bronze.product_prices_cdc):
  _op=0  product_id=24852  unit_price=0.29  updated_at=2015-01-01   (snapshot)
  _op=4  product_id=24852  unit_price=0.25  updated_at=2026-03-01   (wave 1)
  _op=4  product_id=24852  unit_price=0.27  updated_at=2026-03-15   (wave 3)

SCD Type 1 → chỉ giữ giá mới nhất:
  product_id=24852  unit_price=0.27

SCD Type 2 → giữ mọi phiên bản, có khoảng hiệu lực:
  product_id  unit_price  __START_AT   __END_AT
  24852       0.29        2015-01-01   2026-03-01
  24852       0.25        2026-03-01   2026-03-15
  24852       0.27        2026-03-15   NULL          ← phiên bản hiện hành
```

SCD2 trả lời được câu hỏi "**đơn đặt ngày 05/03 trả giá bao nhiêu**": tìm phiên bản có `__START_AT ≤ order_ts_sim < __END_AT`.

![CDC từ Azure SQL đến SCD2](diagrams/02_cdc_scd2.png)

<sub>File gốc: [diagrams/de_diagrams.drawio](diagrams/de_diagrams.drawio), trang 2.</sub>

### 2.2 Code

```python
# src/pipelines/batch/silver_retail.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.retail import PRICE_CHANGE_RULES, PROMO_CHANGE_RULES, assess_changes, deduplicate_changes

CAT    = spark.conf.get("catalog")
BRONZE = f"{CAT}.{spark.conf.get('bronze')}"
SILVER = f"{CAT}.{spark.conf.get('silver')}"

@dp.temporary_view(name="product_prices_valid")
@dp.expect_all_or_drop(PRICE_CHANGE_RULES)
def product_prices_valid():
    return assess_changes(spark.readStream.table(f"{BRONZE}.product_prices_cdc"), "prices")


@dp.temporary_view(comment="Thay đổi giá đã qua DQ rồi loại bản sao; nguồn của AUTO CDC")
def product_prices_changes():
    return deduplicate_changes(spark.readStream.table("product_prices_valid"), "prices")


dp.create_streaming_table(
    name=f"{SILVER}.product_prices_history",
    comment="SCD2: lịch sử giá, mỗi phiên bản có __START_AT/__END_AT",
    table_properties={"delta.enableChangeDataFeed": "true"})

dp.create_auto_cdc_flow(
    target=f"{SILVER}.product_prices_history",
    source="product_prices_changes",
    keys=["product_id"],
    sequence_by=F.col("updated_at"),               # thời gian nghiệp vụ → __START_AT có nghĩa
    except_column_list=["_op", "_lsn", "_seqval"],
    stored_as_scd_type=2,
    track_history_column_list=["unit_price", "currency"],   # đổi cột khác không sinh phiên bản mới
)
```

Bản SQL tương đương, viết vào báo cáo cho dễ đọc:

```sql
CREATE OR REFRESH STREAMING TABLE product_prices_history;

CREATE FLOW prices_scd2 AS AUTO CDC INTO product_prices_history
FROM stream(product_prices_changes)
KEYS (product_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (_op, _lsn, _seqval)
STORED AS SCD TYPE 2
TRACK HISTORY ON unit_price, currency;
```

**`AUTO CDC` lo những việc mà viết MERGE tay dễ sai:**
- **Dữ liệu đến sai thứ tự:** thay đổi của wave 3 đến trước wave 2 vẫn được xếp đúng nhờ `SEQUENCE BY`.
- **Đóng phiên bản cũ:** gán `__END_AT` cho phiên bản cũ và mở phiên bản mới trong cùng một giao dịch.
- **Idempotent:** áp dụng lại cùng một thay đổi (cùng khóa, cùng `SEQUENCE BY`) không tạo phiên bản mới.

Giá chỉ truyền snapshot, insert và update-after vào SCD2. `no_price_delete` đưa op=1 vào
quarantine vì delete SQL có thể mang lại cùng `updated_at` của bản trước.

**Vì sao `SEQUENCE BY updated_at` mà không dùng `_lsn`:**
- Cả hai đều đúng thứ tự.
- Nhưng `__START_AT` / `__END_AT` lấy giá trị từ cột sequence. Dùng `updated_at` thì khoảng hiệu lực là **ngày giả lập**, join được với `order_ts_sim`. Dùng LSN thì chỉ ra một chuỗi hex.
- Điều kiện để dùng được: mỗi sản phẩm không đổi giá hai lần trong cùng một `updated_at`. Script seed đảm bảo điều này.

### 2.3 Promotions: SCD Type 1 có xóa

```python
@dp.temporary_view(name="promotions_valid")
@dp.expect_all_or_drop(PROMO_CHANGE_RULES)
def promotions_valid():
    return assess_changes(spark.readStream.table(f"{BRONZE}.promotions_cdc"), "promotions")


@dp.temporary_view(name="promotions_changes")
def promotions_changes():
    return deduplicate_changes(spark.readStream.table("promotions_valid"), "promotions")


dp.create_streaming_table(name=f"{SILVER}.promotions",
                          comment="SCD1: khuyến mãi hiện hành; khuyến mãi bị hủy (delete ở nguồn) bị xóa")

dp.create_auto_cdc_flow(
    target=f"{SILVER}.promotions",
    source="promotions_changes",
    keys=["promo_id"],
    sequence_by=F.struct("_lsn", "_seqval"),       # thứ tự log tuyệt đối, kể cả delete
    apply_as_deletes=F.expr("_op = 1"),
    except_column_list=["_op", "_lsn", "_seqval"],
    stored_as_scd_type=1,
)
```

Ở đây **phải** dùng LSN: một dòng delete mang `updated_at` của lần sửa cuối, trùng với dòng update trước nó. Chỉ `(_lsn, _seqval)` mới xếp được delete đứng sau.

### 2.4 Kiểm tra SCD

`24852` dưới đây là ví dụ. Khi nghiệm thu, chọn product_id thật từ kế hoạch seed đã đổi giá
ở cả wave 1 và wave 3; không mặc định Banana nằm trong tập ngẫu nhiên của cả hai wave.

```sql
-- 3 phiên bản của Banana, chỉ 1 phiên bản đang mở
SELECT product_id, unit_price, __START_AT, __END_AT
FROM instacart.silver.product_prices_history WHERE product_id = 24852 ORDER BY __START_AT;

-- Không sản phẩm nào có > 1 phiên bản đang mở
SELECT product_id, count(*) FROM instacart.silver.product_prices_history
WHERE __END_AT IS NULL GROUP BY 1 HAVING count(*) > 1;              -- 0 dòng

-- Khuyến mãi bị xóa ở nguồn không còn ở silver
SELECT count(*) FROM instacart.silver.promotions p
JOIN (SELECT promo_id FROM instacart.bronze.promotions_cdc WHERE _op = 1) d USING (promo_id);   -- 0
```

### 2.5 Triển khai AD-DE-11 và điều kiện đầu vào

Code thực tế ở [`silver_retail.py`](../../src/pipelines/batch/silver_retail.py), helper Spark thuần
ở [`common/retail.py`](../../src/common/retail.py). Hướng dẫn nghiệm thu, giới hạn test local và
các ca delete/late/replay ở [03b · Nghiệm thu silver retail](03b_nghiem_thu_retail.md).

Helper tái sử dụng PRICE_RULES/PROMO_RULES từ common.dq, bổ sung kiểm khóa/operation/sequence/rescue;
delete không cần business payload để tránh chặn tombstone hợp lệ. LSN/seqval chuẩn hóa thành binary(10),
before-image op=3 chỉ giữ ở bronze. Pipeline đặt session UTC, updated_at chuẩn hóa TIMESTAMP_LTZ.
DQ xảy ra trước dedup; quarantine giá và khuyến mãi giữ nguyên raw payload và mọi tên luật lỗi.

Chỉ gộp bản sao cùng payload: không chọn tùy ý một giá khi hai giá khác nhau trùng business time.
AUTO CDC vẫn yêu cầu một trạng thái duy nhất cho mỗi key/sequence; seed và nguồn phải bảo đảm điều này.
Không đặt watermark theo updated_at vì có snapshot lịch sử 2015 và sự kiện đến muộn; state dedup được
giữ trong checkpoint, cần theo dõi dung lượng khi mở rộng ngoài quy mô đồ án.

## 3. Đơn hàng và thời gian giả lập

### 3.1 `order_ts_sim` tất định

Instacart không có ngày tháng ([data_dictionary §4](../data/data_dictionary.md)). Cách tính:
- Đơn cuối của mỗi khách (đơn train hoặc test) rơi vào ngày `2026-03-01 + (user_id × 7919 mod 30)`.
- Các đơn trước được tính lùi bằng tổng `days_since_prior_order` của các đơn sau nó.

Công thức chỉ dùng phép số nguyên. Simulator Python tính lại **ra đúng cùng giá trị** ([04 §2](04_streaming.md)); test `tests/test_sim_time.py` so khớp hai bản.
AD-DE-02 đặt mốc `SIM_END_ANCHOR`, hàm `order_ts_sim` và truy vấn SQL ở
`src/common/sim_time.py`. Hàm Python trả `datetime` có múi giờ UTC; SQL dùng
`make_timestamp_ltz(..., 'UTC')` để cùng một thời điểm dù session đổi múi giờ hoặc kiểu timestamp mặc định.

```python
# src/pipelines/batch/silver_instacart.py  (đoạn đầu)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.sim_time import order_time_sim_sql
from common.instacart import ORDER_INPUT_RULES, assess_instacart, canonical_headers

CAT    = spark.conf.get("catalog")
BRONZE = f"{CAT}.{spark.conf.get('bronze')}"
SILVER = f"{CAT}.{spark.conf.get('silver')}"


@dp.temporary_view(name="order_headers_assessed")
@dp.expect_all(ORDER_INPUT_RULES)
def order_headers_assessed():
    return assess_instacart(spark.read.table(f"{BRONZE}.orders"), "orders")


@dp.temporary_view(name="order_headers_canonical")
@dp.expect_all_or_fail(ORDER_INPUT_RULES)
def order_headers_canonical():
    return canonical_headers(spark.read.table("order_headers_assessed"))


@dp.materialized_view(name=f"{SILVER}.order_time_sim",
                      comment="order_ts_sim tất định cho mọi đơn (prior/train/test)")
@dp.expect_or_fail("has_ts", "order_ts_sim IS NOT NULL")
def order_time_sim():
    return spark.sql(order_time_sim_sql("order_headers_canonical", canonical=True))
```

Header vào clock đã qua DQ và duy nhất theo `order_id`. Ở chế độ canonical, truy vấn giữ
payload header và bỏ xếp hạng lại theo đơn. `orders_batch` đọc trực tiếp từ clock đã tính.

AD-DE-12 đánh giá DQ trên mọi bản header, rồi chọn một bản đạt DQ/ingest mới nhất cho mỗi order_id.
Nếu không có bản hợp lệ cho một đơn thì clock bị chặn, không bỏ đơn đó rồi tính lịch sử thiếu.
Clock luôn dùng **đủ prior/train/test**; chỉ lọc split khi dựng orders_batch. Raw payload lỗi vẫn
được giữ ở quarantine_orders. Công thức SQL dùng lại nguyên helper của AD-DE-02.

Pipeline batch viết toàn bộ bằng Python. Chỗ nào SQL dễ đọc hơn thì dùng `spark.sql(f"""…""")` bên trong hàm, tên catalog/schema lấy từ `spark.conf`. Như vậy không phải lo cú pháp thay biến `${…}` của file SQL trong LDP.

### 3.2 `orders_batch` và `order_items_prior`

```python
# src/pipelines/batch/silver_instacart.py  (tiếp)
from common.dq import ORDER_RULES
from common.instacart import ITEM_COLUMNS, ITEM_INPUT_RULES, batch_orders


@dp.materialized_view(name=f"{SILVER}.orders_batch",
                      comment="Header đơn prior/test từ batch; train đến từ stream",
                      cluster_by=["user_id"])
@dp.expect_or_fail("has_ts", "order_ts_sim IS NOT NULL")
def orders_batch():
    return batch_orders(spark.read.table(f"{SILVER}.order_time_sim"))


@dp.temporary_view(name="order_items_prior_valid")
@dp.expect_all_or_drop(ITEM_INPUT_RULES)
def order_items_prior_valid():
    return assess_instacart(spark.readStream.table(f"{BRONZE}.order_products_prior"), "items")


@dp.temporary_view(name="order_items_prior_src")
def order_items_prior_src():
    return spark.readStream.table("order_items_prior_valid").select(*ITEM_COLUMNS)


dp.create_streaming_table(name=f"{SILVER}.order_items_prior", cluster_by=["order_id"])
dp.create_auto_cdc_flow(
    target=f"{SILVER}.order_items_prior",
    source="order_items_prior_src",
    keys=["order_id", "product_id"],           # mỗi sản phẩm một lần trong một đơn
    sequence_by=F.struct("_ingest_ts", "_file_mtime", "_source_file"),
    except_column_list=["_rescued_data", "_ingest_ts", "_source_file", "_file_mtime"],
    stored_as_scd_type=1,
)
```

**Vì sao `order_items_prior` dùng `AUTO CDC` thay vì append:**
- Nếu file prior phải nạp lại (đặt ở đường dẫn mới, xem [02 §5](02_bronze_ldp.md)), bronze sẽ có bản sao.
- Upsert theo khóa thì silver vẫn đúng 32.434.489 dòng.

**Cái giá:** lần nạp đầu là một MERGE 32 triệu dòng, khoảng 10–20 phút trên 4 vCPU. Đo lại và ghi vào `ops.benchmark_results`.

Con số 10–20 phút là ước lượng trong kế hoạch, chưa phải kết quả đo của AD-DE-12.
Code thực tế ở [`silver_instacart.py`](../../src/pipelines/batch/silver_instacart.py), helper DQ/header ở
[`common/instacart.py`](../../src/common/instacart.py). Xem [03c · Nghiệm thu silver đơn hàng](03c_nghiem_thu_orders.md)
để chạy đối soát, thử replay và ghi thời gian MERGE đầu tiên; chưa có số đo workspace trong phiên triển khai.

## 4. Kiểm chất lượng: expectations và quarantine

### 4.1 Ba mức phản ứng

| Decorator | Dòng vi phạm | Pipeline | Dùng khi |
|---|---|---|---|
| `@dp.expect(name, cond)` | **Giữ lại**, chỉ đếm | Chạy tiếp | Luật "nên đúng", ví dụ sản phẩm có ghép được OFF không |
| `@dp.expect_or_drop` | **Bỏ** khỏi bảng đích | Chạy tiếp | Dòng lỗi không dùng được, ví dụ giá âm |
| `@dp.expect_or_fail` | — | **Dừng** update, rollback flow đó | Lỗi nghiêm trọng, chạy tiếp sẽ hỏng dữ liệu, ví dụ `dim_product` bị nhân dòng |

Các bản `expect_all*` nhận một dict nhiều luật cùng lúc.

### 4.2 Không xóa im lặng: bảng quarantine

`expect_or_drop` chỉ **đếm** số dòng bị bỏ, không giữ lại nội dung. Để xem lại được dòng nào lỗi và vì sao, nhóm dùng **cùng bộ luật** cho một bảng quarantine với điều kiện **đảo ngược**:

```python
from common.dq import dq_failed


def quarantine(name: str, source: str, rules: dict):
    @dp.table(name=f"{SILVER}.quarantine_{name}",
              comment=f"Dòng vi phạm luật của {source}; giữ để điều tra, không xóa")
    def _q():
        return (spark.readStream.table(source)
                .withColumn("_dq_failed", dq_failed(rules))
                .where(F.size("_dq_failed") > 0)
                .withColumn("_quarantined_at", F.current_timestamp()))


# Instacart dùng hai quarantine trong silver_instacart.py với cùng INPUT_RULES của nhánh xử lý.
# Retail dùng quarantine chuyên biệt trong silver_retail.py, cùng rules với valid view.
```

Các bộ `ORDER_RULES`, `ITEM_RULES`, `PRICE_RULES`, `PROMO_RULES`, `EVENT_RULES` được định nghĩa
duy nhất trong `src/common/dq.py`. Pipeline import cùng dict cho expectations và quarantine,
giữ nguyên tên luật để đối soát số liệu; không sao chép định nghĩa vào từng notebook.

Quy ước NULL: cột bắt buộc rỗng là **không đạt**, dùng `coalesce(điều_kiện, false)`;
`days_since_prior_order IS NULL` vẫn hợp lệ cho đơn đầu và `_rescued_data IS NULL` là đạt.
Mỗi luật đã được test cả đạt/lỗi, biên và NULL. Dict giữ đúng phạm vi luật ở các phần trên;
kiểm khóa/đối soát liên bảng vẫn được làm ở task tương ứng.

`dq_failed(rules)` trả biểu thức Spark `array<string>` tên mọi luật không đạt theo thứ tự khai báo.
Dòng hợp lệ nhận `[]`; dict rỗng cũng có đúng kiểu `array<string>`. Hàm coi cả NULL từ luật
tùy chỉnh là lỗi để không mất dòng quarantine; không che lỗi SQL hoặc cột nguồn bị thiếu.
Hàm không phụ thuộc `dp`, không chạy action/ghi dữ liệu, dùng được trong batch và stream.
Nguồn phải có đủ cột luật tham chiếu (kể cả `_rescued_data` khi dùng ORDER/ITEM_RULES).

Số liệu đạt/bị loại theo từng luật được lấy từ **event log** của pipeline và ghi vào `ops.dq_results` ([06 §3](06_van_hanh_chay_lai.md)). Power BI trang vận hành đọc bảng này.

## 5. Làm giàu từ Open Food Facts: làm phẳng JSON và fuzzy match

Triển khai ở [`silver_off.py`](../../src/pipelines/batch/silver_off.py), notebook source `.py`
có `%pip install rapidfuzz==3.14.6` ở ô đầu. Cấu hình glob của `instacart_batch` đã nạp thư mục này;
không cần tạo pipeline hay cluster mới. Logic trả DataFrame nằm ở
[`common/off.py`](../../src/common/off.py), dùng lại `norm` từ `common/text.py` của AD-DE-02.
Không gọi API, cài package hoặc ghi bảng bên trong hàm định nghĩa dataset.

### 5.1 Làm phẳng và chọn phiên bản

- Đọc batch từ `bronze.off_products` (mỗi dòng JSONL là một hit của AD-DE-10).
  `_product_id` trở thành `product_id` INT, `code` thành `off_code` STRING để giữ số 0 đầu.
- `brands` nhận chuỗi phân cách dấu phẩy hoặc mảng; đầu ra `brand` là chuỗi.
  Tags nhận mảng hoặc chuỗi phân cách dấu phẩy/JSON array; trường thiếu thành mảng rỗng.
- Nutriments nhận MAP theo schema hint bronze hoặc STRUCT có trường thưa; trích
  `energy-kcal_100g`, `sugars_100g`, `fat_100g` thành ba cột DOUBLE. Trường thiếu hay
  số không parse được nhận NULL qua `try_cast`, kể cả ANSI mode. `nova_group` là INT.
  Không suy diễn giá trị dinh dưỡng hoặc chuẩn hóa đơn vị ngoài hợp đồng trên 100g của nguồn.
- `OFF_RULES` kiểm khóa dương, mã vạch 1–32 chữ số, ingest timestamp hợp lệ và rescue NULL.
  `silver.quarantine_off_products` giữ mọi raw payload cùng metadata, toàn bộ tên luật lỗi
  và `_quarantined_at`, **trước** khử trùng. Đây là MV theo snapshot bronze, không append lặp mỗi update.
- `silver.off_products_clean` chỉ lấy bản đạt các luật trên, mới nhất theo `_ingest_ts`
  cho mỗi `(product_id, off_code)`. Phá hòa bằng SHA-256 raw; chạy lại cùng snapshot không đổi lựa chọn.
  Nếu bản mới lỗi khóa/ingest/rescue, nó vào quarantine và bản cũ hợp lệ vẫn được dùng.
  Hash chỉ giúp chọn tất định, không xác nhận đâu là nội dung nghiệp vụ đúng khi cùng timestamp.
- `has_name` là expectation warn: tên rỗng/NULL vẫn giữ, nhưng scorer trả 0.
  Trường dinh dưỡng tùy chọn có thể NULL; dữ liệu gốc vẫn còn ở bronze.

### 5.2 Chọn ứng viên và chấm điểm

1. Danh mục gốc là **toàn bộ** `bronze.products`, khử trùng `product_id` theo ingest mới nhất,
   rồi tên tăng dần khi hòa; nguồn Instacart là release bất biến. Key không hợp lệ làm flow
   `product_off_match` fail qua expectation, không âm thầm loại sản phẩm.
2. Chỉ xét hit của **đúng** `product_id` do crawler gắn, có `countries_tags` chứa chính xác
   `en:united-states`. Không cross join cả danh mục Instacart và OFF.
3. LEFT JOIN để giữ cả sản phẩm chưa crawl, không có kết quả hoặc chỉ có kết quả ngoài Mỹ.
   Khác ví dụ INNER JOIN cũ, bảng cầu nối luôn có một dòng cho mỗi khóa sản phẩm nguồn.
4. Pandas UDF dùng `rapidfuzz.token_set_ratio(norm(ic_name), norm(off_name))`.
   Cộng 5 nếu thương hiệu đầu tiên xuất hiện nguyên cụm từ trong tên Instacart đã chuẩn hóa;
   chặn trên 100. Tên sau chuẩn hóa rỗng nhận 0, không cộng bonus. Không làm tròn điểm trước phân nhãn.
5. Chọn điểm cao nhất rồi `off_code` tăng dần khi hòa. Từ 85: `matched`; từ 70 đến dưới 85:
   `low_confidence`; dưới 70: `no_match`. Khi không có ứng viên, mã NULL và điểm 0.
   Nếu có ứng viên nhưng điểm thấp thì vẫn giữ mã/điểm để kiểm tra, gắn `no_match`.
6. Đầu ra chỉ có `product_id, off_code, off_match_score, off_match_status`.
   Expectations fail kiểm key, miền điểm và nhãn tương ứng. Chỉ `matched` được làm giàu BI
   ở AD-DE-14; `low_confidence`/`no_match` không được dùng để gắn thương hiệu hay dinh dưỡng.

`token_set_ratio` có thể cho 100 khi tên này chứa toàn bộ token của tên kia; đây là heuristic,
không phải xác suất hai sản phẩm giống nhau. Cần kiểm tay một mẫu sau khi có dữ liệu thật.
Xem [RapidFuzz](https://rapidfuzz.github.io/RapidFuzz/Usage/fuzz.html#token-set-ratio) và
[pandas UDF trên Spark](https://spark.apache.org/docs/3.5.7/api/python/user_guide/sql/arrow_pandas.html#pandas-udfs-a-k-a-vectorized-udfs).

### 5.3 Kiểm và nghiệm thu

`uv run pytest tests/test_off.py tests/test_norm.py -q` kiểm flatten, ANSI, thiếu trường,
quarantine, replay, phá hòa, quốc gia, đủ sản phẩm và thực thi pandas UDF qua Arrow worker.
PyArrow trong dev dependencies phục vụ Spark local; notebook dùng Arrow/pandas của runtime LDP.
Test đăng ký `dp` giả lập không thay cho update pipeline trên workspace.

Cách chạy dev, lấy đủ ba nhãn, mẫu số và ghi tỉ lệ thật:
[03d_nghiem_thu_off.md](03d_nghiem_thu_off.md). AD-DE-10 (#46) đã nghiệm thu Bronze OFF trên dev.
Kết quả đo AD-DE-13 ở [data dictionary §5.6](../data/data_dictionary.md#56-cách-join-ghép-theo-tên-qua-bảng-cầu-nối).
`dim_product` thuộc AD-DE-14.

## 6. `dim_product`: join nhiều nguồn mà không nhân dòng

MV khai báo trong [`silver_off.py`](../../src/pipelines/batch/silver_off.py), truy vấn batch nằm ở
[`common/product.py`](../../src/common/product.py). Năm nguồn cơ sở gồm products, aisles, departments, product_prices_history và promotions.
Khi bật OFF, đọc thêm product_off_match, off_products_clean và product_images.
Bảng không tự ghi dữ liệu, không gọi action, không cần job/cluster riêng.

### 6.1 Grain và giá/khuyến mãi

- Danh mục gốc là mọi `product_id` hợp lệ từ `bronze.products`, **LEFT JOIN** các nguồn làm giàu.
  Thiếu giá, danh mục con hoặc OFF không làm mất sản phẩm.
- Products/aisles/departments là release bất biến. Với mỗi khóa, ưu tiên bản không có
  `_rescued_data`, rồi chọn bản mới nhất theo `_ingest_ts`. Nếu mọi bản đều có rescue, vẫn giữ
  bản mới nhất và ghi cảnh báo. Nhiều payload khác nhau trong nhóm được ưu tiên cũng ghi cảnh báo.
- Khóa sản phẩm NULL, không ép được sang INT hoặc không dương được đếm ở view
  `catalog_products_assessed` trước khi loại khỏi bridge và dimension.
- `dim_product` luôn được đăng ký. Khi `enable_off=false`, dim chỉ đọc năm nguồn cơ sở, đặt các thuộc
  tính OFF/ảnh thành NULL và giữ `off_match_status=no_match`, `off_match_score=0`.
  Khi có dữ liệu OFF, bật `enable_off=true` để dùng thêm bridge, OFF sạch và ảnh.
- Giá lấy **chỉ** hàng `__END_AT IS NULL`, giữ `unit_price` thành `current_price` DECIMAL và `currency`.
  Không lấy MAX giá, không dùng giá đã hết hiệu lực để bù thiếu. Nhiều hàng đang mở cho cùng sản phẩm
  là lỗi hợp đồng SCD2 và làm flow fail, kể cả hai hàng giống nhau.
- `as_of_date` từ configuration, mặc định `2026-03-30`, phải là ngày ISO `YYYY-MM-DD` hợp lệ.
  Chỉ ngày cấu hình này dùng để lọc khuyến mãi `start_date <= as_of_date <= end_date`, rồi
  `GROUP BY product_id / MAX(discount_pct)` **trước** join. Không cộng chồng mức giảm.
  `on_promotion = discount_pct IS NOT NULL`, nên khuyến mãi 0% vẫn là đang chạy.
- Giá hiện hành không phải giá tại `as_of_date` hay tại lúc đặt hàng. Doanh thu lịch sử ở
  AD-DE-22 phải join `product_prices_history` theo khoảng hiệu lực `[__START_AT, __END_AT)`.

### 6.2 OFF và ảnh

- Giữ `off_match_score`/`off_match_status` của **mọi** sản phẩm để BI đo chất lượng phủ.
  Thiếu dòng bridge nhận `no_match`/0. Chỉ khi `matched` mới giữ `off_code` để làm giàu.
- Join OFF bằng **cặp `(product_id, off_code)`**, không bỏ `product_id` rồi khử trùng toàn cục theo barcode.
  Hai sản phẩm dùng cùng barcode có thể mang payload OFF khác nhau từ hai lần tra; phải giữ đúng bản của từng sản phẩm.
- `low_confidence` và `no_match` giữ điểm/nhãn nhưng các trường OFF/ảnh là NULL.
  Điều này chặn cả ảnh trong cache và thông tin dinh dưỡng của ứng viên không đáng tin đi vào BI.
- `categories_tags` xuất thành `off_categories`; còn `off_product_name`, `brand`, `countries_tags`,
  `nutriscore_grade`, `nova_group`, ba cột dinh dưỡng, `image_url` và `image_path` theo hợp đồng nguồn.
- Ảnh được gộp theo `off_code`: ưu tiên path có giá trị, modificationTime mới nhất, ingest mới nhất,
  rồi path tăng dần khi hòa. Metadata ảnh được chọn trước join, không thể nhân dòng.

### 6.3 Kiểm chất lượng và nghiệm thu

Mỗi nguồn bên phải được rút về một dòng mỗi khóa trước join, giữ số bản nguồn để không che xung đột.
Các bản snapshot giống nhau được gộp; riêng giá đang mở phải duy nhất ngay trong SCD2 target.
Hash payload phá hòa để kết quả nhất quán. Expectation vẫn ghi nhận các payload xung đột.

| Expectation | Hành động | Điều kiện |
|---|---|---|
| `no_fanout` | Fail | `dup_cnt = 1` sau toàn bộ join, tính bằng window theo product_id |
| `unique_sources` | Fail | `_join_sources_unique` không có nguồn liên quan nào xung đột |
| `valid_product_id` | Fail | Khóa sản phẩm không NULL và dương |
| `has_price` | Warn | `current_price IS NOT NULL`; sản phẩm thiếu giá vẫn còn trong dim |
| `catalog_payload_unique` | Warn | Mỗi khóa danh mục có một payload trong nhóm bản ghi được ưu tiên |
| `catalog_unrescued` | Warn | Dòng products/aisles/departments được chọn không phải bản rescue |
| `valid_catalog_product_id` | Warn | Khóa gốc ép được sang INT và lớn hơn 0; đếm trước khi chọn phiên bản |

`dup_cnt` và `_join_sources_unique` ở lại bảng để expectations kiểm được, không dùng làm measure BI.
Nguồn lỗi vẫn giữ trong bronze/silver; dim không xóa hay sửa ngược nguồn. Flow fail không có nghĩa các
flow khác cùng update rollback; bảng dim có thể còn phiên bản thành công cũ, phải kiểm update ID.

Release đầy đủ phải có **49.688 dòng / 49.688 khóa**, không phải mọi fixture đều có số này.
Đã nghiệm thu đủ số dòng và số khóa trên dev với OFF tắt và bật. Xem tử/mẫu số `has_price`,
pipeline update và cách replay trong [runbook dim_product](03e_nghiem_thu_dim_product.md).

## 7. Gộp batch ∪ stream và dựng `baskets`

Mỗi bảng chỉ có một pipeline chủ. Task [`src/tasks/create_views.sql`](../../src/tasks/create_views.sql)
tạo **view Unity Catalog**, còn `baskets` là MV của pipeline batch; pipeline batch chỉ đọc các bảng
silver stream như nguồn bên ngoài, không khai báo lại target stream.

### 7.1 Hai view có tham số schema

File SQL nhận hai tham số STRING bắt buộc `catalog` và `silver_schema` (từng tên riêng, không bọc backtick).
Câu đầu `USE IDENTIFIER(...)` chọn namespace đã quote cả catalog/schema; hai câu tiếp theo
`CREATE OR REPLACE VIEW orders` và `order_items` dùng namespace này. Chạy **cả file cùng một phiên**,
dừng nếu USE lỗi. Không chọn riêng hai câu CREATE dựa vào schema hiện tại của SQL editor.
Cách này tránh đặt parameter marker trong thân CREATE VIEW, vốn không được Spark 3.5 chấp nhận.

| View | Nhánh batch | Nhánh stream |
|---|---|---|
| `orders` | Header prior/test từ orders_batch, giữ source và is_test | stream_orders: eval_set=train, event_time đổi tên order_ts_sim, source=stream, is_test=false |
| `order_items` | order_items_prior, source=batch | stream_order_items, source=stream |

Cả hai dùng **UNION ALL**, chọn cột tường minh; không đọc `bronze.order_products_train` để nạp lại train
bằng batch, không che trùng khóa bằng UNION/DISTINCT. Nguồn phải upsert/duy nhất và hai nhóm mã đơn
phải rời nhau. View phản ánh thay đổi bảng nguồn, không cần refresh hay chạy lại DDL khi có stream mới.
`orders` giữ header test và cờ is_test; test không tạo giỏ DS.

### 7.2 Materialized view baskets

MV ở cuối [`silver_instacart.py`](../../src/pipelines/batch/silver_instacart.py), helper
[`common/baskets.py`](../../src/common/baskets.py) đọc batch bốn bảng silver trực tiếp:

- orders_batch lọc eval_set=prior + order_items_prior → split `prior` để học luật.
- stream_orders + stream_order_items → split `holdout` để đánh giá.
- Không đọc hai view UC để tránh vòng phụ thuộc bootstrap. Target stream phải tồn tại đúng schema
  trước khi bật `enable_baskets=true`. Cờ mặc định là `false` để batch vẫn chạy khi stream chưa sẵn sàng.
- Giữ mọi header đủ điều kiện bằng LEFT JOIN món. Header chưa có món có `items=[]`, n_items=0 và
  làm check `non_empty_prior_basket` fail ở prior. Ở holdout, header chờ món chỉ cảnh báo trong
  `basket_candidates` và được đưa vào basket ở update sau khi items đã commit.
- `items = array_sort(collect_set(product_id))`, kiểu `ARRAY<INT>`; `n_items = size(items)`.
  Món trùng do replay không làm tăng n_items; danh sách là tập sản phẩm, không phải số lượng mua.
  Bản sao header cùng order_id/user_id được gộp, header cùng mã nhưng khác user bị fail.
- Giữ cờ kiểm tra kỹ thuật để fail khi có sản phẩm NULL/không dương, khóa header sai,
  order_id xuất hiện ở nhiều giỏ hoặc mã đơn stream giao với **bất kỳ** header batch nào (kể cả test).
  Nguồn vẫn còn nguyên trong silver để điều tra, không sửa/xóa nguồn từ MV.

Expectations fail: `non_empty_prior_basket`, `valid_basket_keys`, `valid_basket_items`,
`disjoint_sources`, `unique_basket_order`. Các cột kỹ thuật `_items_valid`, `_source_disjoint`,
`dup_cnt` ở lại bảng cho expectations; DS chỉ lấy năm cột nghiệp vụ.
Món không có header không thể thành basket; đối soát orphan/count ở AD-DE-17 vẫn bắt buộc.

Sau khi stream phát đủ, mục tiêu **3.214.874 prior / 131.209 holdout**, cần đối chiếu số thật.
Giỏ rỗng trong lúc stream mới cập nhật một target chưa phải mất dữ liệu: chờ cả hai target hoàn tất
rồi chạy lại batch. Một update thất bại có thể để lại MV phiên bản thành công cũ; kiểm update ID.

Hướng dẫn bootstrap, chạy SQL hai lần trên warehouse, đối soát và replay:
[03f_nghiem_thu_baskets.md](03f_nghiem_thu_baskets.md). Còn chờ AD-ST-05 (#68) để bật giỏ và nghiệm thu với stream thật.
Tham khảo [IDENTIFIER](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-names-identifier-clause)
và [USE](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-use-schema).

## 8. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách chạy lại | Vì sao không trùng, không mất |
|---|---|---|
| Update lỗi ở một flow `AUTO CDC` | Chạy lại update | Checkpoint chưa commit thì micro-batch được áp dụng lại. Upsert theo khóa + sequence nên kết quả như nhau |
| `expect_or_fail` dừng pipeline | Xem event log → sửa dữ liệu nguồn hoặc code → chạy lại | Flow lỗi rollback, các bảng khác giữ trạng thái cũ |
| Sửa logic một MV (ví dụ ngưỡng fuzzy) | Deploy → `refresh_selection` gồm MV đó và các bảng phía sau | MV luôn tính lại từ nguồn |
| Sửa logic SCD2 (ví dụ thêm cột theo dõi) | `full_refresh_selection = [product_prices_history]` | Bảng được dựng lại từ **toàn bộ** `bronze.product_prices_cdc`. Bronze không bị reset vì có `reset.allowed = false` |
| Silver sai do dữ liệu nguồn sai | Sửa nguồn (hoặc thêm luật quarantine) → `full_refresh_selection` các bảng silver liên quan. **Không** `RESTORE` / `UPDATE` tay bảng do pipeline quản lý ([06 §5](06_van_hanh_chay_lai.md)) | Silver dựng lại từ bronze còn đủ; time travel dùng để so trước/sau |

## 9. Checklist G3: silver đạt đối soát → DS bắt đầu

Chạy sau khi stream đã phát đủ ([04 §6](04_streaming.md)):

```sql
SELECT count(*) FROM instacart.silver.orders;                                   -- 3.421.083
SELECT source, count(DISTINCT order_id) FROM instacart.silver.orders GROUP BY 1; -- batch 3.289.874 | stream 131.209
SELECT count(*) FROM instacart.silver.order_items WHERE source = 'stream';      -- 1.384.617
SELECT count(*), count(DISTINCT order_id, product_id) FROM instacart.silver.order_items_prior;  -- 32.434.489 = nhau
SELECT count(*) FROM instacart.silver.dim_product;                              -- 49.688
SELECT split, count(*) FROM instacart.silver.baskets GROUP BY 1;                -- prior 3.214.874 | holdout 131.209
SELECT off_match_status, count(*) FROM instacart.silver.product_off_match GROUP BY 1;   -- ghi tỉ lệ khớp
```

Số đơn batch được tính bằng 3.421.083 − 131.209. Số giỏ `prior` được tính bằng 3.421.083 − 131.209 − 75.000. Cả hai phải kiểm lại bằng số thật của bản Zenodo.

- [ ] Các con số khớp, và `ops.reconciliation` ghi `passed = true` cho mọi bảng.
- [ ] `silver.quarantine_*` có số dòng giải thích được, đã ghi vào `ops.dq_results`.
- [ ] Chạy job `ds-prepare` để chụp `silver.baskets` thành `ds_data.baskets` (`data_version = 'v1'`) và dựng dữ liệu thực nghiệm cho DS ([08](08_du_lieu_cho_ds.md)). Không dùng `VERSION AS OF` trên `baskets`: đây là materialized view do pipeline quản lý, bản cố định phải là một bảng riêng.
