# 04 · Luồng streaming: simulator → Event Hubs → bronze/silver/gold

**Đầu vào:**
- Stack `20-stream` đã bật (`stream-toggle up`).
- Secret `eh-send-connection-string` và `eh-listen-connection-string` có trong Key Vault, đọc được qua secret scope `kv` ([devops/03 §4](../devops/03_terraform_azure.md), [devops/05 bước 7](../devops/05_databricks_platform.md)).

**Đầu ra:**
- Pipeline `instacart_stream` gồm:
  - `bronze.order_events`.
  - `silver.stream_orders`, `silver.stream_order_items`.
  - `gold.stream_pair_counts_10m`, `gold.stream_kpi_1m`.
  - `ops.streaming_latency`.

**Xong khi:**
- 131.209 đơn và 1.384.617 dòng hàng đã vào silver.
- Phát lại không làm tăng số đơn.
- Có số đo độ trễ cùng region và khác region.

---

## 1. Batch hay streaming: chọn thế nào

| | Batch (`instacart_batch`) | Streaming (`instacart_stream`) |
|---|---|---|
| Dữ liệu | Lịch sử (prior), danh mục, giá | Đơn mới nhất (train) dưới dạng sự kiện |
| Nguồn | File trên landing | Event Hubs (Kafka) |
| Tần suất | Hằng ngày 02:00 | Mỗi 15 phút (triggered) hoặc liên tục khi demo |
| Độ trễ | Giờ | Giây đến phút |
| Kiểu bảng | Streaming table + materialized view | Streaming table |
| Thời gian dùng để tính | Thời gian nghiệp vụ (`order_ts_sim`) | **Hai** loại thời gian, xem mục 4.3 |

Cả hai đều chạy bằng **Structured Streaming** có checkpoint. Khác nhau ở chỗ batch dùng chế độ *triggered*: mỗi update xử lý hết dữ liệu mới rồi dừng, tương đương `trigger(availableNow=True)`.

**Ràng buộc quota:** mỗi pipeline dùng một cluster 4 vCPU, nên **không chạy hai pipeline cùng lúc**. Lịch stream bỏ qua khung 02:00–03:00.

## 2. Simulator

### 2.1 Sự kiện

Theo [data_dictionary §4](../data/data_dictionary.md), bổ sung thêm 2 trường để đo độ trễ:

```json
{"event_id": "order-1187899", "event_type": "order_placed", "schema_version": 1,
 "order_id": 1187899, "user_id": 1, "order_number": 11, "order_dow": 4,
 "order_hour_of_day": 8, "days_since_prior_order": 14,
 "event_time": "2026-03-30T08:00:00Z", "replay_run_id": "run-03",
 "produced_at": "2026-11-20T09:15:02.123Z", "producer_location": "laptop-hcm",
 "items": [{"product_id": 196, "add_to_cart_order": 1, "reordered": 1}]}
```

- `event_time` là thời gian nghiệp vụ giả lập. Công thức trùng với `silver.order_time_sim`: đơn train là đơn cuối, nên `days_back = 0`.
- `produced_at` là đồng hồ thật lúc gửi, dùng để đo độ trễ.
- `producer_location` ghi simulator chạy ở đâu (laptop, Codespace, VM region A, region B).

### 2.2 Code

Simulator có hai chế độ. `--dry-run FILE` ghi JSON Lines để kiểm tại máy. `--send` gửi
cùng cấu trúc JSON tới `orders-stream` qua Kafka. Xem [README simulator](../../src/streaming/simulator/README.md)
để chạy từ laptop hoặc Codespace và đối soát 1.000 sự kiện trên Azure Portal.

- `key = order_id`, nên mọi sự kiện của một đơn vào cùng partition và giữ thứ tự trong partition.
- `order_ts_sim` import từ `src/common/sim_time.py`; đơn train là đơn cuối nên dùng
  `days_back = 0` mặc định. Đặt `PYTHONPATH=src` khi chạy script trực tiếp từ gốc repo.
  Không viết lại công thức trong simulator; test local đã so khớp với SQL ở silver.
- Simulator đổi `days_since_prior_order` rỗng hoặc NaN thành `null` để JSON hợp lệ.
- Producer đọc chuỗi kết nối SAS từ `EH_SEND_CONN` và bật `enable.idempotence`. Event Hubs
  Standard dùng `--compression none`. [Hỗ trợ nén Kafka của Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview#compression).

## 3. Pipeline `instacart_stream`

### 3.1 Khai báo DAB

[Bundle stream](../../bundles/resources/pipeline_stream.yml) dùng classic single-node
và ghi event log vào `<catalog>.<schema_prefix>ops.event_log_stream`. Source được nạp
từ `src/pipelines/stream/`. Các biến `eh_generation`, `eh_region` và `dbx_region`
truyền mã thế hệ của hub và nhãn region cho các tầng streaming. Xem cách tạo schema,
deploy và chạy smoke trong [README bundle](../../bundles/README.md#nghiệm-thu-pipeline-stream-ad-st-03).

### 3.2 Bronze: đọc Kafka, giữ nguyên bản tin

```python
# src/pipelines/stream/bronze_events.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT    = spark.conf.get("catalog")
BRONZE = f"{CAT}.{spark.conf.get('bronze')}"
TOPIC  = spark.conf.get("eh_topic")
GEN    = spark.conf.get("eh_generation")

conn = dbutils.secrets.get("kv", "eh-listen-connection-string")
ns   = conn.split("Endpoint=sb://")[1].split("/")[0]
KAFKA = {
    "kafka.bootstrap.servers": f"{ns}:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config":
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn}";',
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "20000",          # giới hạn micro-batch cho cluster 4 vCPU
}

dp.create_streaming_table(
    name=f"{BRONZE}.order_events",
    comment="Bronze: bản tin Kafka nguyên vẹn (JSON dạng chuỗi) + metadata Event Hubs",
    table_properties={"pipelines.reset.allowed": "false", "quality": "bronze"})


@dp.append_flow(target=f"{BRONZE}.order_events", name=f"kafka_{GEN}")
def kafka_to_bronze():
    return (spark.readStream.format("kafka").options(**KAFKA).load()
            .select(F.col("key").cast("string").alias("event_key"),
                    F.col("value").cast("string").alias("value_json"),
                    F.col("partition").alias("_kafka_partition"),
                    F.col("offset").alias("_kafka_offset"),
                    F.col("timestamp").alias("_enqueued_ts"),
                    F.lit(GEN).alias("_eh_generation"),
                    F.current_timestamp().alias("_ingest_ts")))
```

**Vì sao bronze giữ JSON dạng chuỗi:** nếu simulator đổi schema (`schema_version = 2`), bronze vẫn nhận đủ. Chỉ bước parse ở silver phải xử lý thêm phiên bản mới.

**Vì sao dùng `append_flow` có tên chứa `eh_generation`:**
- Event Hubs bị **xóa mỗi đêm** và tạo lại khi cần (`stream-toggle`). Hub mới đánh offset lại từ 0.
- Checkpoint cũ thì nhớ offset của hub cũ, ví dụ 65.000. Nếu đọc tiếp bằng checkpoint đó, Kafka source sẽ báo mất dữ liệu, hoặc bỏ qua sự kiện của hub mới.
- Đổi tên flow (`kafka_g1` → `kafka_g2`) thì LDP tạo **checkpoint mới** và đọc hub mới từ `earliest`, trong khi bảng bronze vẫn giữ toàn bộ dữ liệu cũ. Không cần full refresh, và bronze cũng không cho full refresh.
- Workflow `stream-toggle up` truyền `--var eh_generation=g<yyyymmdd>` khi deploy.

> **Spike:** xác nhận đổi tên flow (bỏ flow cũ khỏi code) không làm lỗi update.
> Nếu có lỗi: trong suốt giai đoạn demo giữ **một** namespace cố định, không xóa mỗi đêm, và chỉ xóa sau khi xong. Chi phí Standard 1 TU khoảng 0,7 USD/ngày, ghi vào budget.

## 4. Silver và gold của luồng stream

### 4.1 Parse, kiểm chất lượng, loại trùng

```python
# src/pipelines/stream/silver_events.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T

from common.dq import EVENT_RULES, dq_failed

CAT    = spark.conf.get("catalog")
BRONZE = f"{CAT}.{spark.conf.get('bronze')}"
SILVER = f"{CAT}.{spark.conf.get('silver')}"

ITEM = T.StructType([T.StructField("product_id", T.IntegerType()),
                     T.StructField("add_to_cart_order", T.IntegerType()),
                     T.StructField("reordered", T.IntegerType())])
EVENT_V1 = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("event_type", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("order_id", T.IntegerType()), T.StructField("user_id", T.IntegerType()),
    T.StructField("order_number", T.IntegerType()), T.StructField("order_dow", T.IntegerType()),
    T.StructField("order_hour_of_day", T.IntegerType()),
    T.StructField("days_since_prior_order", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()), T.StructField("replay_run_id", T.StringType()),
    T.StructField("produced_at", T.TimestampType()), T.StructField("producer_location", T.StringType()),
    T.StructField("items", T.ArrayType(ITEM)),
])

@dp.temporary_view(comment="Sự kiện đã parse; nguồn chung cho silver, gold stream và latency")
def events_parsed():
    return (spark.readStream.table(f"{BRONZE}.order_events")
            .withColumn("e", F.from_json("value_json", EVENT_V1))
            .select("e.*", "value_json", "_enqueued_ts", "_ingest_ts", "_kafka_partition", "_kafka_offset")
            .withColumn("_silver_ts", F.current_timestamp()))


@dp.temporary_view
@dp.expect_all_or_drop(EVENT_RULES)
def events_valid():
    return spark.readStream.table("events_parsed").drop("value_json")


@dp.table(name=f"{SILVER}.quarantine_order_events", comment="Sự kiện lỗi và payload gốc để điều tra")
def quarantine_order_events():
    return (spark.readStream.table("events_parsed")
            .withColumn("_dq_failed", dq_failed(EVENT_RULES))
            .where(F.size("_dq_failed") > 0)
            .withColumn("_quarantined_at", F.current_timestamp()))


# --- header đơn: SCD1 theo order_id → phát lại không nhân dòng
dp.create_streaming_table(name=f"{SILVER}.stream_orders",
                          comment="Đơn train đến qua stream; một dòng mỗi order_id")
dp.create_auto_cdc_flow(
    target=f"{SILVER}.stream_orders",
    source="events_valid",
    keys=["order_id"],
    sequence_by=F.struct("_enqueued_ts", "_kafka_partition", "_kafka_offset"),
    except_column_list=["items", "event_type", "schema_version", "_kafka_partition", "_kafka_offset"],
    stored_as_scd_type=1,
)


@dp.temporary_view
def event_items():
    return (spark.readStream.table("events_valid")
            .select("order_id", "_enqueued_ts", "_kafka_partition", "_kafka_offset",
                    F.explode("items").alias("it"))
            .select("order_id", "it.*", "_enqueued_ts", "_kafka_partition", "_kafka_offset"))


dp.create_streaming_table(name=f"{SILVER}.stream_order_items")
dp.create_auto_cdc_flow(
    target=f"{SILVER}.stream_order_items",
    source="event_items",
    keys=["order_id", "product_id"],
    sequence_by=F.struct("_enqueued_ts", "_kafka_partition", "_kafka_offset"),
    except_column_list=["_kafka_partition", "_kafka_offset"],
    stored_as_scd_type=1,
)
```

Bản ghi không parse được hoặc sai luật được giữ trong `silver.quarantine_order_events`:
payload `value_json` cùng tên các luật lỗi nằm ngay trong dòng quarantine.
`EVENT_RULES` import từ `common.dq`, dùng chung cho `events_valid` và quarantine;
NULL ở trường bắt buộc, sai loại/schema/id hoặc mảng món rỗng đều là lỗi.
Luật `valid_item_keys` đưa cả sự kiện vào quarantine nếu có món thiếu `product_id`
hoặc một `product_id` xuất hiện nhiều lần trong cùng đơn, vì `stream_order_items`
upsert theo khóa `(order_id, product_id)`.
Quy ước chi tiết ở [03 §4.2](03_silver_cdc_scd_dq.md).

### 4.2 Vì sao phát lại không nhân dòng

```
run-01: order-1187899 (enqueued 09:15)  → stream_orders: 1 dòng (replay_run_id = run-01)
run-02: order-1187899 (enqueued 14:02)  → cùng khóa order_id, sequence lớn hơn → UPDATE
                                          stream_orders: vẫn 1 dòng (replay_run_id = run-02)
bronze.order_events:                       2 dòng (lịch sử đầy đủ của cả 2 lần phát)
```

| Cơ chế | Chống trùng ở đâu |
|---|---|
| Producer `enable.idempotence` | Retry mạng trong **một** phiên simulator |
| Checkpoint Kafka source | Pipeline dừng/chạy lại không đọc lại offset đã commit |
| `AUTO CDC` theo `order_id` | Mọi trường hợp còn lại: phát lại, đổi `eh_generation`, sự kiện trùng |

### 4.3 Gold gần thời gian thực: hai loại thời gian

| Thời gian | Cột | Dùng cho | Ghi chú |
|---|---|---|---|
| **Event time** (khi sự việc xảy ra) | `event_time` (tháng 3/2026 giả lập) | KPI theo ngày/giờ, join giá. Nằm ở gold batch qua `silver.orders` | 30 ngày giả lập được phát trong vài chục phút, nên cửa sổ 10 phút theo event time chỉ có vài đơn |
| **Processing / arrival time** (khi hệ thống nhận) | `_enqueued_ts` | Tile real-time: "trong 10 phút vừa qua nhận được gì" | Phát lại thì đếm lại, đúng nghĩa "đã nhận" |

Watermark trên `_enqueued_ts` cho phép sự kiện đến trễ tối đa 2 phút. Ngoài khoảng đó, cửa sổ được chốt và trạng thái được dọn khỏi bộ nhớ.

```python
# src/pipelines/stream/gold_stream.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT  = spark.conf.get("catalog")
GOLD = f"{CAT}.{spark.conf.get('gold')}"
OPS  = f"{CAT}.{spark.conf.get('ops')}"


def deduped_events():
    # Loại trùng trong phạm vi watermark (retry mạng, đọc lặp), trạng thái chỉ giữ 2 phút
    return (spark.readStream.table("events_valid")
            .withWatermark("_enqueued_ts", "2 minutes")
            .dropDuplicatesWithinWatermark(["event_id", "replay_run_id"]))


@dp.table(name=f"{GOLD}.stream_pair_counts_10m",
          comment="Cặp sản phẩm mua cùng nhau theo cửa sổ 10 phút thời gian nhận")
def stream_pair_counts_10m():
    pairs = F.expr("""flatten(transform(items, (a, i) ->
                         transform(slice(items, i + 2, size(items)), b ->
                           named_struct('p1', least(a.product_id, b.product_id),
                                        'p2', greatest(a.product_id, b.product_id)))))""")
    return (deduped_events()
            .select("_enqueued_ts", F.explode(pairs).alias("pr"))
            .groupBy(F.window("_enqueued_ts", "10 minutes"), "pr.p1", "pr.p2")
            .agg(F.count("*").alias("pair_count"))
            .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                    "p1", "p2", "pair_count"))


@dp.table(name=f"{GOLD}.stream_kpi_1m", comment="Số đơn, số món mỗi phút (thời gian nhận)")
def stream_kpi_1m():
    return (deduped_events()
            .groupBy(F.window("_enqueued_ts", "1 minute"))
            .agg(F.count("*").alias("orders"),
                 F.sum(F.size("items")).alias("items"),
                 F.approx_count_distinct("user_id").alias("users"))
            .select(F.col("window.start").alias("minute"), "orders", "items", "users"))


@dp.table(name=f"{OPS}.streaming_latency", comment="Độ trễ từng chặng của mỗi sự kiện")
def streaming_latency():
    ms = lambda a, b: (F.col(b).cast("double") - F.col(a).cast("double")) * 1000
    return (spark.readStream.table("events_parsed")
            .where("event_id IS NOT NULL")
            .select("event_id", "replay_run_id", "producer_location",
                    "produced_at", "_enqueued_ts", "_ingest_ts", "_silver_ts",
                    ms("produced_at", "_enqueued_ts").alias("ms_producer_to_eh"),
                    ms("_enqueued_ts", "_ingest_ts").alias("ms_eh_to_bronze"),
                    ms("produced_at", "_silver_ts").alias("ms_end_to_end"),
                    F.lit(spark.conf.get("eh_region", "unknown")).alias("eh_region"),
                    F.lit(spark.conf.get("dbx_region", "unknown")).alias("dbx_region")))
```

- Trên classic compute, `stream_pair_counts_10m` ở chế độ *append* chỉ ghi một cửa sổ khi watermark đã qua cửa sổ đó, tức trễ khoảng 10 + 2 phút. Tile real-time vì vậy lấy từ `stream_kpi_1m` (cửa sổ 1 phút).
- `streaming_latency` không qua watermark nên có ngay.

### 4.4 Đo độ trễ khác region (rubric 3.3)

| Lần đo | Event Hubs | Databricks | Simulator | Chặng chính cần so |
|---|---|---|---|---|
| A (khác region) | Sub B, region B | Sub A, region A | Codespace / laptop | `ms_eh_to_bronze` |
| B (cùng region) | Tạm tạo hub ở region A (`stream-toggle up region=A`) | Sub A, region A | Như lần A | `ms_eh_to_bronze` |

```sql
SELECT replay_run_id, eh_region, dbx_region, count(*) n,
       percentile_approx(ms_eh_to_bronze, 0.5)  p50_eh_to_bronze,
       percentile_approx(ms_eh_to_bronze, 0.95) p95_eh_to_bronze,
       percentile_approx(ms_end_to_end, 0.5)    p50_e2e
FROM instacart.ops.streaming_latency GROUP BY ALL ORDER BY replay_run_id;
```

- `ms_eh_to_bronze` chỉ dùng **đồng hồ của Azure** (thời điểm enqueued của Event Hubs, và thời điểm ingest của Databricks), nên tin được hơn `produced_at` lấy từ đồng hồ laptop.
- Giá trị này gồm cả độ trễ mạng và **thời gian chờ micro-batch**. Vì vậy phải đo ở chế độ **continuous**, rồi so trung vị giữa hai lần đo.

### 4.5 Đẩy số lên tile Power BI (push semantic model)

**Ưu tiên:** ForEachBatch sink của LDP. Tính năng này đang preview; phải spike xem chạy được trên classic Advanced không.

```python
import json, requests

PUSH_URL = dbutils.secrets.get("kv", "pbi-push-url")


@dp.foreach_batch_sink(name="pbi_push")
def pbi_push(df, batch_id):
    rows = [{"minute": r.minute.isoformat(), "orders": r.orders, "items": r.items}
            for r in df.collect()]                        # vài dòng mỗi batch, collect được
    if rows:
        requests.post(PUSH_URL, data=json.dumps(rows), timeout=10).raise_for_status()


@dp.append_flow(target="pbi_push")
def kpi_to_pbi():
    return spark.readStream.table(f"{GOLD}.stream_kpi_1m")
```

Tên decorator `foreach_batch_sink` lấy theo tài liệu preview, cần kiểm lại khi spike.

**Dự phòng nếu không có ForEachBatch sink:**
- Một job Structured Streaming chạy ngoài pipeline, đọc `gold.stream_kpi_1m` và `foreachBatch` với cùng hàm, không chạy được cùng lúc. Lý do: nó cần cluster thứ hai, và cộng với pipeline continuous là 8 vCPU, vượt quota.
- Vì vậy dự phòng thực tế là: pipeline continuous chỉ cập nhật `gold.stream_kpi_1m`, còn Power BI đọc bảng này bằng DirectQuery và bật auto page refresh khi trình chiếu ([07 §6](07_power_bi.md)).

Gửi lại một batch (khi pipeline retry) chỉ làm tile nhận thêm một bản ghi của cùng phút. Tile hiển thị giá trị mới nhất nên không sai số. Đây là đầu ra duy nhất **không** cần chống trùng tuyệt đối.

## 5. Chế độ chạy

| Chế độ | Khi nào | Lệnh |
|---|---|---|
| Triggered theo lịch 15 phút | Khi Event Hubs hoạt động và lịch đã bật | Job `instacart-stream-15m` bỏ khung 02:00–03:00 giờ Việt Nam |
| Chạy tay một lần | Kiểm thử dev rồi kiểm event log | `databricks bundle run -t dev stream_schedule` |
| **Continuous** | Buổi demo, đo độ trễ | CI prod triển khai với `stream_continuous=true` và giữ lịch stream `PAUSED`. Sau demo, dừng pipeline rồi đưa cấu hình về triggered. |

Trước khi chạy continuous, tạm dừng lịch batch và xác nhận các lượt batch đang chạy đã kết thúc để giữ quota 4 vCPU.

## 6. Quy trình phát lại (replay)

Cây quyết định chạy lại (có nhánh replay) ở [06 §1](06_van_hanh_chay_lai.md).
Các lệnh phát đủ tập train, truy vấn đối soát ba mốc và demo 95/5 nằm trong
[runbook AD-ST-07](06_van_hanh_chay_lai.md#61-phát-lại-toàn-bộ-tập-train-ad-st-07).

**Nguồn sự thật là file `order_products__train` trên landing, không phải Event Hubs.** Event Hubs chỉ giữ dữ liệu 1–7 ngày và bị xóa sau demo.

1. **Ghi số trước khi phát:**
   ```sql
   SELECT count(*) FROM instacart.silver.stream_orders;
   SELECT count(*) FROM instacart.bronze.order_events;
   ```
2. **Event Hubs:**
   - Nếu hub đã bị xóa: chạy `stream-toggle up`, rồi deploy với `eh_generation` mới.
   - Nếu hub còn: giữ nguyên.
3. **Phát lại:**
   ```bash
   python simulate.py --run-id run-<mới> --from-frac 0 --to-frac 1
   ```
   Hoặc chỉ phát một phần.
4. **Chạy pipeline:** triggered hoặc continuous.
5. **Đối soát:**
   ```sql
   SELECT count(*) FROM instacart.silver.stream_orders;                 -- KHÔNG đổi (131.209)
   SELECT get_json_object(value_json, '$.replay_run_id') AS replay_run_id, count(*)
   FROM instacart.bronze.order_events GROUP BY 1;                          -- thêm một nhóm run mới
   SELECT replay_run_id, count(*) FROM instacart.silver.stream_orders GROUP BY 1;  -- dòng chuyển sang run mới
   ```
6. Ghi kết quả vào `ops.reconciliation`. Ảnh chụp bước 5 là bằng chứng "chạy lại không ghi đè, không nhân dữ liệu" cho báo cáo.

**Kịch bản demo 95/5:**
- Trước buổi demo phát `--to-frac 0.95`.
- Trong buổi demo phát `--from-frac 0.95 --to-frac 1.0` với `--rate 20`.
- Khán giả thấy tile tăng dần trong khoảng 5 phút.

## 7. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách xử lý | Vì sao an toàn |
|---|---|---|
| Simulator chết giữa chừng | Chạy lại cùng `--run-id` từ đầu (hoặc từ `--from-frac` gần nhất) | Đơn đã gửi được gửi lại → `AUTO CDC` cập nhật cùng khóa |
| Pipeline dừng (mất cluster) | Start lại | Checkpoint Kafka: đọc tiếp từ offset đã commit |
| Event Hubs bị xóa khi còn sự kiện chưa đọc | Phát lại phần đó (bước 3) | Nguồn sự thật là file trên landing |
| Sự kiện parse hỏng | Xem `quarantine_order_events`, sửa simulator, phát lại các đơn đó | Bronze giữ `value_json` gốc |
| Sai logic `stream_pair_counts_10m` | `full_refresh_selection = [gold.stream_pair_counts_10m]` | Tính lại từ toàn bộ bronze (bronze không bị reset) |

## 8. Checklist G2 và phần stream của G3

- [ ] G2: phát 1.000 sự kiện thử → `bronze.order_events` = 1.000; `silver.stream_orders` = 1.000.
- [ ] G2: phát lại cùng 1.000 với `run-id` khác → bronze = 2.000, silver **vẫn** 1.000.
- [ ] G3: phát đủ → `stream_orders` = 131.209; `stream_order_items` = 1.384.617 = `count(*)` của `bronze.order_products_train`.
- [ ] Đổi `eh_generation` sau khi tạo lại hub → bronze giữ dữ liệu cũ, nhận tiếp dữ liệu mới.
- [ ] `ops.streaming_latency` có số p50/p95 cho lần đo A và B.
