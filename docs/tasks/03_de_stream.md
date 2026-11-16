# Phân việc · DE streaming (`AD-ST-xx`)

10 task. Quy ước, trạng thái và bảng tổng ở [README](README.md). Vai ghi theo P1–P4, người thật được gán trên GitHub Project.

| Mã | Việc | Vai | Cổng | Phụ thuộc | Issue |
|---|---|---|---|---|---|
| [AD-ST-01](#ad-st-01) | Viết simulator dựng sự kiện đơn hàng từ tập train | P3 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DE-02 | [#64](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/64) |
| [AD-ST-02](#ad-st-02) | Gửi sự kiện thử vào Event Hubs bằng giao thức Kafka | P3 | G2 | Bắt đầu: AD-ST-01; Hoàn tất: AD-OPS-24 | [#65](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/65) |
| [AD-ST-03](#ad-st-03) | Khai báo pipeline instacart_stream và job instacart-stream-15m trong DAB | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-DE-08, AD-OPS-21, AD-OPS-20, AD-OPS-22, AD-OPS-24 | [#66](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/66) |
| [AD-ST-04](#ad-st-04) | Viết bronze_events đọc Kafka vào bronze.order_events | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-ST-02, AD-ST-03 | [#67](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67) |
| [AD-ST-05](#ad-st-05) | Dựng silver stream: parse, kiểm chất lượng, AUTO CDC theo order_id | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-DE-03, AD-ST-04 | [#68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68) |
| [AD-ST-06](#ad-st-06) | Dựng gold stream và bảng độ trễ ops.streaming_latency | P3 | G2 | Chuẩn bị ngay; Hoàn tất: AD-ST-05 | [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69) |
| [AD-ST-07](#ad-st-07) | Phát đủ tập train, phát lại và đối soát phần stream | P3 | G3 | Bắt đầu: AD-ST-05; Hoàn tất: AD-ST-06 | [#70](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/70) |
| [AD-ST-08](#ad-st-08) | Kiểm đổi eh_generation khi Event Hubs được tạo lại | P3 | G2 | Bắt đầu: AD-ST-04; Hoàn tất: AD-OPS-24, AD-OPS-05 | [#71](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/71) |
| [AD-ST-09](#ad-st-09) | Đo độ trễ Event Hubs khác region và cùng region | P3 | G4 | Bắt đầu: AD-ST-06; Hoàn tất: AD-OPS-24 | [#72](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/72) |
| [AD-ST-10](#ad-st-10) | Đẩy KPI stream lên push semantic model của Power BI | P3 | G4 | Bắt đầu: AD-ST-06; Hoàn tất: AD-BI-08, AD-OPS-05 | [#73](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/73) |

### AD-ST-01

**Viết simulator dựng sự kiện đơn hàng từ tập train**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G0**
- Issue: [#64](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/64)
- Tài liệu: `docs/de/04_streaming.md §2`, `docs/data/data_dictionary.md §4`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-02](02_de_batch.md#ad-de-02) [#38](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/38)
- **Mở khóa:** [AD-ST-02](03_de_stream.md#ad-st-02) (bắt đầu)

Simulator phát đơn train thành sự kiện JSON. Phần dựng sự kiện viết và test được local trước khi có Event Hubs.

**Việc cần làm**

1. Viết src/streaming/simulator/simulate.py: build_events, dựng body sự kiện, cắt --from-frac/--to-frac
2. Dùng order_ts_sim từ src/common/sim_time.py (không chép công thức)
3. Thêm chế độ --dry-run ghi JSON lines ra file để kiểm không cần Kafka
4. Viết tests/test_simulator.py: schema sự kiện, event_id = order-<id>, NaN → null, thứ tự theo event_time

**Đầu ra**

- src/streaming/simulator/simulate.py
- tests/test_simulator.py
- README simulator cập nhật cách chạy

**Xong khi**

- [ ] uv run pytest tests/test_simulator.py xanh
- [ ] Dry-run trên mẫu 1% ra đúng số đơn train của mẫu
- [ ] event_time khớp order_ts_sim của silver cho cùng đơn

### AD-ST-02

**Gửi sự kiện thử vào Event Hubs bằng giao thức Kafka**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G2**
- Issue: [#65](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/65)
- Tài liệu: `docs/de/04_streaming.md §2.2`
- **Bắt đầu sau:** [AD-ST-01](03_de_stream.md#ad-st-01) [#64](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/64)
- **Hoàn tất còn cần:** [AD-OPS-24](01_devops.md#ad-ops-24) [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26)
- **Mở khóa:** [AD-ST-04](03_de_stream.md#ad-st-04) (hoàn tất)

Nối simulator với Event Hubs ở Sub B (khác region), key là order_id, producer idempotent.

**Việc cần làm**

1. Thêm Producer confluent-kafka (SASL_SSL, enable.idempotence, gzip)
2. Đọc connection string từ biến môi trường EH_SEND_CONN, không ghi ra file
3. Gửi 1.000 sự kiện thử với run-id riêng
4. Ghi producer_location cho từng nơi chạy (laptop, Codespace)

**Đầu ra**

- simulate.py có nhánh Kafka
- Lệnh chạy mẫu trong README simulator

**Xong khi**

- [ ] Event Hubs nhận 1.000 message (xem metric hoặc kcat)
- [ ] Không có connection string trong repo hay log

### AD-ST-03

**Khai báo pipeline instacart_stream và job instacart-stream-15m trong DAB**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G2**
- Issue: [#66](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/66)
- Tài liệu: `docs/de/04_streaming.md §3.1, §5`, `docs/de/06_van_hanh_chay_lai.md §3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-08](02_de_batch.md#ad-de-08) [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44), [AD-OPS-21](01_devops.md#ad-ops-21) [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24), [AD-OPS-24](01_devops.md#ad-ops-24) [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26)
- **Mở khóa:** [AD-ST-04](03_de_stream.md#ad-st-04) (hoàn tất)

Pipeline stream độc lập với batch, triggered mỗi 15 phút hoặc continuous khi demo; mỗi lần tạo lại hub dùng eh_generation mới.

**Việc cần làm**

1. Viết bundles/resources/pipeline_stream.yml (biến stream_continuous, eh_generation, eh_region, dbx_region)
2. Khai báo job instacart-stream-15m bỏ qua khung 02:00–03:00
3. Thêm event_log vào ops (event_log_stream)
4. databricks bundle validate -t dev

**Đầu ra**

- bundles/resources/pipeline_stream.yml

**Xong khi**

- [ ] bundle validate -t dev đạt
- [ ] Deploy dev tạo được pipeline và job (schedule bị tạm dừng ở dev)

### AD-ST-04

**Viết bronze_events đọc Kafka vào bronze.order_events**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G2**
- Issue: [#67](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67)
- Tài liệu: `docs/de/04_streaming.md §3.2`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-ST-02](03_de_stream.md#ad-st-02) [#65](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/65), [AD-ST-03](03_de_stream.md#ad-st-03) [#66](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/66)
- **Mở khóa:** [AD-ST-05](03_de_stream.md#ad-st-05) (hoàn tất), [AD-ST-08](03_de_stream.md#ad-st-08) (bắt đầu)

Bronze stream giữ nguyên bản tin JSON và metadata Event Hubs, append-only, flow mang tên theo eh_generation.

**Việc cần làm**

1. Viết src/pipelines/stream/bronze_events.py: create_streaming_table + append_flow kafka_<gen>
2. Đọc eh-listen-connection-string bằng dbutils.secrets.get('kv', …)
3. Đặt maxOffsetsPerTrigger 20000, pipelines.reset.allowed = false
4. Chạy với 1.000 sự kiện thử

**Đầu ra**

- src/pipelines/stream/bronze_events.py
- Bảng bronze.order_events

**Xong khi**

- [ ] bronze.order_events = 1.000 dòng, không trùng event_id trong một run
- [ ] Chạy update lần hai không đọc lại offset đã commit

### AD-ST-05

**Dựng silver stream: parse, kiểm chất lượng, AUTO CDC theo order_id**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G2**
- Issue: [#68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68)
- Tài liệu: `docs/de/04_streaming.md §4.1, §4.2, §8`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-03](02_de_batch.md#ad-de-03) [#39](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/39), [AD-ST-04](03_de_stream.md#ad-st-04) [#67](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67)
- **Mở khóa:** [AD-DE-15](02_de_batch.md#ad-de-15) (hoàn tất), [AD-ST-06](03_de_stream.md#ad-st-06) (hoàn tất), [AD-ST-07](03_de_stream.md#ad-st-07) (bắt đầu)

Silver stream_orders và stream_order_items upsert theo khóa nên phát lại không nhân dòng; bản tin hỏng vào quarantine_order_events.

**Việc cần làm**

1. Viết src/pipelines/stream/silver_events.py: schema EVENT_V1, events_parsed, events_valid với EVENT_RULES
2. AUTO CDC SCD1 cho stream_orders (khóa order_id) và stream_order_items (khóa order_id, product_id)
3. Thêm quarantine_order_events bằng khuôn dq dùng chung
4. Test parse schema trên Spark local với vài JSON mẫu (đúng, hỏng, sai version)
5. Phát lại cùng 1.000 sự kiện với run-id khác

**Đầu ra**

- src/pipelines/stream/silver_events.py
- tests/test_stream_parse.py
- Bảng silver.stream_orders, stream_order_items, quarantine_order_events

**Xong khi**

- [ ] G2: silver.stream_orders = 1.000 sau lần phát đầu
- [ ] G2: phát lại 1.000 → bronze 2.000, silver vẫn 1.000, replay_run_id chuyển sang run mới
- [ ] Bản tin hỏng có trong quarantine_order_events

### AD-ST-06

**Dựng gold stream và bảng độ trễ ops.streaming_latency**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G2**
- Issue: [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69)
- Tài liệu: `docs/de/04_streaming.md §4.3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-ST-05](03_de_stream.md#ad-st-05) [#68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68)
- **Mở khóa:** [AD-ST-07](03_de_stream.md#ad-st-07) (hoàn tất), [AD-ST-09](03_de_stream.md#ad-st-09) (bắt đầu), [AD-ST-10](03_de_stream.md#ad-st-10) (bắt đầu), [AD-BI-07](04_power_bi.md#ad-bi-07) (hoàn tất)

Gold gần thời gian thực theo thời gian nhận: KPI mỗi phút cho tile và trang 5, cặp sản phẩm cửa sổ 10 phút, độ trễ từng chặng.

**Việc cần làm**

1. Viết src/pipelines/stream/gold_stream.py: deduped_events (watermark 2 phút)
2. gold.stream_kpi_1m, gold.stream_pair_counts_10m
3. ops.streaming_latency với ms_producer_to_eh, ms_eh_to_bronze, ms_end_to_end, eh_region, dbx_region
4. Chạy với sự kiện thử và kiểm số

**Đầu ra**

- src/pipelines/stream/gold_stream.py
- Bảng gold.stream_kpi_1m, gold.stream_pair_counts_10m, ops.streaming_latency

**Xong khi**

- [ ] Tổng orders của stream_kpi_1m bằng số sự kiện hợp lệ đã nhận (trong một run)
- [ ] ops.streaming_latency có một dòng mỗi sự kiện parse được, nhãn region đúng
- [ ] stream_pair_counts_10m có cửa sổ đã chốt sau watermark

### AD-ST-07

**Phát đủ tập train, phát lại và đối soát phần stream**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G3**
- Issue: [#70](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/70)
- Tài liệu: `docs/de/04_streaming.md §6, §7, §8`, `docs/de/06_van_hanh_chay_lai.md §6 (dòng 11, 12)`
- **Bắt đầu sau:** [AD-ST-05](03_de_stream.md#ad-st-05) [#68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68)
- **Hoàn tất còn cần:** [AD-ST-06](03_de_stream.md#ad-st-06) [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69)
- **Mở khóa:** [AD-DE-17](02_de_batch.md#ad-de-17) (hoàn tất), [AD-DE-27](02_de_batch.md#ad-de-27) (hoàn tất)

G3 cần đủ 131.209 đơn stream trong silver. Phát lại toàn bộ là bằng chứng chạy lại không nhân dữ liệu.

**Việc cần làm**

1. Ghi số trước khi phát, phát đủ 100% tập train
2. Đối soát stream_orders, stream_order_items với bronze.order_products_train
3. Phát lại toàn bộ với run-id mới (kịch bản C của de/06 §7), chụp ảnh đối soát
4. Viết phần stream của runbook: replay, Event Hubs bị xóa, checkpoint hỏng
5. Chuẩn bị lệnh demo 95/5 (--to-frac 0.95, rồi 0.95–1.0 với --rate 20)

**Đầu ra**

- Ảnh đối soát trước/sau phát lại
- Phần stream của runbook

**Xong khi**

- [ ] silver.stream_orders = 131.209
- [ ] silver.stream_order_items = 1.384.617 = count(*) bronze.order_products_train
- [ ] Sau phát lại: stream_orders không đổi, bronze tăng thêm một nhóm replay_run_id

### AD-ST-08

**Kiểm đổi eh_generation khi Event Hubs được tạo lại**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G2**
- Issue: [#71](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/71)
- Tài liệu: `docs/de/04_streaming.md §3.2`, `docs/devops/06_de_den_dashboard.md §2.2`
- **Bắt đầu sau:** [AD-ST-04](03_de_stream.md#ad-st-04) [#67](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67)
- **Hoàn tất còn cần:** [AD-OPS-24](01_devops.md#ad-ops-24) [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26), [AD-OPS-05](01_devops.md#ad-ops-05) [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7)
- **Mở khóa:** Không có.

Event Hubs bị xóa mỗi đêm và offset về 0. Flow mới theo eh_generation tạo checkpoint mới mà bronze vẫn giữ dữ liệu cũ.

**Việc cần làm**

1. Tạo lại hub bằng stream-toggle up với eh_generation mới
2. Deploy pipeline với --var eh_generation=g<yyyymmdd>
3. Phát một lô nhỏ, chạy update
4. Nếu lỗi: chuyển sang giữ một namespace cố định suốt giai đoạn demo và ghi vào báo cáo chi phí

**Đầu ra**

- Ghi chú kết quả trong runbook stream

**Xong khi**

- [ ] Update với flow mới chạy xanh
- [ ] bronze.order_events giữ dữ liệu cũ và có thêm dữ liệu mới
- [ ] silver.stream_orders không nhân dòng

### AD-ST-09

**Đo độ trễ Event Hubs khác region và cùng region**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G4**
- Issue: [#72](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/72)
- Tài liệu: `docs/de/04_streaming.md §4.4, §5`
- **Bắt đầu sau:** [AD-ST-06](03_de_stream.md#ad-st-06) [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69)
- **Hoàn tất còn cần:** [AD-OPS-24](01_devops.md#ad-ops-24) [#26](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/26)
- **Mở khóa:** [AD-BI-07](04_power_bi.md#ad-bi-07) (hoàn tất), [AD-RP-03](06_bao_cao_demo.md#ad-rp-03) (bắt đầu)

Bằng chứng rubric mục 3 về độ trễ khi server ở region khác: so trung vị ms_eh_to_bronze giữa lần đo A và B.

**Việc cần làm**

1. Tạm dừng pl_master và job batch, chạy pipeline ở chế độ continuous
2. Lần A: hub ở Sub B (region B), phát một lô với run-id riêng
3. Lần B: hub tạm ở region A, cùng simulator và cùng tốc độ
4. Chạy truy vấn p50/p95, chụp ảnh, deploy lại stream_continuous=false
5. Viết đoạn phân tích ngắn cho báo cáo

**Đầu ra**

- Bảng p50/p95 theo replay_run_id, eh_region trong ops.streaming_latency
- Ảnh và đoạn phân tích cho báo cáo

**Xong khi**

- [ ] ops.streaming_latency có số p50/p95 cho cả lần A và B
- [ ] Pipeline đã về triggered sau khi đo

### AD-ST-10

**Đẩy KPI stream lên push semantic model của Power BI**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G4**
- Issue: [#73](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/73)
- Tài liệu: `docs/de/04_streaming.md §4.5`, `docs/de/07_power_bi.md §6`
- **Bắt đầu sau:** [AD-ST-06](03_de_stream.md#ad-st-06) [#69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69)
- **Hoàn tất còn cần:** [AD-BI-08](04_power_bi.md#ad-bi-08) [#81](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/81), [AD-OPS-05](01_devops.md#ad-ops-05) [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7)
- **Mở khóa:** [AD-BI-12](04_power_bi.md#ad-bi-12) (bắt đầu)

Tile real-time cần số mới trong vài giây. Ưu tiên ForEachBatch sink của LDP, dự phòng là DirectQuery + auto page refresh.

**Việc cần làm**

1. Thêm sink pbi_push và append_flow từ gold.stream_kpi_1m vào gold_stream.py
2. Đọc push URL bằng dbutils.secrets.get('kv', 'pbi-push-url')
3. Nếu spike không cho ForEachBatch sink: ghi quyết định dùng dự phòng, không chạy job thứ hai
4. Chạy continuous với lô nhỏ và quan sát tile

**Đầu ra**

- gold_stream.py có sink pbi_push (hoặc ghi chú dự phòng)

**Xong khi**

- [ ] Tile Instacart Live tăng trong ≤ 1 phút sau khi phát sự kiện
- [ ] Gửi lại batch khi retry không làm sai số hiển thị
