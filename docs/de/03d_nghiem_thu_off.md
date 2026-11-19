# Nghiệm thu OFF và bảng cầu nối AD-DE-13

## Phạm vi và điều kiện

Code: [`silver_off.py`](../../src/pipelines/batch/silver_off.py),
[`common/off.py`](../../src/common/off.py), kiểm thử tại [`test_off.py`](../../tests/test_off.py).
Ba MV do `instacart_batch` sở hữu trong schema silver đã resolve:

| Dataset | Khóa / mục đích |
|---|---|
| `off_products_clean` | Một bản hợp lệ mới nhất cho mỗi `(product_id, off_code)` |
| `product_off_match` | Một dòng cho mỗi `product_id` trong danh mục Instacart |
| `quarantine_off_products` | Mọi raw payload lỗi khóa/ingest/rescue trước khử trùng |

Đọc [bộ luật và cách ghép](03_silver_cdc_scd_dq.md#5-làm-giàu-từ-open-food-facts-làm-phẳng-json-và-fuzzy-match)
trước nghiệm thu. Chỉ nhận hit của đúng sản phẩm đã tra, có nhãn quốc gia Mỹ.
Các trường dinh dưỡng không parse được thành NULL; có thể xem lại raw ở bronze.
Tên thiếu vẫn giữ trong clean với expectation warn và điểm 0.

AD-DE-10 (#46) đã nghiệm thu crawler và Bronze OFF trên dev. Dùng nguồn Bronze của #45,
bundle #44 và quyền/schema/policy trong [bundles/README](../../bundles/README.md).
Dev phải dùng **cùng `off_root`** giữa crawler và bronze; không trộn lô prod vào dev.
Chạy crawler lần đầu với `source=landing` nếu bronze OFF chưa tồn tại theo
[hướng dẫn crawler](../../src/ingest/README.md). Không reset checkpoint hay full refresh bronze.

## Chạy dev

Sau khi xác nhận `off_root` có lô JSONL và ảnh, chọn profile dev rồi chạy trong `bundles/`:

```bash
export BUNDLE_VAR_enable_off=true
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> instacart_batch
```

Cờ mặc định là `false`. Khi bật, pipeline đăng ký các bảng bronze và ba MV OFF.
Notebook được glob hiện tại nạp; ô đầu cài `rapidfuzz==3.14.6`, cùng phiên bản trong lockfile.
Driver nạp `common` từ `root_path=src` và đóng gói scorer cùng hàm chuẩn hóa theo giá trị. Worker dùng RapidFuzz trong môi trường pipeline.
Pipeline chỉ dùng cluster/policy hiện có; tránh chạy cùng crawler/seed/stream trong quota 4 vCPU.
Không deploy prod thủ công. Lưu commit, pipeline ID, update ID và trạng thái update thực tế.

## Lấy số thật

Sau update thành công, chạy trong notebook kiểm tra có `src` trên Python path; thay catalog/schema
bằng giá trị dev đã resolve. Các action dưới đây là thao tác nghiệm thu, **không đặt trong dataset function**.

```python
from pyspark.sql import functions as F
from common.off import match_statistics

catalog, bronze, silver = "instacart", "p2_bronze", "p2_silver"
matches = spark.read.table(f"{catalog}.{silver}.product_off_match")
products = spark.read.table(f"{catalog}.{bronze}.products")
clean = spark.read.table(f"{catalog}.{silver}.off_products_clean")

expected = products.select("product_id").distinct().count()
actual = matches.count()
assert actual == expected == matches.select("product_id").distinct().count()
assert matches.where("product_id IS NULL OR product_id <= 0").count() == 0
assert clean.count() == clean.select("product_id", "off_code").distinct().count()
assert matches.where("off_match_score IS NULL OR off_match_score < 0 OR off_match_score > 100").count() == 0
match_statistics(matches).orderBy("off_match_status").show(truncate=False)

# Tách độ phủ crawler và số sản phẩm có ứng viên Mỹ khỏi tỷ lệ matched toàn danh mục.
state = spark.read.table(f"{catalog}.p2_ops.off_crawl_state")  # dùng off_state_schema đã resolve
state.groupBy("status").count().show()
print("Sản phẩm có hit Mỹ:", clean.where(
    F.array_contains("countries_tags", "en:united-states")
).select("product_id").distinct().count())
spark.read.table(f"{catalog}.{silver}.quarantine_off_products").select(
    "product_id", "off_code", "_dq_failed"
).show(truncate=False)
```

Release đầy đủ phải có **49.688 dòng / 49.688 khóa** trong bridge. Mẫu 1% có thể vẫn chứa toàn bộ
danh mục tham chiếu; luôn đối chiếu manifest và bảng products thật thay vì đoán từ tên mẫu.
`match_statistics` trả đủ ba nhãn, cột `count`, `total_products`, `pct_all_products`; nhãn vắng có 0.
Mẫu số là toàn bộ bridge, không chỉ sản phẩm có hit. Bảng rỗng cho tỉ lệ 0 và **không** được nghiệm thu
như full release. `no_match` gộp chưa crawl, no_result, bị lọc quốc gia và ứng viên điểm thấp.

Kiểm tay một mẫu ở mỗi nhãn, nhất là các tên có một tập token nằm trọn trong tên kia
vì token_set_ratio có thể cho điểm 100. Ở AD-DE-14 chỉ cho `matched` làm giàu BI.

## Chạy lại và báo cáo

1. Giữ nguyên nguồn bronze, chạy update pipeline thêm lần nữa. So bridge theo đủ bốn cột bằng
   `exceptAll` hai chiều với snapshot trước; số dòng, distinct key, điểm/nhãn phải không đổi.
   Không so `_quarantined_at` vì đó là thời điểm đánh giá MV, không phải khóa hay thời điểm lỗi nguồn.
2. Chỉ ở dev: nạp bản mới cho cùng `(product_id, off_code)` bằng file landing mới bất biến;
   xác nhận clean chọn ingest mới nhất và bridge vẫn không nhân dòng. Không sửa file đã công bố.
3. Kiểm expectation `has_name`, các luật bridge và nội dung quarantine trên UI/event log.
   Expectation fail chỉ rollback flow liên quan, không xác nhận các dataset khác cùng update đã rollback.
4. Ghi số thật vào [data_dictionary §5.6](../data/data_dictionary.md#56-cách-join-ghép-theo-tên-qua-bảng-cầu-nối)
   và comment issue #49: commit, update ID, thời điểm đo, nguồn/Delta version, số sản phẩm,
   số crawl theo status, số có hit Mỹ, số/tỉ lệ đủ ba nhãn, số quarantine, kết quả replay và kiểm tay.

**Trạng thái hiện tại:** đã kiểm trên Spark local và chạy đủ ba flow Silver trên dev với lô 305 sản phẩm.
UDF hoàn tất trên worker. Tỷ lệ theo từng nhóm và thông tin update nằm ở
[data dictionary §5.6](../data/data_dictionary.md#56-cách-join-ghép-theo-tên-qua-bảng-cầu-nối).
Replay Delta và tỷ lệ trên lô prod đầy đủ còn chờ nghiệm thu.
