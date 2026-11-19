# Nghiệm thu silver.dim_product — AD-DE-14

## Phạm vi

MV ở [`silver_off.py`](../../src/pipelines/batch/silver_off.py), helper
[`common/product.py`](../../src/common/product.py), test tại [`test_product.py`](../../tests/test_product.py).
Pipeline `instacart_batch` nạp notebook qua glob hiện có; không thêm pipeline/job/cluster.

Mỗi dòng là một sản phẩm Instacart. Luôn đọc danh mục và các snapshot giá/khuyến mãi
AUTO CDC của AD-DE-11. Ba nguồn OFF chỉ được đọc khi bật `enable_off`. Hai chế độ công bố cùng schema. Chi tiết join/constraints ở
[DE/03 §6](03_silver_cdc_scd_dq.md#6-dim_product-join-nhiều-nguồn-mà-không-nhân-dòng).

## Điều kiện và chạy dev

- Cần nguồn Bronze, retail AD-DE-11 và quyền/schema/policy. Chế độ bật OFF cần thêm AD-DE-13 và dữ liệu OFF.
  Đọc [retail](03b_nghiem_thu_retail.md), [OFF](03d_nghiem_thu_off.md) và
  [bundle](../../bundles/README.md). Dùng cùng catalog/schema/off_root đã resolve.
- Cài CLI, đăng nhập và **để người chạy chọn profile**; không tự lấy profile đầu tiên.
- `as_of_date` trong configuration phải là ISO `YYYY-MM-DD`; mặc định `2026-03-30`.
  Ngày này chỉ chọn khuyến mãi. Giá dim luôn là hàng SCD2 có `__END_AT IS NULL`, khác giá lịch sử của fact.
- Trong `bundles/`, kiểm cấu hình, deploy và chạy một lần cho mỗi chế độ. Lần đầu đặt `enable_off=false`.
  Sau khi có lô OFF, lặp ba lệnh với `enable_off=true`.

```bash
export BUNDLE_VAR_enable_off=false
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> instacart_batch
```

Chỉ chạy một workload trong quota single-node hiện có. Không deploy prod thủ công,
không full refresh bronze/reset checkpoint. Ghi commit, ngày cấu hình, pipeline ID, update ID,
trạng thái update và Delta version của các bảng nguồn. Không lấy bảng dim cũ còn tồn tại làm bằng chứng
update mới thành công: expectation fail chỉ rollback flow liên quan.

## Đối soát số dòng và has_price thật

Sau update thành công, chạy trong notebook nghiệm thu. Thay catalog/schema theo bundle đã resolve;
giữ nguyên các nguồn trong lúc đo hoặc đọc bằng versionAsOf đã chốt. Các action dưới đây không nằm
trong dataset function.

```python
from pyspark.sql import functions as F

catalog, bronze, silver = "instacart", "p2_bronze", "p2_silver"
dim = spark.read.table(f"{catalog}.{silver}.dim_product")
products = spark.read.table(f"{catalog}.{bronze}.products")
expected = products.selectExpr("try_cast(product_id AS INT) AS product_id").where("product_id > 0").distinct().count()
actual = dim.count()
assert actual == expected == dim.select("product_id").distinct().count()
assert dim.where("product_id IS NULL OR product_id <= 0").count() == 0
assert dim.where("dup_cnt <> 1 OR NOT _join_sources_unique").count() == 0

dim.agg(
    F.count("*").alias("total_products"),
    F.count("current_price").alias("products_with_price"),
    F.sum(F.col("current_price").isNull().cast("int")).alias("products_without_price"),
    F.round(100.0 * F.count("current_price") / F.count("*"), 4).alias("pct_has_price"),
).show(truncate=False)

dim.where("off_match_status <> 'matched'").select(
    "product_id", "off_match_score", "off_match_status", "off_code", "brand", "image_path"
).show(truncate=False)
```

Full release phải có **49.688 dòng / 49.688 khóa**. Nguồn mẫu vẫn có thể giữ toàn bộ danh mục;
đọc manifest trước kết luận. Bảng rỗng không đạt nghiệm thu. `has_price` gần 100% là mục tiêu, phải
báo **products_with_price / total_products** và tỉ lệ thật, không làm tròn thành 100% khi còn thiếu.
`has_price` là warn: thiếu giá vẫn giữ sản phẩm; kiểm nguồn CDC/SCD2 và quarantine retail để giải thích thiếu.

Kiểm thêm một mẫu:

1. Giá chỉ từ hàng đang mở, không lấy MAX giá/giá cũ và không suy ra từ as_of_date.
2. Nhiều KM cùng ngày: lấy mức giảm lớn nhất; bao gồm cả ngày bắt đầu/kết thúc; 0% vẫn on_promotion=true.
3. Hai product_id dùng cùng barcode: brand/dinh dưỡng phải từ đúng cặp product_id/off_code.
4. low_confidence/no_match vẫn có điểm/nhãn, nhưng mã dùng để làm giàu, brand/dinh dưỡng/ảnh đều NULL.
   Ảnh cache tồn tại không được vượt qua điều kiện matched.
5. Bản metadata ảnh lặp không nhân dòng; path chọn theo modificationTime, ingest và thứ tự path.

## Chạy lại, lỗi nguồn và bằng chứng

- Chạy lại với **cùng nguồn và as_of_date**, so snapshot dim trước/sau bằng `exceptAll` hai chiều.
  Số dòng/khóa và toàn bộ business payload phải không đổi. Kiểm trên dev, không sửa file landing đã công bố.
- Nếu `unique_sources` fail: kiểm khóa trùng trong giá đang mở, bridge theo product_id và clean OFF theo cặp khóa.
  Khắc phục tại nguồn, giữ nguyên bronze rồi chạy lại.
- `no_fanout` kiểm số dòng sau join, `unique_sources` kiểm xung đột trước join; cả hai đều fail cho khóa đích và nguồn enrichment.
- CSV danh mục ưu tiên bản không rescue rồi chọn bản mới nhất. `catalog_unrescued` cảnh báo khi
  phải dùng bản rescue. `catalog_payload_unique` đếm xung đột trong nhóm bản được ưu tiên.
  `valid_catalog_product_id` đếm các khóa NULL, không ép được sang INT hoặc không dương ở đầu vào.
- Ghi issue #50: commit, ngày cấu hình, pipeline/update ID, nguồn/version, count/distinct,
  tử/mẫu số has_price, tỉ lệ, các khóa thiếu giá, kết quả kiểm mẫu và replay, event log các expectation.
  Cập nhật số thật tại [data_dictionary §6.1](../data/data_dictionary.md#61-hợp-đồng-silverdim_product-ad-de-14).

## Trạng thái hiện tại

Đã kiểm Spark local với fixture tự dựng, gồm thiếu nguồn, replay, xung đột và join OFF cùng barcode.
Test đăng ký `dp`/điều kiện expectations không thay engine LDP hoặc transaction Delta.

Đo trên catalog dev ngày 2026-11-19 với commit `672f6e0`.
Strict validation, deploy và hai lượt batch đều thành công.

| `enable_off` | Pipeline update | Số dòng | Khóa `product_id` | `has_price` | Dòng có dữ liệu OFF |
|---|---|---:|---:|---:|---:|
| `false` | [Update `d94b95c1`](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/d94b95c1-13f2-47b9-87d9-7a9ab5ad15ca) | 49.688 | 49.688 | 49.688 / 49.688 (100,0000%) | 0 |
| `true` | [Update `7ec7d6c7`](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/7ec7d6c7-bccb-4269-b35e-5654f4891bb9) | 49.688 | 49.688 | 49.688 / 49.688 (100,0000%) | 259 |

OFF tắt cho **49.688** dòng `no_match` với điểm 0, toàn bộ thuộc tính OFF và ảnh là NULL.
OFF bật dùng lô 305 sản phẩm. **259** sản phẩm có nhãn `matched` và được làm giàu.

Còn chờ kiểm replay Delta và triển khai prod.
