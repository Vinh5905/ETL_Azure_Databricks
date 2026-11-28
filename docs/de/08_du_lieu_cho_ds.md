# 08 · Chuẩn bị dữ liệu cho DS (silver → `ds_data`)

**Đầu vào:**
- Checklist G3 đã đạt ([03 §9](03_silver_cdc_scd_dq.md)): `silver.baskets` có đủ `prior` và `holdout`, stream đã phát xong.
- Schema `ds_data` và volume `ds_data.exports` đã có ([devops/05 bước 4](../devops/05_databricks_platform.md)).

**Đầu ra (schema `ds_data`, DE sở hữu, DS chỉ đọc):**

| Bảng / file | Nội dung | Ai dùng |
|---|---|---|
| `ds_data.data_versions` | Mỗi phiên bản dữ liệu một dòng: thời điểm tạo, số giỏ `prior` / `holdout` | Đối soát |
| `ds_data.baskets` | **Bản cố định** của `silver.baskets` theo `data_version` | Nguồn của mọi bảng dưới |
| `ds_data.ds_prepare_products` | Danh mục cố định theo phiên bản | Nội bộ DE, DS không dùng |
| `ds_data.ds_prepare_holdout` | Thứ tự món stream cố định theo phiên bản | Nội bộ DE, DS không dùng |
| `ds_data.item_index` | Mã hóa item thành `idx` (0 = phổ biến nhất) cho 3 cấp `product` / `aisle` / `department` | DS, view `gold.*` |
| `ds_data.transactions` | Giỏ hàng theo 3 cấp, `items array<int>` tăng dần, `scale_bucket` 0–99 | DApriori, DATID, FP-Growth |
| `ds_data.holdout_eval` | Đơn holdout chia thành `context` / `targets`, chia `val` / `test` | Đánh giá gợi ý (E7) |
| `/Volumes/instacart/ds_data/exports/<v>/<grain>/scale=<pct>/part-*.txt` + `<v>/_manifest.json` | File txt, mỗi dòng một giao dịch | Hadoop MapReduce / Spark ở máy local ([ds/03](../ds/03_hadoop_mapreduce_local.md)) |

**Xong khi:** checklist mục 7 đạt, và DS xác nhận đọc được (mốc G3 của DS, [ds/00 §5](../ds/00_tong_quan_ds.md)).

---

## 1. Vì sao DE làm phần này

- **Một nguồn dữ liệu cho mọi thực nghiệm.** Databricks và cụm Hadoop local đều đọc từ cùng `ds_data.transactions` (local đọc qua file txt xuất ra). Số cặp phổ biến của MapReduce vì vậy so được với số của DApriori.
- **Dữ liệu cố định.** `silver.baskets` là materialized view và có thể được tính lại khi pipeline chạy. Thực nghiệm kéo dài nhiều tuần thì cần một bản **không đổi**.
  - Không dùng `VERSION AS OF` trên MV, vì time travel trên bảng do pipeline quản lý không chắc được hỗ trợ.
  - Không dùng `CLONE`, vì clone không áp dụng cho MV / streaming table.
  - Dùng **`INSERT … SELECT` có chặn ghi lần hai**.
- **Quyền rõ ràng.** DS có `SELECT` trên `ds_data` và không sửa được dữ liệu đầu vào của chính mình.

## 2. Snapshot theo `data_version`

[`01_snapshot.sql`](../../src/tasks/ds_prepare/01_snapshot.sql) cố định ba đầu vào:

| Bảng | Dữ liệu theo phiên bản |
|---|---|
| `ds_data.baskets` | Giỏ prior/holdout từ `silver.baskets`, giữ tập `product_id` tăng dần |
| `ds_data.ds_prepare_products` | Khóa, tên, aisle và department từ `silver.dim_product` |
| `ds_data.ds_prepare_holdout` | Danh sách `product_id` theo `add_to_cart_order` của stream cho từng giỏ holdout |

`order_id`, `user_id`, `product_id` và khóa danh mục phải nằm trong miền INT dương. Script dừng khi
phát hiện giỏ trùng, sản phẩm không có trong danh mục, hoặc thứ tự món stream thiếu, trùng hay lệch tập
sản phẩm của giỏ. Khi cả ba phần đã ghi và kiểm đủ prior/holdout, script ghi `data_versions` với số giỏ,
nguồn và thời điểm tạo. Metadata này xác nhận snapshot sẵn sàng cho bước mã hóa.

Mỗi phần chỉ insert khi chưa có dữ liệu của phiên bản đó. Nếu lỗi giữa các phần, repair dùng cùng
catalog, schema và nguồn được giữ ổn định từ lần đầu. Delta commit theo từng bảng, nên cần giữ nguồn
silver đứng yên đến khi snapshot hoàn tất và chỉ chạy một writer vào schema DS. Các phần đã ghi được
sử dụng lại. Nếu cần thay dữ liệu đầu vào đã cố định, dùng một phiên bản mới và lưu lại nguyên nhân.

**Quy tắc phiên bản**

- Tạo `v1` sau G3 và giữ dữ liệu đó cố định. Các lần chạy lại cùng phiên bản chỉ tái tạo dữ liệu dẫn xuất từ snapshot đã chốt.
- Khi cần dữ liệu khác, tạo `v2` và ghi lý do để DS chạy lại thí nghiệm. Giữ `v1` để so sánh.
- Nếu thực sự phải tạo lại `v1`, thông báo DS để loại bỏ kết quả cũ, tạm dừng mọi writer và
  xóa riêng phiên bản đó (`DELETE … WHERE data_version = 'v1'`) ở đủ bảy bảng
  `data_versions`, `baskets`, `ds_prepare_products`, `ds_prepare_holdout`, `item_index`,
  `transactions` và `holdout_eval`. Chỉ chạy lại sau khi đối soát cả bảy bảng đã sạch `v1`.
  Nếu đã có file xuất, thu hồi manifest cũ trước khi công bố lại.

## 3. Mã hóa item và giao dịch theo 3 cấp

[`02_index_transactions.sql`](../../src/tasks/ds_prepare/02_index_transactions.sql) dùng dữ liệu snapshot:

- `item_index` có ba cấp `product`, `aisle`, `department`. `freq_prior` đếm **số giỏ prior chứa item**.
  `idx` đánh liên tục từ 0 theo tần suất giảm dần và khóa số tăng dần khi hòa. DS lọc top-N bằng `idx < N`.
  Thứ tự support này giúp DATID bỏ qua bước sắp xếp ([ds/02 §3](../ds/02_thuat_toan_spark.md)).
- `transactions.items` là tập `idx` tăng dần. Một đơn mua nhiều sản phẩm cùng aisle hoặc department
  vẫn có một `idx` của nhóm đó. Holdout chỉ giữ item đã có trong prior theo từng cấp. Nếu một giỏ không
  còn item ở một cấp, giỏ đó không tạo giao dịch ở cấp đó. Ghi số món holdout bị bỏ do
  không có trong prior trong checklist §7.
- `scale_bucket = pmod(xxhash64(order_id), 100)`, với `order_id` kiểu INT. Các ngưỡng 10, 25, 50, 100
  tạo tập con lồng nhau và tất định ([ds/01 §1](../ds/01_du_lieu_thuc_nghiem.md)).
- `REPLACE WHERE data_version = :v` tính lại riêng index và transactions của phiên bản đó. Danh mục
  đã cố định, nên thay đổi tên hoặc phân loại ở silver không đổi mã hay giao dịch của `v1`.

## 4. Tập đánh giá `ds_data.holdout_eval`

[`03_holdout.sql`](../../src/tasks/ds_prepare/03_holdout.sql) mã hóa danh sách holdout đã cố định ở bước 01.
Lọc sản phẩm chưa có trong prior rồi chọn đơn còn ít nhất hai item. `context` là nửa đầu làm tròn lên,
`targets` là phần còn lại **theo thứ tự bỏ vào giỏ**. Sau khi chia, cả hai mảng được sắp tăng dần theo
`idx` để DS so tập bằng phép `=`. Hash `xxhash64(order_id, 'eval')` chia đơn vào `val` hoặc `test`.
Bảng chỉ insert lần đầu cho mỗi phiên bản. Nếu kết quả bằng 0 dòng, replay cũng cho cùng kết quả vì
đầu vào đã cố định.

## 5. Xuất file cho Hadoop local

[`export_txt.py`](../../src/tasks/ds_prepare/export_txt.py) lấy `transactions` của
phiên bản đã chọn và chỉ xuất split `prior`. Mỗi dòng chứa các `idx` cách nhau
bằng dấu cách. Giữ nguyên `scale_bucket` để ba cấp có cùng tập đơn ở mỗi quy mô.

Job ghi 12 thư mục `/Volumes/<catalog>/<ds_schema>/exports/<v>/<grain>/scale=<pct>/`
với ba cấp và bốn quy mô 10/25/50/100. Sau mỗi lần ghi, Spark đọc lại và so số dòng
với số giao dịch nguồn. Job thu hồi manifest trước khi ghi đè và công bố
`<v>/_manifest.json` sau khi cả 12 thư mục đạt. DS chỉ tải file khi job `check`
đã đạt và manifest có đủ 12 số đếm.

Volume prod `instacart.ds_data.exports` trỏ vào `ds-data/exports` cho AzCopy.
Dev dùng volume `exports` trong schema dev riêng. Xem cách đưa file vào HDFS
ở [DS/03 §3](../ds/03_hadoop_mapreduce_local.md).

## 6. Job `ds-prepare`

[`jobs_ds.yml`](../../bundles/resources/jobs_ds.yml) khai báo `ds_prepare` với
`max_concurrent_runs: 1`, không có lịch. Ba task dùng chung cluster single-node.
Chạy khi quota 4 vCPU rảnh và nguồn Silver đã đạt G3.

| Task | Việc thực hiện |
|---|---|
| `prepare_tables` | Chạy 01 → 02 → 03 qua [`run_sql.py`](../../src/tasks/ds_prepare/run_sql.py) để cố định snapshot và tạo bảng dẫn xuất |
| `export_txt` | Xuất và kiểm 12 nhóm file rồi ghi manifest |
| `check` | Chạy [`04_check.sql`](../../src/tasks/ds_prepare/04_check.sql) để chặn bàn giao nếu dữ liệu sai |

Tạo schema DS dev và volume managed `exports` trong schema đó trước khi chạy.
Trong `bundles/`, deploy target `dev` rồi chạy với Silver đã nghiệm thu và
schema đích dev. Ví dụ:

```bash
databricks bundle run -t dev --params data_version=v1,silver_schema=<silver_dev>,ds_schema=<ds_dev> ds_prepare
```

Giữ nguyên `catalog`, `silver_schema`, `ds_schema` và `data_version` khi repair
hoặc chạy lại. Runner kiểm và quote tên bảng, phiên bản trước khi chạy SQL trên DBR 15.4. Prod dùng mặc định
`silver` và `ds_data`, chạy tay sau khi G3 của prod đạt.

## 7. Checklist (mốc G3 của DS)

[`04_check.sql`](../../src/tasks/ds_prepare/04_check.sql) kiểm số giỏ prior/holdout
của phiên bản khớp nguồn và snapshot, prior đủ ba cấp với khóa duy nhất, tổng món
cấp product khớp snapshot, index có 21 department và 134 aisle, và holdout chia
`val`/`test` lệch dưới 5%.

**Ghi vào báo cáo** (chạy tay, không chặn):

| Số liệu | Truy vấn | Kỳ vọng |
|---|---|---|
| Số giỏ `prior` / `holdout` | `ds_data.data_versions` | 3.214.874 / 131.209 (khớp checklist G3) |
| Số item cấp product | `count(*) FROM item_index WHERE grain = 'product'` | Khoảng 49.677 (sản phẩm có trong prior) |
| Số đơn holdout dùng được để đánh giá | `count(*) FROM holdout_eval` | Ít hơn 131.209 một chút (bỏ đơn 1 món) |
| Số món holdout bị bỏ vì không có trong prior | Đếm `stream_order_items` không join được `item_index` | Rất nhỏ; ghi con số thật |
| Số dòng file xuất | `_manifest.json` | Bằng `transactions` theo cấp × quy mô (task tự `assert`) |

- [ ] Task `check` xanh.
- [ ] `_manifest.json` có đủ 3 cấp × 4 quy mô.
- [ ] P4 (vai `ds`, không thuộc `de`) đọc được `ds_data.*` và volume `exports`; **không** ghi được (`INSERT` báo `PERMISSION_DENIED`). Không thử bằng P1 vì P1 thuộc `de`.
- [ ] Báo DS (P4) rằng `v1` đã sẵn sàng; ghi ngày tạo `v1` vào [ds/00 §5](../ds/00_tong_quan_ds.md).

### Nghiệm thu v1 trên dev

Ngày 26/11/2026, tạo `v1` trong `instacart.23521394_ad21_ds_data` từ `instacart.23521394_ad17_replay_silver`
đã đạt G3 ở [AD-DE-17](06c_nghiem_thu_reconciliation.md#nghiệm-thu-sau-replay-ad-st-07).
Cả ba task đều thành công ở hai lượt. [Lượt đầu](https://adb-7405615740735217.17.azuredatabricks.net/jobs/977787802741923/runs/447868960559557?o=7405615740735217) ·
[Lượt chạy lại](https://adb-7405615740735217.17.azuredatabricks.net/jobs/977787802741923/runs/971019481850633?o=7405615740735217)

| Số liệu | Kết quả dev |
|---|---:|
| Giỏ prior / holdout trong snapshot | 3.214.874 / 131.209 |
| Item cấp product / aisle / department | 49.677 / 134 / 21 |
| Holdout dùng được theo val / test | 62.232 / 62.132 |
| Dòng món holdout có sản phẩm chưa có trong prior | 9 |

Manifest tại `/Volumes/instacart/23521394_ad21_ds_data/exports/v1/_manifest.json` có đủ 12 số đếm:

| Cấp | 10% | 25% | 50% | 100% |
|---|---:|---:|---:|---:|
| product | 321.030 | 803.462 | 1.606.589 | 3.214.874 |
| aisle | 321.030 | 803.462 | 1.606.589 | 3.214.874 |
| department | 321.030 | 803.462 | 1.606.589 | 3.214.874 |

Hai lượt kiểm `01f1e99a-339c-1383-b5f7-1ebfd72adcc4` và `01f1e99c-de1e-1e82-86d5-fbb3d21bac8c` cho cùng số dòng và cùng tổng
`xxhash64` trên từng hàng nghiệp vụ của cả bảy bảng. Hai bản manifest trùng nội dung.
Đã kiểm quyền hiệu lực của Trung trên schema dev và volume để bàn giao cho AD-DS-11.
Bài thử bằng chính tài khoản DS đọc bảng, đọc file và bị từ chối `INSERT` được theo dõi
ở [issue #57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57).

### Kiểm trước khi tạo v1 trên prod Sub D

Ngày 28/11/2026, job `instacart-ds-prepare` đã có trên Sub D với ba task
`prepare_tables`, `export_txt` và `check`. Tham số mặc định đã được xác nhận là
`data_version=v1`, `catalog=instacart`, `silver_schema=silver` và `ds_schema=ds_data`.
Job chưa có lượt chạy trên Sub D. Schema `instacart.ds_data` chưa có bảng và volume
`exports` chưa có thư mục `v1`.

Truy vấn `instacart.ops.reconciliation` trên warehouse prod trả 22/22 kiểm tra Batch
và chưa có hàng G3 (statement `01f1eae8-a5f8-11d1-a2ab-e0a31b24cfe7`).
Silver prod chưa có hai bảng stream và hai view hợp nhất. Sau khi P1/P3 phục hồi
stream và prod hoàn tất batch, chọn một lượt [đối soát G3](06c_nghiem_thu_reconciliation.md)
đúng nguồn `instacart.silver`. Kiểm đủ 25/25 dòng xanh cùng `run_id`, `profile`
và `config_hash` trước khi chạy job DS. Kết quả G3 dev ở trên dùng để so sánh số liệu.

Chạy job prod một lần với bốn tham số mặc định. Ghi run ID, trạng thái ba task,
số giỏ prior/holdout và 12 số đếm từ `/Volumes/instacart/ds_data/exports/v1/_manifest.json`
vào [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57), rồi báo P4.
Nếu số liệu khác dev, ghi rõ chênh lệch và nguồn trước khi bàn giao.

## 8. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách chạy lại | Vì sao không trùng / không mất |
|---|---|---|
| `prepare_tables` lỗi trong hoặc giữa 01 → 03 | Repair run cùng phiên bản, catalog và schema ban đầu | Bước 01 giữ phần snapshot đã commit, bước 02 chỉ thay phần của phiên bản đang chạy, bước 03 giữ tập đánh giá đã tạo |
| `export_txt` lỗi ở một quy mô | Repair run | Ghi đè đường dẫn tất định; công bố manifest sau khi kiểm đủ 12 nhóm file |
| `check` báo lệch | Điều tra theo thông báo; nếu silver sai thì sửa silver rồi tạo **`v2`** | `v1` không bị sửa ngầm; DS biết rõ đang dùng phiên bản nào |
| Lỡ chạy job với `data_version` đã có | Không cần làm gì | Snapshot và holdout bị chặn ghi; index/transactions tính lại ra y hệt |
| File trên volume bị xóa nhầm | Chạy lại riêng task `export_txt` | Tạo lại từ `ds_data.transactions` |
