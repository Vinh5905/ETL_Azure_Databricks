# Nghiệm thu đối soát — AD-DE-17

## Phạm vi và điều kiện

SQL trong `src/tasks/` được tách để giai đoạn sớm không tham chiếu bảng chưa tồn tại:

| File | Nội dung |
|---|---|
| `init_reconciliation.sql` | Tạo bảng Delta thường expected_counts/reconciliation, seed số tham chiếu full nếu chưa có |
| `reconcile.sql` | 22 check batch: sáu bronze CSV, split, clock, header/món silver, khóa, payload và orphan |
| `reconcile_g3.sql` | 25 check G3: dim_product, SCD2 mở, stream, view hợp nhất, baskets |
| `reconcile_g4.sql` | 3 check gold fact: số dòng, không fanout và cặp khóa khớp silver |

Mỗi file chạy nguyên trong cùng phiên SQL, dừng khi bất kỳ assert/statement lỗi.
G3 kiểm 22 check batch đã xanh cùng run/cấu hình trước khi đọc nguồn G3; G4 yêu cầu cả 47 check trước.
Không dùng điều kiện WHERE=false để giả tắt nhánh tham chiếu bảng thiếu.

Nghiệm thu G3 cần AD-DE-14 (#50), AD-DE-15 (#51), AD-ST-07 (#70) và hạ tầng/quyền/warehouse sẵn sàng.
Code trên nhánh dựa #163 có Delta local; dim/baskets/stream trong test là fixture theo hợp đồng các PR
nguồn, không phải kết quả pipeline thật. G4 chỉ chạy sau AD-DE-22 có `gold.fact_order_lines`.

## Cấu hình kỳ vọng độc lập nguồn

Profile chỉ đối soát sáu CSV Instacart. Kiểm tra Azure SQL retail và cửa sổ CDC theo AD-DE-07.
Khi cần đối chiếu replay, chạy `COUNT(*)` trên bảng bronze để xem số dòng thô bên cạnh số khóa
phân biệt. Chỉ số khóa phân biệt và payload đã chọn theo khóa dùng để chặn cổng.

1. Chạy `init_reconciliation.sql` trên warehouse **serverless** `wh-instacart-bi` với tham số
   `catalog` và `ops_schema` dev đã resolve. Schema phải tồn tại; file không cấp grant/tạo schema.
2. Profile `instacart_2017` seed **số tham chiếu chưa xác minh trên Zenodo trong phiên này**:
   orders 3.421.083; prior items 32.434.489; train items 1.384.617; products 49.688;
   aisles 134; departments 21; header prior/train/test 3.214.874 / 131.209 / 75.000.
   Init chỉ INSERT khóa thiếu, không ghi đè số đã cấu hình. Kiểm checksum/manifest nguồn trước dùng.
3. Với mẫu AD-DE-01 hoặc nguồn đầy đủ đã tạo manifest, từ gốc repo:

```bash
python3 scripts/expected_counts_sql.py --manifest data/sample_1pct/_manifest.json \
  --catalog instacart --ops-schema p2_ops --profile sample_1pct > /tmp/expected-counts.sql
```

Chạy SQL vừa in trên warehouse. Script đọc **manifest nguồn**, không đọc bảng bronze/silver, không kết nối
cloud. Nó kiểm chín số nguyên không âm, tổng ba split bằng orders và nguồn không rỗng; chỉ xuất số tổng hợp
và SHA-256 manifest, không đưa user_ids/đường dẫn vào SQL. Không suy số đếm bằng cách nhân tỉ lệ 1% vì danh mục
được giữ toàn bộ. Không lấy số actual rồi chép vào expected để làm check xanh.

`expected_counts` có profile/metric/expected/source/configured_at, một khóa mỗi profile/metric.
Đối soát yêu cầu đúng chín metric, không NULL/âm/trùng. Hai nguồn chuẩn hóa tên bảng món từ
`order_products__prior.csv` / `order_products__train.csv` sang metric một underscore theo tên bronze.

## Chạy theo cổng

Các file đối soát nhận tám tham số STRING bắt buộc, không bọc tên riêng bằng backtick:

| Tham số | Ví dụ | Ý nghĩa |
|---|---|---|
| catalog | instacart | Catalog đã resolve |
| ops_schema | p2_ops | Bảng cấu hình/kết quả |
| bronze_schema | p2_bronze | Nguồn batch |
| silver_schema | p2_silver | Target batch/stream/MV/view |
| gold_schema | p2_gold | Namespace dự kiến G4; batch/G3 không đọc bảng gold |
| profile | sample_1pct | Profile có manifest/số kỳ vọng tương ứng nguồn đang kiểm |
| run_id | Một UUID manual hoặc job run ID | Cùng giá trị cho toàn bộ sequence và repair của lần này |
| check_date | 2026-03-30 | Ngày ISO cố định cho sequence, không lấy lại ngày khi qua nửa đêm |

Sau pipeline nguồn hoàn tất và đứng yên trong lúc kiểm, chạy lần lượt:

1. `reconcile.sql`: có thể chạy trước khi bảng dim/stream/gold tồn tại.
2. `reconcile_g3.sql`: chỉ sau khi stream phát đủ và dim/view/baskets sẵn sàng; không dùng G3 partial làm đầu vào DS.
3. `reconcile_g4.sql`: chỉ khi gold đã triển khai. Chưa có gold thì không chạy file này, không ghi check G4 xanh giả.

MERGE theo `(check_date, check_name)` giữ kết quả **mới nhất trong ngày** của namespace ops.
Run_id/profile/stage/config_hash kèm mỗi hàng ngăn lấy kết quả xanh cũ làm prerequisite.
Hash gồm chín kỳ vọng, provenance và catalog/cả bốn schema; đổi nguồn namespace, profile hoặc manifest
cần chạy lại từ batch. Một ops namespace chỉ đại diện một dataset/profile hiện hành; mẫu và prod dùng
schema riêng, không chạy đồng thời nhiều sequence ghi cùng namespace.

MERGE ghi cả hàng sai rồi **assert_true cuối file làm task fail**; kết quả sai vẫn tồn tại cho điều tra/BI.
Assert yêu cầu đủ inventory (22/47/50), mọi check passed và cùng run/profile/hash, tránh trường hợp
không có dòng hoặc chỉ có kết quả cũ mà vẫn xanh. AD-DE-18 phải truyền tham số thống nhất và chỉ cho
refresh_bi chạy khi stage yêu cầu thành công; PR này chưa khai báo job điều phối.

## Kiểm lỗi, repair và bằng chứng

- Chạy cùng stage hai lần trong ngày: số khóa không đổi; checked_at mới hơn được phép, đó là thời điểm kiểm.
- Chỉ ở dev: đổi tạm expected products trong profile, chạy batch. Phải có hàng failed và task đỏ.
  Khôi phục expected đúng nguồn rồi repair sequence, hàng đó cập nhật về passed, không thêm khóa mới.
- Kiểm một lỗi có cùng count: payload món bị đổi hoặc một item giỏ sai. FULL OUTER JOIN theo khóa và so null-safe phải bắt được,
  không chỉ dựa vào count/distinct. Các check stream đối chiếu header/train payload với bronze train;
  basket đối chiếu header/split và explode(items) với các target món.
- Không sửa bảng do pipeline quản lý trên cloud để diễn tập. Dùng fixture local hoặc nguồn dev hợp lệ rồi
  refresh pipeline theo runbook; không xóa checkpoint/full refresh bronze hay sửa expected để che dữ liệu sai.
- Nếu cấu hình/hash/run sai hoặc bảng/field thiếu, dừng trước ghi check downstream; kiểm schema/input.
  Nếu bảng reconciliation cũ chỉ có sáu cột, cần P1 audit/migration trước dùng; script không DROP/overwrite lịch sử.

Đọc kết quả bằng query ở DE/06 §4, lọc đúng check_date/run_id/profile; không chỉ đếm tất cả hàng xanh trong ngày.
Ghi issue #53: commit, params/manifest hash, nguồn và pipeline/update ID, stage đã chạy, số check/failed,
count/distinct thật, lần chạy lại, thử sai expectation và repair. Không dùng dữ liệu fixture làm bằng chứng
3.421.083 orders / 49.688 products / 131.209 stream hoặc đạt G3 thật.

## Trạng thái hiện tại

Ngày 2026-11-19, chạy trên warehouse dev với profile `instacart_2017` và schema ops cách ly. Nguồn là snapshot Bronze/Silver sau [update batch](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/7d05cb18-cfe9-46e8-9159-812a5ebffe6c).

| Lượt | Statement ID | Thời gian | Kết quả |
|---|---|---:|---|
| Lần một | `01f1e40a-5a63-1d58-abd6-ff69466eecac` | 53,36 giây | MERGE ghi 22 check xanh |
| Lần hai | `01f1e40a-859b-14e5-81f2-d2395fbfb7f4` | 43,85 giây | MERGE vẫn có 22 khóa, tất cả xanh |
| Thử sai | `01f1e40a-c9f6-1ba3-b693-18dde291364d` | 4,62 giây | Assert thất bại. Đã lưu `bronze.products` với `passed=false` (49.689 kỳ vọng, 49.688 thực tế) |
| Sau khi khôi phục | `01f1e40a-d7ff-1cec-b9fe-f4f074b4ed4e` | 42,82 giây | MERGE khôi phục 22 check xanh sau khi trả kỳ vọng về 49.688 |

Thời gian là từ khi gửi từng statement đến khi nhận trạng thái cuối. Hai lần chạy dùng cùng ngày và run ID.
Cổng G3/G4 còn chờ dữ liệu AD-DE-14, AD-DE-15, AD-ST-07 và bảng fact Gold. G4 dùng FULL OUTER JOIN
trên số dòng của từng cặp khóa để kiểm cả khóa bị thiếu lẫn số lần xuất hiện bị lệch.
