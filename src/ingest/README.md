# Seed retail_db · AD-DE-05

`seed_retail_db.py` sinh dữ liệu **giả lập** theo [DE/01 §3.4](../../docs/de/01_ingest_landing.md).
Đây là job riêng chạy tay, không chạy trong pipeline medallion, không có lịch tự động.

## Điều kiện

- AD-DE-04 (#40, PR #140) đã nghiệm thu schema nguồn và CDC trên `retail_db`.
- AD-DE-06 (#42, PR #144): landing CSV danh mục theo đường dẫn mặc định. Nếu truyền `products_csv` khác,
  người vận hành vẫn phải cấp CSV thật và quyền đọc trước khi nghiệm thu.
- Policy `instacart-single-node` (#23) và secret scope `kv` (#24) đã được triển khai.
  Người chạy job cần quyền dùng policy, đọc volume và đọc key `sql-admin-password` qua scope.
- CSV `products.csv` đủ **49.688** sản phẩm, `product_id` duy nhất và `department_id` hợp lệ.
  Chưa có ADF ingest thì đưa CSV thật vào volume trước, hoặc truyền đường dẫn CSV thật khác đã cấp quyền.
- Không có ADF đang đọc nguồn. Mỗi wave nằm trong một transaction; không chạy nhiều wave song song.
  Khóa SQL chỉ điều phối các tiến trình seed, không khóa pipeline ADF.

## Kế hoạch tất định

`load_products()` đọc hai cột CSV, sắp theo khóa và từ chối danh mục rỗng/trùng khóa/sai kiểu.
`generate_wave(products, wave, seed=42)` là hàm thuần, không kết nối SQL/Spark, không ghi file:

| Wave | Giá | Khuyến mãi | updated_at |
|---|---|---|---|
| 0 | Insert đủ 49.688 giá, USD, theo khoảng giá từng ngành hàng | 3.000 chương trình trên 3.000 sản phẩm khác nhau | 2015-01-01 |
| 1–4 | UPDATE 2.484 sản phẩm (floor 5%) bằng hệ số 0,85 / 0,90 / 1,05 / 1,10 | Thêm 300, gia hạn 50 thêm 7 ngày, hủy 30; các tập gia hạn/hủy không giao nhau | 01/03, 08/03, 15/03, 22/03/2026 |

Khuyến mãi mới có loại `percent_off` hoặc `bogo`, thời hạn ban đầu 3–14 ngày; giá luôn là Decimal,
làm tròn hai chữ số bằng SQL khi đổi giá. Đầu ra giữ nguyên với cùng danh mục/seed/wave, kể cả CSV đảo thứ tự.
Mã khuyến mãi không tái dùng. Các đợt sau chỉ chọn gia hạn/hủy những chương trình chưa hủy của các đợt trước.

`apply_wave(connection, plan, source_mode)` dùng connection riêng `autocommit=False`:

- Lấy application lock theo transaction trước khi kiểm/tạo `etl.seed_wave` và ghi nguồn.
- Bắt buộc chạy tuần tự 0 → 1 → 2 → 3 → 4. Wave 0 cần hai bảng nguồn trắng, không tự xóa dữ liệu cũ.
- Ghi marker chứa wave, seed, hash danh mục/kế hoạch, chế độ nguồn và giờ áp dụng UTC.
- DML theo lô tối đa 500 câu gửi SQL, **không commit từng lô**. Marker và toàn bộ DML cùng commit;
  lỗi ở giữa rollback tất cả. Không dùng bulk-copy tự commit theo batch.
- Retry cùng đầu vào in `wave N đã áp dụng, bỏ qua`, kể cả retry wave cũ sau wave mới hơn.
  Fingerprint của từng wave trước cũng phải khớp kế hoạch được tái sinh bằng thuật toán hiện tại.
  Đổi seed/danh mục/kế hoạch/chế độ khi đã có marker bị từ chối; không tự reset để seed lại.
- CDC hủy bằng DELETE thật; `soft-delete` giữ tombstone và cập nhật `is_deleted = 1, updated_at = thời gian wave`.
  Không trộn hai chế độ trong cùng lịch sử seed.

Nếu sửa thuật toán sinh dữ liệu sau khi đã seed, kiểm tra fingerprint trước; không xóa marker để ép chạy lại.
Database thử nghiệm mới là cách tái lập từ đầu. Không sửa tay dữ liệu nguồn giữa các wave.

## Chạy thử không cần SQL, secret hay Spark

Từ gốc repo, với dữ liệu thật đã giải nén:

```bash
uv run python src/ingest/seed_retail_db.py --wave 0 --products-csv data/instacart/products.csv --dry-run
```

`--dry-run` chỉ in số lượng và hash kế hoạch. Mặc định từ chối danh mục khác 49.688 dòng;
fixture nhỏ phải truyền `--expected-products <số dòng>` tường minh và chỉ dùng kiểm local.
Không cung cấp mật khẩu qua CLI, file hoặc biến môi trường cho đường chạy thật.

## Job dev

`bundles/resources/jobs_ingest.yml` khai báo resource `seed_retail_db`, tên job `seed-retail-db`.
Dev nhận tiền tố của DAB; **tên job dev/prod không tự tách database SQL**.
Chọn `server`/`database` đúng nguồn được cấp; dùng database thử riêng nếu cần thử lại từ đầu.

Theo chốt G0 #146 và `bundles/AGENTS.md`, job dùng classic single-node qua `${var.policy_id}`,
khai đủ các trường fixed của policy: DBR 15.4 LTS, `${var.node_type_id}` (mặc định Standard_D4ds_v4),
`num_workers: 0`, `SINGLE_USER`, Spark singleNode/local[*], tags cost_owner p2/ResourceClass SingleNode.
**Không đặt `apply_policy_default_values` hoặc `autotermination_minutes`** vì job cluster không nhận
thuộc tính auto-termination của policy; Databricks dừng cluster khi job kết thúc. Policy vẫn kiểm các giá trị
fixed nên đây không phải bỏ qua policy. Không mở pool hoặc chọn VM ngoài cấu hình được P1 chốt.
Job tối đa một run, timeout 30 phút, không tự retry (`max_retries: 0`); sửa cấu hình trước khi chạy tay lại.
Thư viện `pymssql==2.4.6` khớp lockfile; kết nối yêu cầu TLS, `datetime2` và không autocommit.

Sau khi hạ tầng, CLI và profile đã được cấu hình, chạy trong `bundles/`:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run seed_retail_db -t dev --profile <profile> --params "wave=0,server=$RETAIL_SQL_SERVER,database=retail_db"
```

`RETAIL_SQL_SERVER` do người vận hành đặt trong shell từ output hạ tầng, không đưa giá trị thật vào Git.
**Xác thực theo chốt G0 #146:** server chính thức dùng SQL auth **cộng** Entra admin, không Entra-only
như server spike ban đầu. SQL login `sqladmin` và secret `sql-admin-password` đã được tạo.
Xác nhận job dev đọc được scope `kv` và kết nối SQL trước khi chạy wave 0.

Job mặc định để server rỗng để tránh kết nối nhầm; truyền thêm `user` nếu tài khoản khác `sqladmin`.
Đường dẫn CSV mặc định là `/Volumes/<catalog>/landing_meta/raw/instacart/products/release=2017/products.csv`;
tham số `products_csv` có thể trỏ CSV thật khác. Với nhánh dự phòng thêm `source_mode=soft-delete`.
Không đưa fixture vào tham số job để vượt kiểm đủ danh mục.

Sau wave 0 kiểm `COUNT(*) = 49688`, chạy lại và kiểm thông báo bỏ qua/marker không tăng.
Sau wave 1 chờ CDC capture bắt kịp rồi kiểm op 4 của giá và op 1 của khuyến mãi trong cửa sổ mới.
Chỉ cho ADF Lookup tiếp khi seed đã commit và CDC đã capture; không deploy prod bằng lệnh local.

## Kiểm tra local và CI

```bash
uv sync --frozen --extra ingest
uv run pytest tests/test_seed_retail_db.py -q
uv run --extra ingest python scripts/check_retail_sql.py --seed
```

- Pytest kiểm cả 5 kế hoạch, thứ tự input, seed, khóa/range/date/count, CSV, dry-run và hợp đồng đọc secret.
- Regression SQL dùng danh mục tự dựng **49.688 sản phẩm**, không phải danh mục thật:
  cả hai chế độ, đầy đủ 5 wave; số dòng; CDC thật của wave 1; retry/hash; ép lỗi sau UPDATE giá để kiểm
  rollback giá/khuyến mãi/marker; hai connection đồng thời chỉ áp dụng một lần.
- Runner giữ hai regression AD-DE-04; `--seed` thêm hai database và cổng **127.0.0.1 ngẫu nhiên tạm** để
  Python dùng driver thật. Mật khẩu local sinh trong bộ nhớ, truyền child qua stdin, không ghi file/log;
  container/cổng được dọn trong `finally`. Không kết nối Azure hoặc database có sẵn.
- Workflow `retail-sql-checks` cài extra ingest và chạy cùng lệnh. Pytest và regression SQL là hai nhóm riêng.
- Nghiệm thu job dev còn chờ CSV của #42 hoặc một bản danh mục thật đã được nhóm cấp trên volume.
  Chạy wave 0, chạy lại wave 0, rồi chạy wave 1 và đối soát CDC trên Azure.

Tham chiếu driver: [pymssql](https://pymssql.readthedocs.io/en/stable/ref/pymssql.html).
