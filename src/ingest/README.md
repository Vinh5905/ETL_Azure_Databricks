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
- Trước wave 1, công bố snapshot giá sau wave 0. Seed kiểm nhật ký theo chế độ nguồn và yêu cầu
  snapshot có đủ số dòng của danh mục, hoàn tất sau khi wave 0 đã áp dụng. Chạy ingest giữa các wave sau
  để giữ từng phiên bản giá và khuyến mãi.
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
Chạy `pl_ingest_retail_db` để công bố snapshot wave 0 trước khi chạy wave 1. Với nhánh dự phòng,
dùng pipeline timestamp tương ứng. Xem [runbook phục hồi giá gốc](../../docs/de/01_ingest_landing.md#khôi-phục-giá-gốc-issue-215).
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

## OFF crawler (AD-DE-10)

Job DAB `off_crawler` (`instacart-off-crawler`) chạy hai task `off_crawler → off_images`
trên **cùng** classic single-node theo policy của repo/G0: DBR 15.4, 0 worker, SINGLE_USER.
Không schedule, queue tắt, `max_concurrent_runs=1`, không tự retry task, timeout job 4 giờ.
Không chạy cùng seed, pipeline batch, stream hay DS đang chiếm quota 4 vCPU. `requests` pin theo lockfile.

### Cấu hình và quyền

| Phần | Dev | Prod |
|---|---|---|
| Nguồn mặc định | `<catalog>.<user>_bronze.products/order_products_prior` | `<catalog>.bronze.products/order_products_prior` |
| Bảng trạng thái | `<catalog>.<user>_ops.off_crawl_state` | `<catalog>.landing_meta.off_crawl_state` |
| `off_root` | `/Volumes/<catalog>/landing_meta/raw/dev/<user>/off_api` | `/Volumes/<catalog>/landing_meta/raw/off_api` |

Bundle truyền cùng `off_root` cho crawler và bronze; chỉ nhánh OFF của dev được tách, nguồn CSV/CDC
vẫn ở landing raw chung. Pipeline cũ chưa có cấu hình này dùng fallback `landing_root/off_api`.
Không chuyển đường đọc của một bảng OFF đã có checkpoint mà chưa có kế hoạch chuyển dữ liệu/pipeline riêng.

P1 phải chuẩn bị catalog, volume raw, policy, các schema và quyền UC. Người chạy cần USE CATALOG/SCHEMA,
SELECT trên hai bảng nguồn, READ/WRITE VOLUME, CREATE TABLE trên schema trạng thái và MODIFY/SELECT
trên bảng trạng thái đã tồn tại. Code chỉ tạo bảng Delta nếu chưa có; không tạo schema hay tự cấp grant.
Bảng mới gồm `product_id,status,attempts,off_code,batch_file,last_error,updated_at,batch_id`;
nếu đã dựng bảng theo bản minh họa cũ không có `batch_id`, cần migration được review trước khi chạy.

### Chọn lô và gọi API

- Mặc định đọc **bronze**, đếm tần suất từ món **prior**, chọn top 2.000, tie-break product_id.
  Mỗi lượt lấy tối đa 300 sản phẩm chưa có trạng thái hoặc `retry` còn dưới 5 lần thử.
  Không tra lại `done`, `no_result`, `failed`; không lấy train/test làm tín hiệu phổ biến.
- `attempts` đếm lần xử lý sản phẩm trong lô, không phải từng request HTTP. Mỗi lần xử lý có
  tối đa 5 HTTP request cho timeout/kết nối/429/5xx. Sau 5 lần xử lý vẫn lỗi thì `failed`;
  HTTP 3xx/4xx khác 429 không retry; riêng 401/403 dừng cả lô và giữ plan để xử lý truy cập,
  không đánh failed toàn bộ danh mục. Phản hồi sai cấu trúc/timeout không bị coi là no_result.
- Endpoint Search-a-licious dùng `q`, `fields`, `page_size=5`, `langs=en`; tên hàng được escape
  để không bị diễn giải thành toán tử Lucene. User-Agent có URL repo. Mọi search request, kể cả retry, cách nhau ít nhất
  6,5 giây; Retry-After dạng giây/HTTP date được tôn trọng, fallback backoff tăng dần.
- Cả job/API chỉ có một writer cho mỗi bộ state/off_root. Không chạy thêm script thủ công hay
  job dev/prod đồng thời dùng chung nguồn/IP. Giới hạn trong client không phải distributed rate limiter.

### Landing và phục hồi

1. Ghi active plan ở `off_root/_staging/off/active.json`, lưu danh sách sản phẩm/tên tra/attempt hiện tại.
2. Sau mỗi kết quả API, lưu journal riêng từng sản phẩm. Restart đọc journal trước, chỉ tra phần chưa có.
   Nếu process chết ngay sau HTTP nhưng trước khi journal được ghi, request đó có thể lặp lại.
3. Dựng JSONL, metadata `_product_id`/`_search_query` do crawler gắn; giữ code dạng chuỗi và payload lồng nhau.
   Batch ID hash từ nội dung plan, gồm attempt, không từ ngày/run ID; tên file còn có hash JSONL:
   `products/batch=<plan_hash>/batch_<content_hash>.json`.
4. Ghi `_staging` rồi rename trong cùng volume, không fallback copy. File đã công bố phải trùng byte
   nếu gặp lại; khác nội dung thì dừng. Chỉ sau publish mới MERGE trạng thái với attempts **tuyệt đối**.
   MERGE không lùi trạng thái terminal và không cộng thêm attempts khi replay cùng plan.
5. Sau MERGE thành công mới xóa active pointer; giữ journal để điều tra. Lỗi publish/MERGE thì chạy lại
   cùng job/configuration để phục hồi plan cũ, không tạo lô mới/đổi ngày/tra lại phần đã có journal.

Một lô toàn no_result/retry/failed vẫn cập nhật trạng thái nhưng **không** công bố JSONL rỗng.
Summary cuối task có số done/no_result/retry/failed; hoàn tất job không có nghĩa đủ 300 terminal.
Hit lỗi được bỏ riêng và đếm trong `invalid_hits`. Ảnh lỗi vĩnh viễn được ghi vào `_staging/images/failed.json` theo mã và hash URL. Lỗi quyền dịch vụ hoặc hết retry vẫn dừng task.

Không xóa journal/reset bảng để ép tra lại sản phẩm terminal. Giữ nguyên state, off_root, logic chọn nguồn
trong lúc phục hồi active plan; nếu cần chuyển cấu hình phải xử lý plan cũ xong trước.

`Path.replace/rename` cần được nghiệm thu thực tế trên UC Volume Azure; test local chỉ xác nhận hành vi
trên filesystem local. Nếu backend không hỗ trợ rename thì task dừng, giữ staging, không thay bằng
`dbutils.fs.mv`: lệnh đó copy rồi xóa, không bảo đảm publish nguyên tử. Dừng ingestion trong lúc kiểm
publish; không dùng một update pipeline song song làm bằng chứng atomicity của source chưa nghiệm thu.

### Ảnh

`off_images` đọc JSONL đã công bố từ `batch_file` của các dòng done, kiểm hash, lấy mọi image_url
và loại trùng mã vạch. Không cần `bronze.off_products`, nên ảnh có thể được chuẩn bị trước update LDP.
Chỉ nhận HTTPS trên images.openfoodfacts.org, không follow redirect. CDN ảnh giãn 1 giây/request,
retry vẫn qua limiter; search có limiter riêng. JPEG tối đa 10 MiB, kiểm Content-Type và marker đầu/cuối.
Ảnh hoàn chỉnh công bố thành `images/<code>.jpg` qua staging/rename, không chuyển byte ảnh vào JSONL.

Ảnh đã tồn tại được kiểm marker và bỏ qua mà không gửi HTTP request. Ảnh lỗi vĩnh viễn hoặc sai
định dạng được ghi vào journal để lượt sau bỏ qua cùng URL. Lỗi quyền dịch vụ hoặc hết retry làm
task lỗi. Repair tiếp tục các ảnh chưa xử lý. Giữ nguyên nội dung và tên ảnh đã công bố.

### Bootstrap và nghiệm thu trên workspace

Sau khi có CLI và profile **người dùng đã chọn**, chạy từ `bundles/`:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> off_crawler
```

Nếu bronze chưa được khởi tạo vì OFF còn rỗng, có thể bootstrap có chủ đích từ CSV landing đã công bố:

```bash
databricks bundle run -t dev --profile <profile> off_crawler --params source=landing,batch_size=300,top=2000
```

Chế độ này vẫn chỉ đọc products và món prior với schema cố định; không ghi bảng bronze thay LDP.
Sau khi OFF/ảnh có nguồn, chạy `instacart_batch` theo AD-DE-09; những nguồn CDC khác cũng phải sẵn sàng.
Từ lượt sau dùng mặc định source=bronze. Không tự bootstrap bằng fixture hoặc dữ liệu nghiệp vụ giả trên prod.

Nghiệm thu issue #46 cần: ít nhất **300 done/no_result**, link job run/state dev đã resolve, JSONL/ảnh đã
công bố, số request lượt chạy lại cho terminal bằng 0 và ảnh đã có không tải lại, số dòng bronze OFF/ảnh
khác 0 sau update pipeline. Tách kiểm repair từng task khỏi chạy lô mới: nếu vẫn còn sản phẩm trong top,
chạy job mới sẽ chọn sản phẩm **khác**, không nhất thiết là no-op toàn job.

Lô dev đã xác nhận MERGE Delta và rename trên UC Volume Azure với 5 sản phẩm, 1 JSONL và 17 ảnh.
Pipeline batch mặc định `enable_off=false` để chạy được trước lượt crawl đầu. Khi dev có JSONL và
nhánh ảnh, chạy từ `bundles/`:

```bash
BUNDLE_VAR_enable_off=true databricks bundle deploy -t dev
BUNDLE_VAR_enable_off=true databricks bundle run -t dev instacart_batch
```

Trên prod, sau lượt crawl đầu tiên tạo dữ liệu, mở PR đặt `enable_off: "true"` trong
`targets.prod.variables` của [bundles/databricks.yml](../../bundles/databricks.yml).
CI triển khai cấu hình prod sau khi PR được duyệt và merge.

Đối soát số dòng OFF và ảnh theo update. Log crawler có `search_requests`, gồm các lần retry.
Log ảnh có `image_requests` và `downloaded`. Kiểm chạy lại với cùng giới hạn top đã nghiệm thu.

Kiểm local: `uv run pytest tests/test_off_crawler.py tests/test_off_state.py tests/test_bronze.py -q`.
Tests dùng HTTP giả lập/filesystem local/Spark thật cho chọn lô/bootstrap; SQL MERGE chỉ được parse và
kiểm cleanup sau lỗi, chưa chứng minh transaction/concurrency Delta trên workspace.

Tham khảo: [API search](https://openfoodfacts.github.io/search-a-licious/users/ref-openapi/),
[giới hạn API](https://openfoodfacts.github.io/openfoodfacts-server/api/),
[dbutils mv](https://docs.databricks.com/aws/en/dev-tools/databricks-utils#mv-command-dbutilsfsmv).
