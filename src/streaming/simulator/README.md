# Simulator đơn hàng Instacart

Simulator ghép đơn `train` trong `orders.csv` với các món trong `order_products__train.csv`.
Mỗi dòng JSON là một sự kiện `order_placed`. `event_id` cố định theo `order_id`,
còn `event_time` dùng cùng hàm tính giờ UTC với silver.

## Chạy thử tại máy

Chạy từ gốc repo bằng Python 3.11. Thư mục `--data-dir` cần chứa hai CSV nói trên.
Ví dụ dưới đây chạy ngay với fixture trong repo. Đổi thành `data/sample_1pct` để thử trên
mẫu 1% tạo bằng [make_sample.py](../../../scripts/make_sample.py).

**PowerShell**

```powershell
$env:PYTHONPATH = 'src'
uv run python -m streaming.simulator.simulate --data-dir tests/fixtures/instacart --run-id local-01 --dry-run data/local-01.jsonl
```

**Bash**

```bash
PYTHONPATH=src uv run python -m streaming.simulator.simulate --data-dir tests/fixtures/instacart --run-id local-01 --dry-run data/local-01.jsonl
```

- `--dry-run FILE` ghi JSON Lines và thay thế file đích khi ghi thành công. Chế độ này chạy tại máy bằng thư viện chuẩn Python.
- `--from-frac` và `--to-frac` chọn khoảng từ 0 đến 1 theo thứ tự `(event_time, order_id)`. Cận đầu được lấy và cận cuối được loại. Chạy phần đầu với `--to-frac 0.95` và phần còn lại với `--from-frac 0.95`.
- `--run-id` phân biệt từng lần phát lại. `--location` ghi nơi chạy simulator, mặc định là `laptop`. Trong dry-run, `produced_at` là giờ ghi sự kiện.

```bash
uv run pytest tests/test_simulator.py -q
```

## Gửi 1.000 sự kiện tới Event Hubs

Đợi AD-OPS-24 bàn giao Event Hub `orders-stream` ở Sub B cùng chuỗi kết nối SAS có quyền
`Send`. Chạy từ gốc repo. Ví dụ giả định `data/instacart` chứa hai CSV với ít nhất 1.000 đơn
train có món hàng. Đổi `--run-id` cho mỗi lần phát và dùng `--location` để ghi nơi chạy vào
`producer_location`.

**PowerShell trên laptop**

```powershell
$env:PYTHONPATH = 'src'
try {
    $env:EH_SEND_CONN = [System.Net.NetworkCredential]::new('', (Read-Host 'Chuỗi kết nối SAS quyền Send' -AsSecureString)).Password
    uv run --frozen --extra ingest python -m streaming.simulator.simulate --data-dir data/instacart --send --limit 1000 --run-id ad-st-02-laptop-01 --location laptop-hcm
} finally {
    Remove-Item Env:EH_SEND_CONN -ErrorAction SilentlyContinue
}
```

**Bash trên Codespace**

```bash
(
  set +x
  read -r -s -p 'Chuỗi kết nối SAS quyền Send: ' EH_SEND_CONN || exit
  printf '\n'
  export EH_SEND_CONN
  PYTHONPATH=src uv run --frozen --extra ingest python -m streaming.simulator.simulate --data-dir data/instacart --send --limit 1000 --run-id ad-st-02-codespace-01 --location codespace
)
```

1. Nhập SAS ở prompt ẩn. Simulator đọc biến môi trường `EH_SEND_CONN` và dùng nó để xác thực
   qua `SASL_SSL`. Mặc định gửi tối đa 200 sự kiện/giây. Dùng `--rate` để chỉnh tốc độ.
2. Kiểm tra kết quả dòng cuối có `events_sent=1000`. Đây là số xác nhận từ Kafka broker.
3. Trong Azure Portal, mở **Metrics** của namespace rồi chọn **Incoming Messages** và phép tổng
   hợp **Sum**. Lọc `EntityName = orders-stream` theo khoảng giờ UTC vừa gửi. Chạy khi chỉ có
   một producer và ghi số đếm, `run-id`, `location` cùng thời gian chạy làm bằng chứng.

Event Hubs Standard dùng mặc định `--compression none`. Tùy chọn `--compression gzip` dành
cho Premium và Dedicated. [Hỗ trợ nén Kafka của Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview#compression).

Kafka dùng `order_id` làm khóa. `enable.idempotence` xử lý retry trong cùng phiên producer.
Khi cần phát lại sau lỗi, dùng `--run-id` mới. Silver dùng khóa nghiệp vụ để xử lý các bản
ghi phát lại.
