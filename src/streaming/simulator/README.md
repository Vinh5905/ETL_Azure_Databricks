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
- Gửi Kafka tới Event Hubs thuộc AD-ST-02. Xem [thiết kế streaming](../../../docs/de/04_streaming.md#2-simulator).

```bash
uv run pytest tests/test_simulator.py -q
```
