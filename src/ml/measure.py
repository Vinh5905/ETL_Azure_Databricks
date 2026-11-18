"""Đo thời gian, RSS và giới hạn thời gian cho benchmark Spark."""

import threading
import time
from contextlib import contextmanager

import psutil


class PeakRss:
    """Lấy mẫu RSS tiến trình Spark/JVM và Python của worker trên node."""

    def __init__(self, every_s: float = 0.5):
        if every_s <= 0:
            raise ValueError("every_s phải > 0")
        self.every_s = every_s
        self.peak = 0.0
        self.baseline = 0.0
        self._stop = threading.Event()

    @staticmethod
    def _sample() -> float:
        total = 0
        for process in psutil.process_iter(["name", "cmdline", "memory_info"]):
            try:
                name = process.info["name"] or ""
                cmd = " ".join(process.info["cmdline"] or []).lower()
                if (name.lower() == "java" and "spark" in cmd) or "pyspark" in cmd:
                    info = process.info["memory_info"]
                    if info:
                        total += info.rss
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        return total / 2**20

    def _loop(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, self._sample())
            self._stop.wait(self.every_s)

    def __enter__(self):
        self.baseline = self._sample()
        self.peak = self.baseline
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._stop.set()
        self._thread.join()

    @property
    def delta_mb(self) -> float:
        return round(max(self.peak - self.baseline, 0.0), 1)


@contextmanager
def time_limit(sc, minutes: int, group: str):
    """Hủy Spark job trong group khi hết hạn; caller ghi nhận trạng thái timeout."""
    if minutes < 1:
        raise ValueError("minutes phải >= 1")
    sc.setJobGroup(group, group, interruptOnCancel=True)
    expired = threading.Event()

    def cancel():
        expired.set()
        sc.cancelJobGroup(group)

    timer = threading.Timer(minutes * 60, cancel)
    timer.daemon = True
    timer.start()
    try:
        yield expired
    finally:
        timer.cancel()


def timed_call(function, *args, **kwargs):
    """Chạy hàm và trả về (kết quả, thời lượng giây)."""
    started = time.perf_counter()
    result = function(*args, **kwargs)
    return result, time.perf_counter() - started
