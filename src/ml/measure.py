"""Đo bộ nhớ tăng thêm và giới hạn thời gian cho job Spark."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

import psutil

_MIB = 1024 * 1024


class PeakRss:
    """Lấy mẫu RSS của JVM Spark và worker PySpark trên máy chạy job."""

    def __init__(self, every_s: float = 0.5) -> None:
        if every_s <= 0:
            raise ValueError("every_s phải lớn hơn 0")
        self.every_s = every_s
        self.baseline = 0.0
        self.peak = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _sample() -> float:
        total = 0
        for process in psutil.process_iter(["name", "cmdline", "memory_info"]):
            try:
                info = process.info
                name = (info.get("name") or "").lower()
                command = " ".join(info.get("cmdline") or []).lower()
                is_spark_jvm = name in {"java", "java.exe"} and "spark" in command
                is_pyspark_worker = "pyspark" in command
                if is_spark_jvm or is_pyspark_worker:
                    memory = info.get("memory_info")
                    if memory is not None:
                        total += memory.rss
            except (psutil.Error, OSError):
                continue
        return total / _MIB

    def _collect(self) -> None:
        while not self._stop.wait(self.every_s):
            self.peak = max(self.peak, self._sample())

    def __enter__(self) -> PeakRss:
        self.baseline = self._sample()
        self.peak = self.baseline
        self._stop.clear()
        self._thread = threading.Thread(target=self._collect, name="peak-rss", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    @property
    def delta_mb(self) -> float:
        """RSS đỉnh tăng thêm so với baseline, tính theo MiB."""
        return round(max(self.peak - self.baseline, 0.0), 1)


@contextmanager
def time_limit(sc, minutes: float, group: str) -> Iterator[threading.Event]:
    """Hủy job Spark thuộc group khi hết thời gian và trả cờ expired cho caller."""
    if minutes <= 0:
        raise ValueError("minutes phải lớn hơn 0")
    if not group:
        raise ValueError("group không được rỗng")

    sc.setJobGroup(group, group, interruptOnCancel=True)
    expired = threading.Event()

    def cancel() -> None:
        expired.set()
        sc.cancelJobGroup(group)

    timer = threading.Timer(minutes * 60, cancel)
    timer.daemon = True
    timer.start()
    try:
        yield expired
    finally:
        timer.cancel()
        # PySpark không có clearJobGroup: gỡ các thuộc tính setJobGroup đã đặt để job sau không mang group cũ.
        for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
            sc.setLocalProperty(key, None)
