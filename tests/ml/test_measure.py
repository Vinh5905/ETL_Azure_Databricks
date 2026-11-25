from __future__ import annotations

import time

import pytest

from ml.measure import PeakRss, time_limit


def test_peak_rss_delta_is_nonnegative(spark):
    with PeakRss(every_s=0.01) as measurement:
        spark.sparkContext.parallelize(range(100), 1).count()
        time.sleep(0.03)

    assert measurement.delta_mb >= 0


def test_time_limit_cancels_spark_job_and_sets_expired(spark):
    sc = spark.sparkContext
    expired = None

    with pytest.raises(Exception, match="(?i)cancel"):
        with time_limit(sc, minutes=0.005, group="test-time-limit") as expired_flag:
            expired = expired_flag
            sc.parallelize(range(100), 1).map(_slow_value).count()

    assert expired is not None
    assert expired.is_set()
    assert sc.getLocalProperty("spark.jobGroup.id") is None


def test_time_limit_rejects_nonpositive_timeout(spark):
    with pytest.raises(ValueError, match="minutes"):
        with time_limit(spark.sparkContext, minutes=0, group="empty"):
            pass


def test_peak_rss_rejects_nonpositive_sampling_interval():
    with pytest.raises(ValueError, match="every_s"):
        PeakRss(every_s=0)


def _slow_value(value: int) -> int:
    time.sleep(0.1)
    return value
