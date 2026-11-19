from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Đảm bảo Python worker và driver dùng cùng interpreter trong kiểm thử Windows.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("instacart-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.jars.ivySettings", (Path(__file__).parent / "fixtures/ivysettings.xml").resolve().as_uri())
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
