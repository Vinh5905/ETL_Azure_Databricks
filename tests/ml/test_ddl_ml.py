"""Chạy ddl_ml.sql trên Delta local để bắt lỗi cú pháp và kiểm cột của bảng hàng đợi/kết quả."""

from pathlib import Path

from ml.plan_grid import RUN_KEY_FIELDS

DDL = Path(__file__).parents[2] / "src/ml/sql/ddl_ml.sql"


def test_ddl_creates_ml_tables_and_is_rerunnable(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_ddl_test")
    sql = DDL.read_text(encoding="utf-8").replace("instacart.ml.", "ml_ddl_test.")
    statements = [s for s in sql.split(";") if "CREATE TABLE" in s]
    assert len(statements) == 6
    try:
        for _ in range(2):  # chạy lại an toàn
            for statement in statements:
                spark.sql(statement)
        queue = set(spark.table("ml_ddl_test.experiment_queue").columns)
        runs = set(spark.table("ml_ddl_test.experiment_runs").columns)
        assert set(RUN_KEY_FIELDS) <= queue and set(RUN_KEY_FIELDS) <= runs
        assert {"status", "wall_s", "rules_s", "job_s", "machine", "node_type", "spark_version"} <= runs
        assert dict(spark.table("ml_ddl_test.experiment_runs").dtypes)["machine"] == "string"
    finally:
        spark.sql("DROP DATABASE IF EXISTS ml_ddl_test CASCADE")
