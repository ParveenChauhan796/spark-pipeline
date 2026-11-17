import pytest

from catalog_pipeline.spark import get_spark


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One shared SparkSession for the whole test run (starting a JVM is slow)."""
    session = get_spark(
        overrides={
            "master": "local[2]",
            "shuffle_partitions": 2,
            "ui_enabled": False,
            "warehouse_dir": str(tmp_path_factory.mktemp("warehouse")),
            # Delta reads its transaction log with 50 tasks by default; 2 is plenty for tiny test tables.
            "extra_conf": {"spark.databricks.delta.snapshotPartitions": 2},
        }
    )
    yield session
    session.stop()
