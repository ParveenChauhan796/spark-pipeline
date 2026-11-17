"""SparkSession factory."""
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from catalog_pipeline.config import load_config
from catalog_pipeline.paths import resolve


def get_spark(config: dict | None = None, overrides: dict | None = None) -> SparkSession:
    """Return a local SparkSession with the Delta Lake extension and catalog enabled.

    A SparkSession is the entry point to Spark: every DataFrame, SQL query and
    read/write goes through it. In local mode the "cluster" is just this machine:
    the driver and the executors run as threads inside one JVM process, and the
    number in `local[N]` is how many tasks can run at once (`local[*]` = all cores).
    No cluster setup is needed, but it is a laptop-scale stand-in for the real thing.

    `overrides` replaces keys of the `spark` section of the config (tests use this
    to get local[2], 2 shuffle partitions, no UI and a temp warehouse). `extra_conf` is a dict of
    further Spark settings.
    """
    cfg = {**(config or load_config())["spark"], **(overrides or {})}

    # Python worker processes (used e.g. by createDataFrame on Python rows) must run the same
    # interpreter as the driver, not whichever python3 is first on PATH. Spark reads this
    # variable when it launches the JVM, so it has to be set before the session is created.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(cfg["app_name"])
        .master(cfg["master"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        .config("spark.ui.enabled", str(cfg["ui_enabled"]).lower())
        .config("spark.sql.warehouse.dir", str(resolve(cfg["warehouse_dir"])))
        # Delta: SQL extension (MERGE, DESCRIBE HISTORY) + catalog that understands Delta tables
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    for key, value in (cfg.get("extra_conf") or {}).items():  # any other Spark setting, e.g. from tests
        builder = builder.config(key, str(value))
    # Adds the matching Delta jar to spark.jars.packages; Spark downloads it from
    # Maven Central the first time a session starts (cached in ~/.ivy2 afterwards).
    return configure_spark_with_delta_pip(builder).getOrCreate()
