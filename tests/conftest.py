import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks"))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.getActiveSession()
    if spark:
        # Enforce ANSI mode off for tests to prevent DateTimeExceptions on invalid input
        spark.conf.set("spark.sql.ansi.enabled", "false")
        yield spark
    else:
        spark = SparkSession.builder \
            .appName("SalesAnalytics_Tests") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.ansi.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()

# Sample data fixtures were removed as individual tests create their own data.
