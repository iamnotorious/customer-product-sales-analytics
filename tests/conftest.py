import pytest
from pyspark.sql import SparkSession
from datetime import date

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.getActiveSession()
    if spark:
        yield spark
    else:
        spark = SparkSession.builder \
            .appName("SalesAnalytics_Tests") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        yield spark
        spark.stop()

# Sample data fixtures were removed as individual tests create their own data.
