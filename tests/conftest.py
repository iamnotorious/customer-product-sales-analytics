import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a SparkSession for testing.
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("sales_ecommerce_analytics") \
        .getOrCreate()
    yield spark
    spark.stop()
