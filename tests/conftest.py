import pytest
import sys
import os
from pyspark.sql import SparkSession

# Add src to path for test discovery
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

@pytest.fixture(scope="session")
def spark():
    """
    Creates a SparkSession for testing.
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("sales_ecommerce_analytics_ingestion_utils") \
        .getOrCreate()
    yield spark
    spark.stop()
