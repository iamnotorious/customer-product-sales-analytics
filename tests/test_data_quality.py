import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sales_analytics.validation import (
    check_duplicates,
    generate_data_quality_report
)



def test_check_duplicates_none(spark):
    """Test duplicate check with no duplicates."""
    data = [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = check_duplicates(df, ["id"])
    assert result["duplicate_count"] == 0
    assert result["total_count"] == 3

def test_check_duplicates_present(spark):
    """Test duplicate check with duplicates."""
    data = [("1", "Alice"), ("1", "Bob"), ("2", "Charlie")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = check_duplicates(df, ["id"])
    assert result["duplicate_count"] == 1
    assert result["total_count"] == 3



def test_generate_data_quality_report(spark):
    """Test data quality report generation."""
    data = [("1", "Alice", 100.0), ("2", None, 200.0), ("3", "Bob", None)]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    report = generate_data_quality_report(df, "TestDataset")
    
    assert report["dataset_name"] == "TestDataset"
    assert report["row_count"] == 3
    assert report["column_count"] == 3
    assert report["null_counts"]["name"] == 1
    assert report["null_counts"]["amount"] == 1
