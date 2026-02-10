import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from sales_analytics.transformation import clean_dataset, parse_date_col
from sales_analytics.aggregation import create_aggregates

def test_clean_dataset_empty_dataframe(spark):
    """Test clean_dataset with empty DataFrame."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    df = spark.createDataFrame([], schema)
    
    result = clean_dataset(df, clean_text_cols=["name"])
    assert result.count() == 0

def test_clean_dataset_all_nulls(spark):
    """Test clean_dataset with all null values."""
    data = [(None, None), (None, None)]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("country", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = clean_dataset(df, handle_null_cols=["country"], null_fill_value="Unknown")
    assert result.filter(result.country == "Unknown").count() == 2

def test_parse_date_col_invalid_dates(spark):
    """Test date parsing with invalid date strings."""
    data = [("invalid_date",), ("21/08/2016",)]
    schema = StructType([StructField("date_str", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    # Should handle gracefully - invalid dates become null
    result = parse_date_col(df, "date_str", "d/M/y", "date_parsed")
    assert "date_parsed" in result.columns

def test_create_aggregates_single_group(spark):
    """Test aggregation with single group."""
    data = [("2023", "A", 100.0), ("2023", "A", 200.0)]
    schema = StructType([
        StructField("year", StringType(), True),
        StructField("category", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = create_aggregates(df, ["year", "category"], "profit", "total")
    assert result.count() == 1
    assert result.first()["total"] == 300.0

def test_create_aggregates_with_negatives(spark):
    """Test aggregation with negative values."""
    data = [("2023", "A", -50.0), ("2023", "A", 150.0)]
    schema = StructType([
        StructField("year", StringType(), True),
        StructField("category", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = create_aggregates(df, ["year", "category"], "profit", "total")
    assert result.first()["total"] == 100.0

def test_clean_dataset_special_characters_only(spark):
    """Test cleaning text with only special characters."""
    data = [("!!!___###",), ("@@@$$$",), ("Normal Text",)]
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    result = clean_dataset(df, clean_text_cols=["text"])
    rows = result.collect()
    # Special chars should be removed, leaving empty or cleaned strings
    assert rows[2]["text"] == "Normal Text"
