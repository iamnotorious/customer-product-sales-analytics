import pytest
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col
from sales_ecommerce_analytics_ingestion_utils.transformation import clean_dataset, join_dataframes, parse_date_col

def test_clean_dataset(spark):
    """
    Test clean_dataset generic function.
    """
    data = [("Alice!", None), ("Bob", "USA"), (None, "UK")]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("country", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Test cleaning text and handling nulls
    cleaned = clean_dataset(
        df, 
        clean_text_cols=["name"], 
        handle_null_cols=["country"], 
        null_fill_value="Unknown"
    )
    
    rows = cleaned.collect()
    # Note: clean_text logic removes special chars like '!'
    assert rows[0]["name"] == "Alice" 
    assert rows[2]["country"] == "Unknown" # None becomes Unknown
    
def test_parse_date_col(spark):
    """
    Test date parsing generic utility.
    """
    data = [("21/08/2016",), ("01/01/2023",)]
    schema = StructType([StructField("date_str", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    parsed = parse_date_col(df, "date_str", "d/M/y", "date_parsed")
    
    assert "date_parsed" in parsed.columns
    row = parsed.first()
    from datetime import date
    assert row["date_parsed"] == date(2016, 8, 21)

def test_join_dataframes(spark):
    """
    Test generic join.
    """
    df1 = spark.createDataFrame([("1", "A")], ["id", "val1"])
    df2 = spark.createDataFrame([("1", "B")], ["id", "val2"])
    
    joined = join_dataframes(df1, df2, "id", "inner")
    
    assert joined.count() == 1
    assert "val1" in joined.columns
    assert "val2" in joined.columns
