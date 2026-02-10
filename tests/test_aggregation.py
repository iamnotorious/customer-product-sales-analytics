import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from sales_ecommerce_analytics_ingestion_utils.aggregation import create_aggregates

def test_create_aggregates(spark):
    """
    Test generic aggregation logic.
    """
    # Create sample data
    data = [
        ("2023", "Electronics", "Phones", "Alice", 100.0),
        ("2023", "Electronics", "Phones", "Alice", 200.0),
        ("2023", "Furniture", "Chairs", "Bob", 50.0)
    ]
    schema = StructType([
        StructField("year", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Run aggregation
    result = create_aggregates(
        df, 
        group_by_cols=["year", "category"], 
        agg_col="profit", 
        alias_col="total_profit"
    )
    
    # Assertions
    assert result.count() == 2
    # Check Alice's category total
    alice_row = result.filter(result.category == "Electronics").first()
    assert alice_row["total_profit"] == 300.0
