import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from databricks_app.aggregation import create_profit_aggregates, get_profit_by_year

def test_create_profit_aggregates(spark):
    """
    Test aggregation logic.
    """
    # Mock Data
    data = [
        (2016, "Cat1", "Sub1", "Cust1", 100.0),
        (2016, "Cat1", "Sub1", "Cust1", 50.5),
        (2017, "Cat2", "Sub2", "Cust2", 200.0)
    ]
    schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    agged = create_profit_aggregates(df)
    rows = agged.collect()
    
    # Expected: 
    # 2016, Cat1, Sub1, Cust1 -> 150.5
    # 2017, Cat2, Sub2, Cust2 -> 200.0
    
    assert len(rows) == 2
    r_2016 = [r for r in rows if r["Year"] == 2016][0]
    assert r_2016["Total Profit"] == 150.5

def test_profit_by_year(spark):
    data = [(2016, 100.0), (2016, 50.0), (2017, 200.0)]
    schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    res = get_profit_by_year(df)
    rows = res.collect()
    
    r_2016 = [r for r in rows if r["Year"] == 2016][0]
    assert r_2016["Total Profit"] == 150.0  
