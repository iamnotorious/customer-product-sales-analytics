import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sales_ecommerce_analytics_ingestion_utils.transformation import clean_text, handle_nulls, clean_dataset, enrich_orders, parse_order_dates

def test_clean_text(spark):
    """
    Tests special character removal.
    """
    data = [("Leather___Chair!!!",), ("Imation Secure+ Encrypted",), ("Clean Text",)]
    schema = StructType([StructField("Product Name", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    cleaned_df = clean_text(df, "Product Name")
    results = [row["Product Name"] for row in cleaned_df.collect()]
    
    assert results[0] == "LeatherChair"
    assert results[1] == "Imation Secure Encrypted"
    assert results[2] == "Clean Text"

def test_handle_nulls(spark):
    """
    Tests null replacement.
    """
    data = [("Laptop", "Tech"), (None, "Tech"), ("Chair", None)]
    schema = StructType([
        StructField("Product", StringType(), True),
        StructField("Category", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    handled_df = handle_nulls(df, ["Product", "Category"], "N/A")
    results = handled_df.collect()
    
    assert results[1]["Product"] == "N/A"
    assert results[2]["Category"] == "N/A"
    assert results[0]["Product"] == "Laptop"

def test_clean_dataset(spark):
    """
    Test generic clean_dataset function.
    """
    data_full = [("C1", "John_Doe!!!", None, "USA", None, None)]
    schema = StructType([
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    df = spark.createDataFrame(data_full, schema)
    
    # Configure cleaning rules
    cleaned = clean_dataset(
        df,
        clean_text_cols=["Customer Name"],
        handle_null_cols=["City", "State", "Region"],
        mandatory_cols=["Customer ID"]
    )
    
    row = cleaned.first()
    
    assert row["Customer Name"] == "JohnDoe" 
    assert row["City"] == "N/A"
    assert row["State"] == "N/A"

def test_enrich_orders(spark):
    """
    Test enrichment logic: Joins, Profit Calc (rounding), Year extraction.
    """
    # Create Mock DataFrames
    orders_data = [
        ("1001", "21/08/2016", "C1", "P1", 10.1234),
        ("1002", "15/01/2017", "C2", "P2", 20.5)
    ]
    orders_schema = StructType([
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Profit", DoubleType(), True)
    ])
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    
    customers_data = [("C1", "Cust One", "USA"), ("C2", "Cust Two", "UK")]
    customers_schema = StructType([
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Country", StringType(), True)
    ])
    cust_df = spark.createDataFrame(customers_data, customers_schema)
    
    products_data = [("P1", "Cat1", "Sub1"), ("P2", "Cat2", "Sub2")]
    products_schema = StructType([
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True)
    ])
    prod_df = spark.createDataFrame(products_data, products_schema)
    
    # Enrichment expects Year to be present? No, parse_order_dates adds Year.
    # But clean_orders/enrich_orders logic in transformation.py needs checks.
    
    # First apply date parsing helper
    orders_parsed = parse_order_dates(orders_df)
    
    # Now enrich
    enriched_df = enrich_orders(orders_parsed, cust_df, prod_df)
    
    rows = enriched_df.collect()
    r1 = rows[0] # 1001
    
    # Verify Joins
    assert r1["Customer Name"] == "Cust One"
    assert r1["Category"] == "Cat1"
    
    # Verify Profit Rounding (10.1234 -> 10.12)
    assert r1["Profit"] == 10.12
    
    # Verify Year Extraction
    # 21/08/2016 -> 2016
    assert r1["Year"] == 2016
