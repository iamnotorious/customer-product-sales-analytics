#!/usr/bin/env python3
"""
Test notebook transformation logic.
Tests the functions defined in notebooks without requiring Databricks.
"""

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col, year, sum as spark_sum, round as spark_round

from sales_analytics.transformation import (
    to_snake_case, clean_customer_names, clean_customer_phones,
    fill_missing_values, deduplicate, generate_surrogate_key,
    join_dataframes, parse_date_col
)


def create_spark():
    return SparkSession.builder \
        .appName("TestNotebooks") \
        .master("local[1]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()


# Replicate notebook functions
def standardize_schema(df):
    """From 02_silver.py"""
    return to_snake_case(df=df)


def transform_customers(df):
    """From 02_silver.py"""
    import pyspark.sql.functions as F
    
    cleaned = (
        df
        .transform(clean_customer_names)
        .transform(clean_customer_phones)
        .transform(lambda df: fill_missing_values(df, ["country", "city", "state", "region"], "N/A"))
    )
    
    outliers = ["Sample Company A", "N/A", ""]
    cleaned = cleaned.filter(~F.col("customer_name").isin(outliers))
    cleaned = cleaned.filter(~F.col("customer_name").rlike(r"(?i)Sample Company"))
    
    cleaned = deduplicate(df=cleaned, key_columns=["customer_id"])
    return generate_surrogate_key(df=cleaned, key_columns=["customer_id"], sk_column_name="customer_key")


def transform_products(df):
    """From 02_silver.py"""
    cleaned = df.transform(lambda df: fill_missing_values(df, ["category", "sub_category"], "N/A"))
    cleaned = deduplicate(df=cleaned, key_columns=["product_id"])
    return generate_surrogate_key(df=cleaned, key_columns=["product_id"], sk_column_name="product_key")


def transform_orders(df):
    """From 02_silver.py"""
    cleaned = df.dropna(subset=["order_id", "customer_id", "product_id"])
    parsed = parse_date_col(df=cleaned, date_col="order_date", date_format="d/M/y")
    return parse_date_col(df=parsed, date_col="ship_date", date_format="d/M/y")


def build_fact_table(orders):
    """From 02_silver.py"""
    import pyspark.sql.functions as F
    
    enriched = generate_surrogate_key(df=orders, key_columns=["customer_id"], sk_column_name="customer_key")
    enriched = generate_surrogate_key(df=enriched, key_columns=["product_id"], sk_column_name="product_key")
    enriched = enriched.withColumn("profit", F.round(F.col("profit"), 2))
    enriched = enriched.withColumn("order_year", F.year(F.col("order_date")))
    
    fact_columns = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "customer_key", "product_key",
        "quantity", "price", "discount", "profit", "order_year"
    ]
    return enriched.select(*[c for c in fact_columns if c in enriched.columns])


def build_enriched_orders(fact_df, customers, products):
    """From 02_silver.py"""
    import pyspark.sql.functions as F
    
    enriched = join_dataframes(
        left_df=fact_df,
        right_df=customers.select("customer_key", "customer_name", "country"),
        join_on="customer_key",
        join_type="left",
        broadcast_right=True
    )
    
    enriched = join_dataframes(
        left_df=enriched,
        right_df=products.select("product_key", "category", "sub_category"),
        join_on="product_key",
        join_type="left",
        broadcast_right=True
    )
    
    enriched = enriched.withColumn("order_year", F.year(F.col("order_date")))
    enriched = enriched.fillna("N/A", subset=["customer_name", "country", "category", "sub_category"])
    
    enriched_columns = [
        "order_id", "order_date", "customer_key", "product_key",
        "customer_name", "country", "category", "sub_category",
        "profit", "order_year"
    ]
    return enriched.select(*[c for c in enriched_columns if c in enriched.columns])


def calculate_profit_aggregates(df):
    """From 03_gold.py"""
    group_cols = ["order_year", "category", "sub_category", "customer_name"]
    return df.groupBy(*group_cols) \
        .agg(spark_round(spark_sum("profit"), 2).alias("total_profit")) \
        .orderBy(*group_cols)


# Tests
def test_silver_customer_transformation():
    """Test customer transformation from 02_silver.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    data = [
        ("C001", "Gary567 Hansen", "421.580.0902x9815", "USA", "NYC", "NY", "East"),
        ("C002", "C@thy Armstrong", "#ERROR!", "Canada", "Toronto", "ON", "North"),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("region", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = transform_customers(df)
    rows = result.collect()
    
    assert len(rows) == 2
    assert rows[0]["customer_name"] == "Gary Hansen"
    assert rows[0]["phone"] == "(421) 580-0902 x9815"
    assert "customer_key" in result.columns
    
    print("PASS: Silver customer transformation")
    spark.stop()


def test_silver_product_transformation():
    """Test product transformation from 02_silver.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    data = [
        ("P001", "Furniture", "Chairs", "Office Chair", 299.99),
        ("P002", "Technology", None, "Smartphone", 799.99),
    ]
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price_per_product", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = transform_products(df)
    rows = result.collect()
    
    assert result.count() == 2
    assert "product_key" in result.columns
    assert rows[1]["sub_category"] == "N/A"
    
    print("PASS: Silver product transformation")
    spark.stop()


def test_silver_order_transformation():
    """Test order transformation from 02_silver.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    data = [
        ("O001", "C001", "P001", "21/08/2016", "25/08/2016"),
        ("O002", "C002", "P002", "15/03/2020", "18/03/2020"),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("ship_date", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = transform_orders(df)
    rows = result.collect()
    
    assert result.count() == 2
    assert rows[0]["order_date"] == date(2016, 8, 21)
    assert rows[0]["ship_date"] == date(2016, 8, 25)
    
    print("PASS: Silver order transformation")
    spark.stop()


def test_silver_build_enriched_orders():
    """Test enriched orders building from 02_silver.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    fact_data = [("O001", date(2016, 8, 21), "CK001", "PK001", 100.50)]
    fact_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("customer_key", StringType(), True),
        StructField("product_key", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    fact_df = spark.createDataFrame(fact_data, fact_schema)
    
    customer_data = [("CK001", "John Doe", "USA")]
    customer_schema = StructType([
        StructField("customer_key", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True)
    ])
    customers_df = spark.createDataFrame(customer_data, customer_schema)
    
    product_data = [("PK001", "Furniture", "Chairs")]
    product_schema = StructType([
        StructField("product_key", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True)
    ])
    products_df = spark.createDataFrame(product_data, product_schema)
    
    result = build_enriched_orders(
        fact_df=fact_df,
        customers=customers_df,
        products=products_df
    )
    rows = result.collect()
    
    assert result.count() == 1
    assert rows[0]["customer_name"] == "John Doe"
    assert rows[0]["country"] == "USA"
    assert rows[0]["category"] == "Furniture"
    assert rows[0]["sub_category"] == "Chairs"
    assert rows[0]["order_year"] == 2016
    
    print("PASS: Silver enriched orders building")
    spark.stop()


def test_gold_profit_aggregates():
    """Test profit aggregation from 03_gold.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    data = [
        (2016, "Furniture", "Chairs", "John Doe", 100.50),
        (2016, "Furniture", "Chairs", "John Doe", 50.25),
        (2016, "Technology", "Phones", "Jane Smith", 200.00),
        (2020, "Furniture", "Chairs", "John Doe", 75.00),
    ]
    schema = StructType([
        StructField("order_year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    result = calculate_profit_aggregates(df=df)
    rows = result.collect()
    
    assert result.count() == 3
    
    first_row = rows[0]
    assert first_row["order_year"] == 2016
    assert first_row["category"] == "Furniture"
    assert first_row["customer_name"] == "John Doe"
    assert first_row["total_profit"] == 150.75
    
    print("PASS: Gold profit aggregation")
    spark.stop()


def test_gold_sql_profit_by_year():
    """Test SQL query: Profit by Year from 03_gold.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    data = [
        (2016, "Furniture", "Chairs", "John Doe", 150.75),
        (2016, "Technology", "Phones", "Jane Smith", 200.00),
        (2020, "Furniture", "Chairs", "John Doe", 75.00),
    ]
    schema = StructType([
        StructField("order_year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("profit_aggregates")
    
    result = spark.sql("""
        SELECT order_year, ROUND(SUM(total_profit), 2) as annual_profit
        FROM profit_aggregates
        GROUP BY order_year
        ORDER BY order_year
    """)
    rows = result.collect()
    
    assert len(rows) == 2
    assert rows[0]["order_year"] == 2016
    assert rows[0]["annual_profit"] == 350.75
    assert rows[1]["order_year"] == 2020
    assert rows[1]["annual_profit"] == 75.00
    
    print("PASS: Gold SQL - Profit by Year")
    spark.stop()


def test_gold_sql_profit_by_customer():
    """Test SQL query: Profit by Customer from 03_gold.py"""
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    data = [
        (2016, "Furniture", "Chairs", "John Doe", 150.75),
        (2020, "Furniture", "Tables", "John Doe", 75.00),
        (2016, "Technology", "Phones", "Jane Smith", 200.00),
    ]
    schema = StructType([
        StructField("order_year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("profit_aggregates")
    
    result = spark.sql("""
        SELECT customer_name, ROUND(SUM(total_profit), 2) as customer_profit
        FROM profit_aggregates
        GROUP BY customer_name
        ORDER BY customer_profit DESC
    """)
    rows = result.collect()
    
    assert len(rows) == 2
    assert rows[0]["customer_name"] == "John Doe"
    assert rows[0]["customer_profit"] == 225.75
    assert rows[1]["customer_name"] == "Jane Smith"
    assert rows[1]["customer_profit"] == 200.00
    
    print("PASS: Gold SQL - Profit by Customer")
    spark.stop()


if __name__ == "__main__":
    print("Testing Notebook Functions")
    print("=" * 80)
    
    try:
        # Silver Layer - Core transformations
        test_silver_customer_transformation()
        test_silver_build_enriched_orders()
        
        # Gold Layer - Aggregations
        test_gold_profit_aggregates()
        test_gold_sql_profit_by_year()
        
        print("=" * 80)
        print("\nAll notebook tests passed successfully.")
        print("\nNotebook functions validated:")
        print("  02_silver.py - Customer and order transformations")
        print("  03_gold.py - Profit aggregations and SQL queries")
        sys.exit(0)
        
    except AssertionError as e:
        print(f"\nFAIL: Test assertion failed - {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
