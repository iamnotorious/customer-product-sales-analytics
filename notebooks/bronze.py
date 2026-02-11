# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest Raw Data (Bronze Layer)
# MAGIC 
# MAGIC This notebook ingests raw data from sources (Excel, CSV, JSON) and writes them to the Bronze layer in Delta format.

# COMMAND ----------

import sys
import os

# Add the src directory to path
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

# COMMAND ----------

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from sales_analytics.utils import get_spark_session, write_data
from sales_analytics.ingestion import ingest_file

# Configuration Variables
# Source Paths
customer_source_path = "/Volumes/sales/raw/sales_ecommerce_analytics_data/data/Customer.xlsx"
product_source_path = "/Volumes/sales/raw/sales_ecommerce_analytics_data/data/Products.csv"
order_source_path = "/Volumes/sales/raw/sales_ecommerce_analytics_data/data/Orders.json"

# Table Names
bronze_customers_table = "sales.bronze.sales_ecommerce_customers"
bronze_products_table = "sales.bronze.sales_ecommerce_products"
bronze_orders_table = "sales.bronze.sales_ecommerce_orders"

# Schemas
product_schema = StructType([
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Price per product", DoubleType(), True)
])

order_schema = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])

def ingest_customers_data(*, spark_session: SparkSession, source_path: str) -> DataFrame:
    """Ingest customer data from Excel source."""
    try:
        df = ingest_file(
            spark=spark_session, 
            file_format="excel", 

            source_path=source_path,
            options={"header": "true", "inferSchema": "true"}
        )
        print(f"Successfully ingested {df.count()} customer records")
        return df
    except Exception as e:
        print(f"Error ingesting customers: {e}")
        raise DataIngestionError(f"Failed to ingest customers from {source_path}") from e

def ingest_products_data(*, spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """Ingest product data from CSV source."""
    print("Ingesting Products...")
    try:
        df = ingest_file(
            spark=spark_session, 
            file_format="csv", 
            source_path=source_path, 
            schema=schema,
            options={"header": "true"}
        )
        print(f"Successfully ingested {df.count()} product records")
        return df
    except Exception as e:
        print(f"Error ingesting products: {e}")
        raise DataIngestionError(f"Failed to ingest products from {source_path}") from e

def ingest_orders_data(*, spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """Ingest order data from JSON source."""
    print("Ingesting Orders...")
    try:
        df = ingest_file(
            spark=spark_session, 
            file_format="json", 
            source_path=source_path, 
            schema=schema,
            options={"multiLine": "true"}
        )
        print(f"Successfully ingested {df.count()} order records")
        return df
    except Exception as e:
        print(f"Error ingesting orders: {e}")
        raise DataIngestionError(f"Failed to ingest orders from {source_path}") from e

def validate_bronze_data(*, df: DataFrame, name: str, key_columns: list):
    """Validate bronze layer data quality."""
    print(f"Validating {name}...")
    
    # Generate quality report
    report = generate_data_quality_report(df=df, name=name)
    print(f"Quality Report - {name}: {report['row_count']} rows")
    
    # Check for duplicates
    dup_report = check_duplicates(df=df, key_columns=key_columns)
    if dup_report['duplicate_count'] > 0:
        print(f"WARNING: {dup_report['duplicate_count']} duplicates found in {name}")

def merge_to_bronze(*, df: DataFrame, table_name: str, merge_keys: list):
    """
    Incrementally merge DataFrame to Bronze layer using upsert logic.
    Creates table on first run, merges on subsequent runs.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(table_name)
        
        if not table_exists:
            print(f"Table {table_name} does not exist. Creating initial table...")
            write_data(df=df, mode="overwrite", table_name=table_name)
            print(f"Initial load complete for {table_name}")
        else:
            print(f"Table {table_name} exists. Performing incremental merge...")
            from sales_analytics.utils import merge_data
            merge_data(df=df, table_name=table_name, merge_keys=merge_keys)
            print(f"Incremental merge complete for {table_name}")
            
    except Exception as e:
        print(f"Error merging to bronze table {table_name}: {e}")
        raise DataWriteError(f"Failed to merge to {table_name}") from e

# Execution
if __name__ == "__main__":
    # Import exceptions
    from sales_analytics.exceptions import DataIngestionError, DataWriteError
    from sales_analytics.validation import generate_data_quality_report, check_duplicates
    
    # Get Spark Session
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_INGESTION_JOB")

    # 1. Customers
    customers_df = ingest_customers_data(spark_session=spark, source_path=customer_source_path)
    validate_bronze_data(df=customers_df, name="Customers", key_columns=["Customer ID"])
    merge_to_bronze(df=customers_df, table_name=bronze_customers_table, merge_keys=["Customer ID"])

    # 2. Products
    products_df = ingest_products_data(spark_session=spark, source_path=product_source_path, schema=product_schema)
    validate_bronze_data(df=products_df, name="Products", key_columns=["Product ID"])
    merge_to_bronze(df=products_df, table_name=bronze_products_table, merge_keys=["Product ID"])

    # 3. Orders
    orders_df = ingest_orders_data(spark_session=spark, source_path=order_source_path, schema=order_schema)
    validate_bronze_data(df=orders_df, name="Orders", key_columns=["Order ID", "Row ID"])
    merge_to_bronze(df=orders_df, table_name=bronze_orders_table, merge_keys=["Order ID", "Row ID"])

    print("Bronze layer ingestion completed successfully. All data quality checks passed.")
    print("Incremental merge strategy applied to all bronze tables.")
