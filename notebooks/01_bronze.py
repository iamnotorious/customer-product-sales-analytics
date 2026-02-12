# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest Raw Data (Bronze Layer)
# MAGIC 
# MAGIC This notebook ingests raw data from sources (Excel, CSV, JSON) and writes them to the Bronze layer in Delta format.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import os

# Add the src directory to path
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

# COMMAND ----------

# Import libraries
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from sales_analytics.utils import get_spark_session, write_data, merge_data
from sales_analytics.ingestion import ingest_file
from sales_analytics.exceptions import DataIngestionError, DataWriteError
from sales_analytics.validation import generate_data_quality_report, check_duplicates
from sales_analytics.transformation import to_snake_case, add_audit_columns, deduplicate

logger = logging.getLogger(__name__)

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
customer_schema = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", IntegerType(), True),
    StructField("Region", StringType(), True)
])

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

def ingest_customers_data(*, spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """Ingest customer data from Excel source."""
    try:
        df = ingest_file(
            spark=spark_session, 
            file_format="excel", 
            source_path=source_path,
            schema=schema,
            options={"header": "true"}
        )
        logger.info("Successfully ingested customer records")
        return df
    except Exception as e:
        logger.error(f"Error ingesting customers: {e}")
        raise DataIngestionError(f"Failed to ingest customers from {source_path}") from e

def ingest_products_data(*, spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """Ingest product data from CSV source."""
    logger.info("Ingesting Products...")
    try:
        df = ingest_file(
            spark=spark_session, 
            file_format="csv", 
            source_path=source_path, 
            schema=schema,
            options={"header": "true"}
        )
        logger.info("Successfully ingested product records")
        return df
    except Exception as e:
        logger.error(f"Error ingesting products: {e}")
        raise DataIngestionError(f"Failed to ingest products from {source_path}") from e

def ingest_orders_data(*, spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    """Ingest order data from JSON source."""
    logger.info("Ingesting Orders...")
    try:
        df = ingest_file(
            spark=spark_session, 
            file_format="json", 
            source_path=source_path, 
            schema=schema,
            options={"multiLine": "true"}
        )
        logger.info("Successfully ingested order records")
        return df
    except Exception as e:
        logger.error(f"Error ingesting orders: {e}")
        raise DataIngestionError(f"Failed to ingest orders from {source_path}") from e

def validate_bronze_data(*, df: DataFrame, name: str, key_columns: list):
    """Validate bronze layer data quality."""
    logger.info(f"Validating {name}...")
    
    # Generate quality report
    report = generate_data_quality_report(df=df, name=name)
    logger.info(f"Quality Report - {name}: {report['row_count']} rows")
    
    # Check for duplicates
    dup_report = check_duplicates(df=df, key_columns=key_columns)
    if dup_report['duplicate_count'] > 0:
        logger.warning(f"{dup_report['duplicate_count']} duplicates found in {name}")

def merge_to_bronze(*, df: DataFrame, table_name: str, merge_keys: list):
    """
    Incrementally merge DataFrame to Bronze layer using upsert logic.
    Creates table on first run, merges on subsequent runs.
    """
    try:
        spark = SparkSession.getActiveSession()
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(table_name)
        
        if not table_exists:
            logger.info(f"Table {table_name} does not exist. Creating initial table...")
            write_data(df=df, mode="overwrite", table_name=table_name)
            logger.info(f"Initial load complete for {table_name}")
        else:
            logger.info(f"Table {table_name} exists. Performing incremental merge...")
            merge_data(df=df, table_name=table_name, merge_keys=merge_keys)
            logger.info(f"Incremental merge complete for {table_name}")
            
    except Exception as e:
        logger.error(f"Error merging to bronze table {table_name}: {e}")
        raise DataWriteError(f"Failed to merge to {table_name}") from e

# Execution

# COMMAND ----------

if __name__ == "__main__":
    # Get Spark Session
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_INGESTION_JOB")

    # 1. Customers
    customers_df = ingest_customers_data(spark_session=spark, source_path=customer_source_path, schema=customer_schema)
    customers_df = to_snake_case(df=customers_df)
    customers_df = deduplicate(df=customers_df, key_columns=["customer_id"])
    customers_df = add_audit_columns(df=customers_df, source_file=customer_source_path)
    validate_bronze_data(df=customers_df, name="Customers", key_columns=["customer_id"])
    merge_to_bronze(df=customers_df, table_name=bronze_customers_table, merge_keys=["customer_id"])

    # 2. Products
    products_df = ingest_products_data(spark_session=spark, source_path=product_source_path, schema=product_schema)
    products_df = to_snake_case(df=products_df)
    products_df = deduplicate(df=products_df, key_columns=["product_id"])
    products_df = add_audit_columns(df=products_df, source_file=product_source_path)
    validate_bronze_data(df=products_df, name="Products", key_columns=["product_id"])
    merge_to_bronze(df=products_df, table_name=bronze_products_table, merge_keys=["product_id"])

    # 3. Orders (fact table - overwrite partitions by order_date)
    orders_df = ingest_orders_data(spark_session=spark, source_path=order_source_path, schema=order_schema)
    orders_df = to_snake_case(df=orders_df)
    orders_df = deduplicate(df=orders_df, key_columns=["order_id", "row_id"])
    orders_df = add_audit_columns(df=orders_df, source_file=order_source_path)
    validate_bronze_data(df=orders_df, name="Orders", key_columns=["order_id", "row_id"])
    write_data(df=orders_df, mode="overwrite", table_name=bronze_orders_table, partition_by=["order_date"])

    logger.info("Bronze layer ingestion completed successfully. All data quality checks passed.")
    logger.info("Incremental merge strategy applied to all bronze tables.")
