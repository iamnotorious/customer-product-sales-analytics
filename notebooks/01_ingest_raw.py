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

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, write_data
from sales_ecommerce_analytics_ingestion_utils.ingestion import ingest_file

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

def ingest_customers_data(spark_session: SparkSession, source_path: str) -> DataFrame:
    print("Ingesting Customers...")
    return ingest_file(
        spark=spark_session, 
        file_format="excel", 
        source_path=source_path,
        options={"header": "true", "inferSchema": "true"}
    )

def ingest_products_data(spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    print("Ingesting Products...")
    return ingest_file(
        spark=spark_session, 
        file_format="csv", 
        source_path=source_path, 
        schema=schema,
        options={"header": "true"}
    )

def ingest_orders_data(spark_session: SparkSession, source_path: str, schema: StructType) -> DataFrame:
    print("Ingesting Orders...")
    return ingest_file(
        spark=spark_session, 
        file_format="json", 
        source_path=source_path, 
        schema=schema,
        options={"multiLine": "true"}
    )

def write_to_bronze(df: DataFrame, table_name: str):
    write_data(
        df=df, 
        mode="overwrite", 
        table_name=table_name
    )

# Execution
if __name__ == "__main__":
    # Get Spark Session
    spark = get_spark_session("SALES_ECOMMERCE_ANALYTICS_INGESTION_JOB")

    # 1. Customers
    customers_df = ingest_customers_data(spark, customer_source_path)
    write_to_bronze(customers_df, bronze_customers_table)

    # 2. Products
    products_df = ingest_products_data(spark, product_source_path, product_schema)
    write_to_bronze(products_df, bronze_products_table)

    # 3. Orders
    orders_df = ingest_orders_data(spark, order_source_path, order_schema)
    write_to_bronze(orders_df, bronze_orders_table)

    print("Ingestion Complete.")
