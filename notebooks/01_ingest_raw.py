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

class Paths:
    # Used for installing the package in editable mode via notebooks
    PROJECT_ROOT = "/Workspace/Repos/sales_analytics/customer-product-sales-analytics"
    
    BASE_DATA_DIR = "/FileStore/tables/data" # Assumed Databricks path, adjustable
    
    # Source Paths (Local mapping for reference, in DBX these would be mounted)
    CUSTOMER_SOURCE = "/Volumes/sales/raw/sales_ecommerce_analytics_data/data/Customer.xlsx"
    PRODUCT_SOURCE = "/Volumes/sales/raw/sales_ecommerce_analytics_data/data/Products.csv"
    ORDER_SOURCE = "/Volumes/sales/raw/sales_ecommerce_analytics_data/data/Orders.json"

    # Layer Paths
    BRONZE_BASE = "dbfs:/mnt/delta/bronze"
    SILVER_BASE = "dbfs:/mnt/delta/silver"
    GOLD_BASE = "dbfs:/mnt/delta/gold"

class Schemas:
    # Defined based on inspection of Products.csv and Orders.json
    
    PRODUCT_SCHEMA = StructType([
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price per product", DoubleType(), True) # Inferred as double
    ])

    ORDER_SCHEMA = StructType([
        StructField("Row ID", IntegerType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True), # format DD/MM/YYYY needs parsing
        StructField("Ship Date", StringType(), True),  # format DD/MM/YYYY needs parsing
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True)
    ])

    # Customer schema inferred from typical domain usage
    CUSTOMER_SCHEMA = StructType([
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal Code", StringType(), True),
        StructField("Region", StringType(), True)
    ])

# Get Spark Session
spark = get_spark_session("SALES_ECOMMERCE_ANALYTICS_INGESTION_JOB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customers

# COMMAND ----------

# Read Customer Data
print("Ingesting Customers...")
customers_df = ingest_file(
    spark=spark, 
    file_format="excel", 
    source_path=Paths.CUSTOMER_SOURCE,
    options={"header": "true", "inferSchema": "true"}
)

# Write to Bronze (Managed Table)
write_data(
    df=customers_df, 
    mode="overwrite", 
    table_name="bronze_customers"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Products

# COMMAND ----------

# Read Product Data
print("Ingesting Products...")
products_df = ingest_file(
    spark=spark, 
    file_format="csv", 
    source_path=Paths.PRODUCT_SOURCE, 
    schema=Schemas.PRODUCT_SCHEMA,
    options={"header": "true"}
)

# Write to Bronze (Managed Table)
write_data(
    df=products_df, 
    mode="overwrite", 
    table_name="bronze_products"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Orders

# COMMAND ----------

# Read Orders Data
print("Ingesting Orders...")
orders_df = ingest_file(
    spark=spark, 
    file_format="json", 
    source_path=Paths.ORDER_SOURCE, 
    schema=Schemas.ORDER_SCHEMA,
    options={"multiLine": "true"}
)

# Write to Bronze (Managed Table)
write_data(
    df=orders_df, 
    mode="overwrite", 
    table_name="bronze_orders"
)

print("Ingestion Complete.")
