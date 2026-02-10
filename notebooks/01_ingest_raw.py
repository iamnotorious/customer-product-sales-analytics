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
    StructField("Price per product", DoubleType(), True) # Inferred as double
])

order_schema = StructType([
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
    source_path=customer_source_path,
    options={"header": "true", "inferSchema": "true"}
)

# Write to Bronze (Managed Table)
# Using Unity Catalog: sales catalog, bronze schema
write_data(
    df=customers_df, 
    mode="overwrite", 
    table_name=bronze_customers_table
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
    source_path=product_source_path, 
    schema=product_schema,
    options={"header": "true"}
)

# Write to Bronze (Managed Table)
write_data(
    df=products_df, 
    mode="overwrite", 
    table_name=bronze_products_table
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
    source_path=order_source_path, 
    schema=order_schema,
    options={"multiLine": "true"}
)

# Write to Bronze (Managed Table)
write_data(
    df=orders_df, 
    mode="overwrite", 
    table_name=bronze_orders_table
)

print("Ingestion Complete.")
