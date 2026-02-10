# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest Raw Data (Bronze Layer)
# MAGIC 
# MAGIC This notebook ingests raw data from sources (Excel, CSV, JSON) and writes them to the Bronze layer in Delta format.

# COMMAND ----------

# Install local package
# MAGIC %pip install -e /Workspace/Repos/sales_analytics/customer-product-sales-analytics

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import os

# Explicitly add the src directory to path in case editable install is slow to register
# Only necessary if you get ModuleNotFound errors
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

# Import libraries
from pyspark.sql import SparkSession
from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, write_data
from sales_ecommerce_analytics_ingestion_utils.ingestion import ingest_customers, ingest_products, ingest_orders
from sales_ecommerce_analytics_ingestion_utils.config import Paths

# Get Spark Session
spark = get_spark_session("SALES_ECOMMERCE_ANALYTICS_INGESTION_JOB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customers

# COMMAND ----------

# Read Customer Data
print("Ingesting Customers...")
customers_df = ingest_customers(spark, Paths.CUSTOMER_SOURCE)

# Write to Bronze
write_data(customers_df, "delta", "overwrite", f"{Paths.BRONZE_BASE}/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Products

# COMMAND ----------

# Read Product Data
print("Ingesting Products...")
products_df = ingest_products(spark, Paths.PRODUCT_SOURCE)

# Write to Bronze
write_data(products_df, "delta", "overwrite", f"{Paths.BRONZE_BASE}/products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Orders

# COMMAND ----------

# Read Orders Data
print("Ingesting Orders...")
orders_df = ingest_orders(spark, Paths.ORDER_SOURCE)

# Write to Bronze
# Partitioning by Order Date (or Year/Month) is often good, but raw might just be flat.
# Let's keep it simple for Bronze - strict copy of source.
write_data(orders_df, "delta", "overwrite", f"{Paths.BRONZE_BASE}/orders")

print("Ingestion Complete.")
