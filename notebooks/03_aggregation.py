# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

# COMMAND ----------

# Install local package
import sys
import os

# Add the src directory to path
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, read_data, write_data
from sales_ecommerce_analytics_ingestion_utils.aggregation import create_aggregates

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

class Tables:
    # Bronze Tables
    BRONZE_CUSTOMERS = "sales.bronze.customers"
    BRONZE_PRODUCTS = "sales.bronze.products"
    BRONZE_ORDERS = "sales.bronze.orders"
    
    # Silver Tables
    SILVER_CUSTOMERS = "sales.silver.customers"
    SILVER_PRODUCTS = "sales.silver.products"
    SILVER_ENRICHED_ORDERS = "sales.silver.enriched_orders"
    
    # Gold Tables
    GOLD_PROFIT_AGGREGATES = "sales.gold.profit_aggregates"

spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

# Read Silver Data
enriched_df = read_data(spark=spark, table_name=Tables.SILVER_ENRICHED_ORDERS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregates

# COMMAND ----------

# Create Aggregate Table: Profit by Year, Product Category, Sub Category, Customer
# Dimensions: year, category, sub_category, customer_name
# Metric: profit
gold_aggregates = create_aggregates(
    df=enriched_df,
    group_by_cols=["year", "category", "sub_category", "customer_name"],
    agg_col="profit",
    alias_col="total_profit",
    round_places=2
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

write_data(
    df=gold_aggregates, 
    mode="overwrite", 
    table_name=Tables.GOLD_PROFIT_AGGREGATES,
    partition_by=["year"]
)

print("Aggregation Complete.")
