# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Analysis and Insights
# MAGIC 
# MAGIC This notebook executes the specific SQL queries requested by the business.

# COMMAND ----------

# Install local package
import sys
import os

# Add the src directory to path
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, read_data

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

spark = get_spark_session("SALES_ECOMMERCE_ANALYTICS_ANALYSIS_JOB")

# COMMAND ----------

# Read Silver Enriched Data (Single Source of Truth for ad-hoc analysis)
enriched_df = read_data(spark=spark, table_name="silver_enriched_orders")

# Create Temp View for SQL
enriched_df.createOrReplaceTempView("enriched_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Analysis (Gold Layer Logic moved here)

# COMMAND ----------

from sales_ecommerce_analytics_ingestion_utils.aggregation import create_aggregates

# 1. Profit by Year
profit_by_year = create_aggregates(
    df=enriched_df,
    group_by_cols=["year"],
    agg_col="profit",
    alias_col="total_profit"
)
display(profit_by_year)

# 2. Profit by Year + Category
profit_by_year_category = create_aggregates(
    df=enriched_df,
    group_by_cols=["year", "category"],
    agg_col="profit",
    alias_col="total_profit"
)
display(profit_by_year_category)

# 3. Profit by Customer
profit_by_customer = create_aggregates(
    df=enriched_df,
    group_by_cols=["customer_name"],
    agg_col="profit",
    alias_col="total_profit"
)
display(profit_by_customer)

# 4. Profit by Customer + Year
profit_by_customer_year = create_aggregates(
    df=enriched_df,
    group_by_cols=["customer_name", "year"],
    agg_col="profit",
    alias_col="total_profit"
)
display(profit_by_customer_year)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Profit by Year
# MAGIC SELECT year, ROUND(SUM(profit), 2) as total_profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY year
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Profit by Year + Product Category
# MAGIC SELECT year, category, ROUND(SUM(profit), 2) as total_profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year, category

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Profit by Customer
# MAGIC SELECT customer_name, ROUND(SUM(profit), 2) as total_profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY customer_name
# MAGIC ORDER BY total_profit DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Profit by Customer + Year
# MAGIC SELECT customer_name, year, ROUND(SUM(profit), 2) as total_profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY customer_name, year
# MAGIC ORDER BY customer_name, year
