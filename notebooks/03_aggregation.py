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

# Configuration
silver_enriched_orders_table = "sales.silver.sales_ecommerce_enriched_orders"
gold_profit_aggregates_table = "sales.gold.sales_ecommerce_profit_aggregates"

spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

# Read Silver Data
enriched_df = read_data(spark=spark, table_name=silver_enriched_orders_table)

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
    table_name=gold_profit_aggregates_table,
    partition_by=["year"]
)

print("Aggregation Complete.")
