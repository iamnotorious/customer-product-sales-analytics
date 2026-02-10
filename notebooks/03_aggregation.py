# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

# COMMAND ----------

# Install local package
# MAGIC %pip install -e /Workspace/Repos/sales_analytics/customer-product-sales-analytics

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, read_data, write_data
from sales_ecommerce_analytics_ingestion_utils.aggregation import create_profit_aggregates
from sales_ecommerce_analytics_ingestion_utils.config import Paths

import sys
# Fallback if editable install path isn't picked up immediately
if "/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src" not in sys.path:
    sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

spark = get_spark_session("SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")

# COMMAND ----------

# Read Silver Data
enriched_df = read_data(spark, "delta", f"{Paths.SILVER_BASE}/enriched_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregates

# COMMAND ----------

# Create Aggregate Table: Profit by Year, Product Category, Sub Category, Customer
gold_aggregates = create_profit_aggregates(enriched_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

write_data(gold_aggregates, "delta", "overwrite", f"{Paths.GOLD_BASE}/profit_aggregates")

print("Aggregation Complete.")
