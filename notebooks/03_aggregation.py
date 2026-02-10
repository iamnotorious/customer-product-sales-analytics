# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

# COMMAND ----------

from databricks_app.utils import get_spark_session, read_data, write_data
from databricks_app.aggregation import create_profit_aggregates
from databricks_app.config import Paths

spark = get_spark_session("AggregationJob")

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
