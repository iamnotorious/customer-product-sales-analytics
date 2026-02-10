# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Analysis and Insights
# MAGIC 
# MAGIC This notebook executes the specific SQL queries requested by the business.

# COMMAND ----------

# Install local package
# MAGIC %pip install -e /Workspace/Repos/sales_analytics/customer-product-sales-analytics

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks_app.utils import get_spark_session, read_data
from databricks_app.config import Paths

spark = get_spark_session("AnalysisJob")

# Read Silver Enriched Data (Single Source of Truth for ad-hoc analysis)
enriched_df = read_data(spark, "delta", f"{Paths.SILVER_BASE}/enriched_orders")

# Create Temp View for SQL
enriched_df.createOrReplaceTempView("enriched_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Profit by Year
# MAGIC SELECT Year, ROUND(SUM(Profit), 2) as Total_Profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY Year
# MAGIC ORDER BY Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Profit by Year + Product Category
# MAGIC SELECT Year, Category, ROUND(SUM(Profit), 2) as Total_Profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY Year, Category
# MAGIC ORDER BY Year, Category

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Profit by Customer
# MAGIC SELECT `Customer Name`, ROUND(SUM(Profit), 2) as Total_Profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY `Customer Name`
# MAGIC ORDER BY Total_Profit DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Profit by Customer + Year
# MAGIC SELECT `Customer Name`, Year, ROUND(SUM(Profit), 2) as Total_Profit
# MAGIC FROM enriched_orders
# MAGIC GROUP BY `Customer Name`, Year
# MAGIC ORDER BY `Customer Name`, Year
