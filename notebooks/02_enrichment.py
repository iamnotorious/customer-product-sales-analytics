# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Data Enrichment (Silver Layer)
# MAGIC 
# MAGIC This notebook cleans the Bronze data and enriches the Orders with Customer and Product information.
# MAGIC Key steps:
# MAGIC 1. Cleanse text (remove special characters).
# MAGIC 2. Handle nulls.
# MAGIC 3. Parse dates.
# MAGIC 4. Join datasets.
# MAGIC 5. Calculate rounded Profit.

# COMMAND ----------

# Install local package
# MAGIC %pip install -e /Workspace/Repos/sales_analytics/customer-product-sales-analytics

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, read_data, write_data
from sales_ecommerce_analytics_ingestion_utils.transformation import clean_dataset, parse_order_dates, enrich_orders
from sales_ecommerce_analytics_ingestion_utils.config import Paths

import sys
# Fallback if editable install path isn't picked up immediately
if "/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src" not in sys.path:
    sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")
from sales_ecommerce_analytics_ingestion_utils.config import Paths

spark = get_spark_session("SALES_ECOMMERCE_ANALYTICS_ENRICHMENT_JOB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_cust = read_data(spark, "delta", f"{Paths.BRONZE_BASE}/customers")
bronze_prod = read_data(spark, "delta", f"{Paths.BRONZE_BASE}/products")
bronze_ord = read_data(spark, "delta", f"{Paths.BRONZE_BASE}/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse Data

# COMMAND ----------

# Clean Customers
silver_cust = clean_dataset(
    bronze_cust, 
    clean_text_cols=["Customer Name"], 
    handle_null_cols=["Country", "City", "State", "Region"],
    null_fill_value="N/A"
)

# Clean Products
silver_prod = clean_dataset(
    bronze_prod, 
    clean_text_cols=["Product Name"], 
    handle_null_cols=["Category", "Sub-Category"],
    null_fill_value="N/A"
)

# Clean Orders (Validate IDs)
silver_ord_cleaned = clean_dataset(
    bronze_ord,
    mandatory_cols=["Order ID", "Customer ID", "Product ID"]
)

# Parse Dates
silver_ord_parsed = parse_order_dates(silver_ord_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Orders

# COMMAND ----------

# Join and enrich
enriched_df = enrich_orders(silver_ord_parsed, silver_cust, silver_prod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

# Write Cleansed Dimensions to Silver
write_data(silver_cust, "delta", "overwrite", f"{Paths.SILVER_BASE}/customers")
write_data(silver_prod, "delta", "overwrite", f"{Paths.SILVER_BASE}/products")

# Write Enriched Data to Silver
# Partition by Year for performance
write_data(enriched_df, "delta", "overwrite", f"{Paths.SILVER_BASE}/enriched_orders", partition_by=["Year"])

print("Enrichment Complete.")
