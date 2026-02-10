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

import sys
import os

# Add the src directory to path
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, read_data, write_data
from sales_ecommerce_analytics_ingestion_utils.transformation import (
    clean_dataset, 
    to_snake_case, 
    join_dataframes, 
    calculate_metric, 
    parse_date_col, 
    add_year_col
)

# Configuration Rules
bronze_customers_table = "sales.bronze.sales_ecommerce_customers"
bronze_products_table = "sales.bronze.sales_ecommerce_products"
bronze_orders_table = "sales.bronze.sales_ecommerce_orders"

silver_customers_table = "sales.silver.sales_ecommerce_customers"
silver_products_table = "sales.silver.sales_ecommerce_products"
silver_enriched_orders_table = "sales.silver.sales_ecommerce_enriched_orders"

spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_ENRICHMENT_JOB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_cust = read_data(spark=spark, table_name=bronze_customers_table)
bronze_prod = read_data(spark=spark, table_name=bronze_products_table)
bronze_ord = read_data(spark=spark, table_name=bronze_orders_table)

# Standardize Columns to Snake Case
bronze_cust = to_snake_case(df=bronze_cust)
bronze_prod = to_snake_case(df=bronze_prod)
bronze_ord = to_snake_case(df=bronze_ord)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse Data

# COMMAND ----------

# Clean Customers
# "Customer Name" -> "customer_name"
silver_cust = clean_dataset(
    df=bronze_cust, 
    clean_text_cols=["customer_name"], 
    handle_null_cols=["country", "city", "state", "region"],
    null_fill_value="N/A"
)

# Clean Products
# "Product Name" -> "product_name", "Sub-Category" -> "sub_category"
silver_prod = clean_dataset(
    df=bronze_prod, 
    clean_text_cols=["product_name"], 
    handle_null_cols=["category", "sub_category"],
    null_fill_value="N/A"
)

# Clean Orders (Validate IDs)
silver_ord_cleaned = clean_dataset(
    df=bronze_ord,
    mandatory_cols=["order_id", "customer_id", "product_id"]
)

# Parse Dates (order_date)
silver_ord_parsed = parse_date_col(
    df=silver_ord_cleaned, 
    date_col="order_date", 
    date_format="d/M/y"
)

# Add Year
silver_ord_parsed = add_year_col(
    df=silver_ord_parsed,
    date_col="order_date",
    year_col_name="year"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Orders

# COMMAND ----------

# Join Orders with Customers
enriched_df = join_dataframes(
    left_df=silver_ord_parsed,
    right_df=silver_cust,
    join_on="customer_id",
    join_type="left"
)

# Join with Products
enriched_df = join_dataframes(
    left_df=enriched_df,
    right_df=silver_prod,
    join_on="product_id",
    join_type="left"
)

# Calculate Metric (Round Profit)
enriched_df = calculate_metric(
    df=enriched_df,
    metric_col="profit",
    round_places=2
)

# Select Columns
# Updated to snake_case
final_columns = [
    "order_id", 
    "order_date", 
    "profit", 
    "customer_name", 
    "country", 
    "category", 
    "sub_category",
    "year"
]
enriched_df = enriched_df.select(final_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

# Write Cleansed Dimensions to Silver
write_data(
    df=silver_cust, 
    mode="overwrite", 
    table_name=silver_customers_table
)
write_data(
    df=silver_prod, 
    mode="overwrite", 
    table_name=silver_products_table
)

# Write Enriched Data to Silver
write_data(
    df=enriched_df, 
    mode="overwrite", 
    table_name=silver_enriched_orders_table, 
    partition_by=["year"]
)

print("Enrichment Complete.")
