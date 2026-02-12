# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Cleanup / Reset Environment
# MAGIC 
# MAGIC This notebook drops all tables in Bronze, Silver, and Gold layers to allow for a fresh run of the pipeline.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# List of tables to drop
tables_to_drop = [
    # Gold Layer
    "sales.gold.sales_ecommerce_profit_aggregates",
    
    # Silver Layer
    "sales.silver.enriched_orders",
    "sales.silver.ft_sales_ecommerce_orders",
    "sales.silver.ft_enriched_orders", # Dropping old name if exists
    "sales.silver.dim_customers",
    "sales.silver.dim_products",
    
    # Bronze Layer
    "sales.bronze.sales_ecommerce_customers",
    "sales.bronze.sales_ecommerce_products",
    "sales.bronze.sales_ecommerce_orders"
]

print("Starting cleanup...")

for table in tables_to_drop:
    print(f"Dropping table: {table}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("Cleanup completed. All specified tables have been dropped.")
