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

def read_bronze_data(spark_session: SparkSession):
    cust = read_data(spark=spark_session, table_name=bronze_customers_table)
    prod = read_data(spark=spark_session, table_name=bronze_products_table)
    ord_ = read_data(spark=spark_session, table_name=bronze_orders_table)
    return cust, prod, ord_

def standardize_schema(df: DataFrame) -> DataFrame:
    return to_snake_case(df=df)

def transform_customers(df: DataFrame) -> DataFrame:
    return clean_dataset(
        df=df, 
        clean_text_cols=["customer_name"], 
        handle_null_cols=["country", "city", "state", "region"],
        null_fill_value="N/A"
    )

def transform_products(df: DataFrame) -> DataFrame:
    return clean_dataset(
        df=df, 
        clean_text_cols=["product_name"], 
        handle_null_cols=["category", "sub_category"],
        null_fill_value="N/A"
    )

def transform_orders(df: DataFrame) -> DataFrame:
    cleaned = clean_dataset(
        df=df,
        mandatory_cols=["order_id", "customer_id", "product_id"]
    )
    parsed = parse_date_col(
        df=cleaned, 
        date_col="order_date", 
        date_format="d/M/y"
    )
    return add_year_col(
        df=parsed,
        date_col="order_date",
        year_col_name="year"
    )

def enrich_order_data(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    # Join Orders with Customers
    enriched = join_dataframes(
        left_df=orders,
        right_df=customers,
        join_on="customer_id",
        join_type="left"
    )
    # Join with Products
    enriched = join_dataframes(
        left_df=enriched,
        right_df=products,
        join_on="product_id",
        join_type="left"
    )
    # Calculate Metric (Round Profit)
    enriched = calculate_metric(
        df=enriched,
        metric_col="profit",
        round_places=2
    )
    
    final_columns = [
        "order_id", "order_date", "profit", "customer_name", 
        "country", "category", "sub_category", "year"
    ]
    return enriched.select(final_columns)

def write_to_silver(cust_df: DataFrame, prod_df: DataFrame, enriched_df: DataFrame):
    # Write Cleansed Dimensions
    write_data(df=cust_df, mode="overwrite", table_name=silver_customers_table)
    write_data(df=prod_df, mode="overwrite", table_name=silver_products_table)
    # Write Enriched Data
    write_data(df=enriched_df, mode="overwrite", table_name=silver_enriched_orders_table, partition_by=["year"])

# Execution
if __name__ == "__main__":
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_ENRICHMENT_JOB")
    
    # Read
    bronze_cust_raw, bronze_prod_raw, bronze_ord_raw = read_bronze_data(spark)
    
    # Standardize
    bronze_cust = standardize_schema(bronze_cust_raw)
    bronze_prod = standardize_schema(bronze_prod_raw)
    bronze_ord = standardize_schema(bronze_ord_raw)
    
    # Transform
    silver_cust = transform_customers(bronze_cust)
    silver_prod = transform_products(bronze_prod)
    silver_ord_parsed = transform_orders(bronze_ord)
    
    # Enrich
    enriched_df = enrich_order_data(silver_ord_parsed, silver_cust, silver_prod)
    
    # Write
    write_to_silver(silver_cust, silver_prod, enriched_df)
    
    print("Enrichment Complete.")
