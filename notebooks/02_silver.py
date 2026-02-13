# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Data Enrichment (Silver Layer) - Star Schema
# MAGIC 
# MAGIC This notebook transforms Bronze data into a **Star Schema**:
# MAGIC - **dim_customers**: SCD Type 2 customer dimension with `customer_key` surrogate key
# MAGIC - **dim_products**: SCD Type 2 product dimension with `product_key` surrogate key
# MAGIC - **ft_enriched_orders**: Fact table with surrogate key FKs, partitioned by `order_date`
# MAGIC 
# MAGIC Key steps:
# MAGIC 1. Cleanse text and handle nulls
# MAGIC 2. Parse dates
# MAGIC 3. Generate surrogate keys for dimensions
# MAGIC 4. Enrich orders with dimension surrogate keys
# MAGIC 5. Write Star Schema tables (SCD Type 2 for dimensions, partition overwrite for fact)

# COMMAND ----------

# Widgets for incremental date-range processing
try:
    dbutils.widgets.text("start_date", "", "Start Date (yyyy-MM-dd)")
    dbutils.widgets.text("end_date", "", "End Date (yyyy-MM-dd)")
except NameError:
    pass

# COMMAND ----------

import os
import sys

# Add src to sys.path
current_dir = os.getcwd()
while current_dir != "/":
    if os.path.exists(os.path.join(current_dir, "src")):
        sys.path.append(os.path.join(current_dir, "src"))
        break
    current_dir = os.path.dirname(current_dir)

# COMMAND ----------

# Import libraries
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

from sales_analytics.utils import get_spark_session, write_data_to_table, merge_data, merge_scd_type2, optimize_table

from sales_analytics.validation import check_duplicates
from sales_analytics.transformation import (
    to_snake_case,
    clean_customer_names,
    clean_customer_phones,
    fill_missing_values,
    deduplicate,
    join_dataframes,
    parse_date_col,
    add_audit_columns,
    generate_surrogate_key
)

# COMMAND ----------

# Configuration
# Bronze source tables
bronze_customers_table = "sales.bronze.sales_ecommerce_customers"
bronze_products_table = "sales.bronze.sales_ecommerce_products"
bronze_orders_table = "sales.bronze.sales_ecommerce_orders"

# Silver Star Schema tables
silver_dim_customers_table = "sales.silver.dim_customers"
silver_dim_products_table = "sales.silver.dim_products"
silver_ft_orders_table = "sales.silver.ft_sales_ecommerce_orders"
silver_enriched_orders_table = "sales.silver.enriched_orders"

# COMMAND ----------

def read_bronze_data(*, spark_session: SparkSession):
    """Load all three Bronze tables into DataFrames."""
    cust = spark_session.read.table(bronze_customers_table)
    prod = spark_session.read.table(bronze_products_table)
    orders_df = spark_session.read.table(bronze_orders_table)
    return cust, prod, orders_df

def standardize_schema(*, df: DataFrame) -> DataFrame:
    """Convert all column names to snake_case."""
    return to_snake_case(df=df)

def transform_customers(*, df: DataFrame) -> DataFrame:
    """Clean names/phones, fill nulls, filter outliers, dedup, and add surrogate key."""
    cleaned = (
        df
        .transform(clean_customer_names)
        .transform(clean_customer_phones)
        .transform(lambda df: fill_missing_values(df, ["country", "city", "state", "region"], "N/A"))
    )
    

    
    # Deduplicate and add surrogate key
    cleaned = deduplicate(df=cleaned, key_columns=["customer_id"])
    return generate_surrogate_key(df=cleaned, key_columns=["customer_id"], sk_column_name="customer_key")

def transform_products(*, df: DataFrame) -> DataFrame:
    """Fill missing categories, dedup by product_id, and add surrogate key."""
    cleaned = df.transform(lambda df: fill_missing_values(df, ["category", "sub_category"], "N/A"))
    
    # Deduplicate and add surrogate key
    cleaned = deduplicate(df=cleaned, key_columns=["product_id"])
    return generate_surrogate_key(df=cleaned, key_columns=["product_id"], sk_column_name="product_key")

def transform_orders(*, df: DataFrame) -> DataFrame:
    """Drop rows missing key IDs and parse order_date/ship_date to DateType."""
    cleaned = df.dropna(subset=["order_id", "customer_id", "product_id"])
    parsed = parse_date_col(df=cleaned, date_col="order_date", date_format="d/M/y")
    return parse_date_col(df=parsed, date_col="ship_date", date_format="d/M/y")

def filter_orders_by_date(*, df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Restrict to a date window. Returns all rows when dates are empty."""
    if start_date and end_date:
        logger.info(f"Filtering: {start_date} to {end_date}")
        return df.filter(
            (F.col("order_date") >= start_date) & (F.col("order_date") <= end_date)
        )
    logger.info("Processing all orders")
    return df

def build_fact_table(*, orders: DataFrame) -> DataFrame:
    """Produce the fact table with surrogate keys, rounded profit, and order_year."""
    enriched = generate_surrogate_key(df=orders, key_columns=["customer_id"], sk_column_name="customer_key")
    enriched = generate_surrogate_key(df=enriched, key_columns=["product_id"], sk_column_name="product_key")
    enriched = enriched.withColumn("profit", F.round(F.col("profit"), 2))
    enriched = enriched.withColumn("order_year", F.year(F.col("order_date")))
    
    fact_columns = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "customer_key", "product_key",
        "quantity", "price", "discount", "profit", "order_year"
    ]
    return enriched.select(*[c for c in fact_columns if c in enriched.columns])

def build_enriched_orders(*, fact_df: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Join fact with customer and product dims into a flat reporting table."""
    enriched = join_dataframes(
        left_df=fact_df,
        right_df=customers.select("customer_key", "customer_name", "country"),
        join_on="customer_key",
        join_type="left",
        broadcast_right=True
    )
    enriched = join_dataframes(
        left_df=enriched,
        right_df=products.select("product_key", "category", "sub_category"),
        join_on="product_key",
        join_type="left",
        broadcast_right=True
    )
    enriched = enriched.withColumn("order_year", F.year(F.col("order_date")))
    # WHY: left join can leave NULLs when a dimension row is missing
    enriched = enriched.fillna("N/A", subset=["customer_name", "country", "category", "sub_category"])
    
    enriched_columns = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "customer_key", "product_key",
        "customer_name", "country", "category", "sub_category",
        "quantity", "price", "discount", "profit", "order_year"
    ]
    return enriched.select(*[c for c in enriched_columns if c in enriched.columns])

# COMMAND ----------

def merge_to_silver(*, cust_df: DataFrame, prod_df: DataFrame, fact_df: DataFrame, enriched_df: DataFrame):
    """Persist Silver tables: SCD2 for dimensions, partition overwrite for facts."""
    spark = SparkSession.getActiveSession()
    
    if spark.catalog.tableExists(silver_dim_customers_table):
        logger.info(f"SCD2 merge: {silver_dim_customers_table}")
        merge_scd_type2(
            df=cust_df, 
            table_name=silver_dim_customers_table, 
            merge_keys=["customer_id"],
            compare_columns=["customer_name", "country", "city", "state", "region"]
        )
    else:
        logger.info(f"Initial load: {silver_dim_customers_table}")
        cust_df_scd = cust_df \
            .withColumn("effective_date", F.to_date(F.current_timestamp())) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        write_data_to_table(df=cust_df_scd, mode="overwrite", table_name=silver_dim_customers_table)
    
    if spark.catalog.tableExists(silver_dim_products_table):
        logger.info(f"SCD2 merge: {silver_dim_products_table}")
        merge_scd_type2(
            df=prod_df, 
            table_name=silver_dim_products_table, 
            merge_keys=["product_id"],
            compare_columns=["category", "sub_category", "product_name", "price_per_product"]
        )
    else:
        logger.info(f"Initial load: {silver_dim_products_table}")
        prod_df_scd = prod_df \
            .withColumn("effective_date", F.to_date(F.current_timestamp())) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        write_data_to_table(df=prod_df_scd, mode="overwrite", table_name=silver_dim_products_table)
    
    logger.info(f"Writing {silver_ft_orders_table}")
    write_data_to_table(
        df=fact_df, 
        mode="overwrite", 
        table_name=silver_ft_orders_table, 
        partition_by=["order_date"]
    )
    
    logger.info(f"Writing {silver_enriched_orders_table}")
    write_data_to_table(
        df=enriched_df, 
        mode="overwrite", 
        table_name=silver_enriched_orders_table, 
        partition_by=["order_date"]
    )
    
# Execution

# COMMAND ----------

if __name__ == "__main__":
    
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_ENRICHMENT_JOB")
    
    # Get widget values
    start_date = dbutils.widgets.get("start_date").strip()
    end_date = dbutils.widgets.get("end_date").strip()
    
    logger.info(f"Date range: {start_date} to {end_date}")
    if not start_date or not end_date:
        logger.info("Full load (no date range)")
    
    try:
        # Read Bronze
        logger.info("Reading Bronze data")
        bronze_cust_raw, bronze_prod_raw, bronze_ord_raw = read_bronze_data(spark_session=spark)
        
        # Standardize
        logger.info("Standardizing schemas")
        bronze_cust = standardize_schema(df=bronze_cust_raw)
        bronze_prod = standardize_schema(df=bronze_prod_raw)
        bronze_ord = standardize_schema(df=bronze_ord_raw)
        

        
        # Transform dimensions (with surrogate keys)
        logger.info("Transforming dimensions")
        silver_cust_clean = transform_customers(df=bronze_cust)
        silver_prod_clean = transform_products(df=bronze_prod)
        
        # Transform orders and apply date filter
        logger.info("Transforming orders")
        silver_ord_clean = transform_orders(df=bronze_ord)
        silver_ord_clean = filter_orders_by_date(df=silver_ord_clean, start_date=start_date, end_date=end_date)
        

        
        # Build fact table with surrogate keys
        logger.info("Building fact table")
        fact_df_clean = build_fact_table(orders=silver_ord_clean)
        
        
        # Build enriched orders (denormalized: joins fact + dims)
        logger.info("Building enriched orders")
        enriched_df = build_enriched_orders(fact_df=fact_df_clean, customers=silver_cust_clean, products=silver_prod_clean)
        
        logger.info("Enrichment complete")
        
        # Add audit columns
        silver_cust_final = add_audit_columns(df=silver_cust_clean)
        silver_prod_final = add_audit_columns(df=silver_prod_clean)
        fact_df_final = add_audit_columns(df=fact_df_clean)
        enriched_df_final = add_audit_columns(df=enriched_df)
        
        # Write Star Schema tables + enriched view
        merge_to_silver(
            cust_df=silver_cust_final, 
            prod_df=silver_prod_final, 
            fact_df=fact_df_final, 
            enriched_df=enriched_df_final
        )
        
        # Optimize tables with Z-ORDER
        optimize_table(
            table_name=silver_ft_orders_table, 
            zorder_columns=["customer_key", "product_key"]
        )
        optimize_table(
            table_name=silver_enriched_orders_table, 
            zorder_columns=["customer_name", "category"]
        )
        
        logger.info("Silver layer complete")
        
    except Exception as e:
        logger.error(f"Error in Silver layer processing: {e}")
        raise
