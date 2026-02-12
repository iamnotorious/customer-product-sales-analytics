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
dbutils.widgets.text("start_date", "", "Start Date (yyyy-MM-dd)")
dbutils.widgets.text("end_date", "", "End Date (yyyy-MM-dd)")

# COMMAND ----------

import os
import sys

# Dynamically find and append the 'src' directory
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

from sales_analytics.utils import get_spark_session, read_data, write_data, merge_data, merge_scd_type2, optimize_table
from sales_analytics.exceptions import DataTransformationError, DataWriteError
from sales_analytics.validation import validate_schema
from sales_analytics.transformation import (
    to_snake_case, 
    clean_dataset, 
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
    """Read Bronze tables."""
    cust = read_data(spark=spark_session, table_name=bronze_customers_table)
    prod = read_data(spark=spark_session, table_name=bronze_products_table)
    ord_ = read_data(spark=spark_session, table_name=bronze_orders_table)
    return cust, prod, ord_

def standardize_schema(*, df: DataFrame) -> DataFrame:
    """Standardize columns to snake_case."""
    return to_snake_case(df=df)

def transform_customers(*, df: DataFrame) -> DataFrame:
    """Clean customers & add surrogate key."""
    cleaned = clean_dataset(
        df=df, 
        clean_text_cols=["customer_name"], 
        handle_null_cols=["country", "city", "state", "region"],
        null_fill_value="N/A"
    )
    return generate_surrogate_key(df=cleaned, key_columns=["customer_id"], sk_column_name="customer_key")

def transform_products(*, df: DataFrame) -> DataFrame:
    """Clean products & add surrogate key."""
    cleaned = clean_dataset(
        df=df, 
        clean_text_cols=["product_name"], 
        handle_null_cols=["category", "sub_category"],
        null_fill_value="N/A"
    )
    return generate_surrogate_key(df=cleaned, key_columns=["product_id"], sk_column_name="product_key")

def transform_orders(*, df: DataFrame) -> DataFrame:
    """Clean orders & parse dates."""
    cleaned = clean_dataset(
        df=df,
        mandatory_cols=["order_id", "customer_id", "product_id"]
    )
    parsed = parse_date_col(df=cleaned, date_col="order_date", date_format="d/M/y")
    return parse_date_col(df=parsed, date_col="ship_date", date_format="d/M/y")

def filter_orders_by_date(*, df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Filter orders by date range."""
    if start_date and end_date:
        logger.info(f"Filtering: {start_date} to {end_date}")
        return df.filter(
            (F.col("order_date") >= start_date) & (F.col("order_date") <= end_date)
        )
    logger.info("Processing all orders")
    return df

def build_fact_table(*, orders: DataFrame) -> DataFrame:
    """Build fact table (deterministic SK generation)."""
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
    """Create denormalized view (Fact + Dims)."""
    # Join fact with customers on surrogate key (Broadcast)
    enriched = join_dataframes(
        left_df=fact_df,
        right_df=customers.select("customer_key", "customer_name", "country"),
        join_on="customer_key",
        join_type="left",
        broadcast_right=True
    )
    # Join with products on surrogate key (Broadcast)
    enriched = join_dataframes(
        left_df=enriched,
        right_df=products.select("product_key", "category", "sub_category"),
        join_on="product_key",
        join_type="left",
        broadcast_right=True
    )
    # Add year derived from order_date (integer for clean reporting)
    enriched = enriched.withColumn("order_year", F.year(F.col("order_date")))
    
    # Handle NULLs from failed joins
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
    """Write to Silver (SCD2 & Partition Overwrite)."""
    spark = SparkSession.getActiveSession()
    
    # --- Dim: Customers (SCD Type 2) ---
    if spark.catalog.tableExists(silver_dim_customers_table):
        logger.info("Merging Customers (SCD2)...")
        merge_scd_type2(
            df=cust_df, 
            table_name=silver_dim_customers_table, 
            business_keys=["customer_id"],
            compare_columns=["customer_name", "country", "city", "state", "region"]
        )
    else:
        logger.info("Creating Customers (SCD2)...")
        cust_df_scd = cust_df \
            .withColumn("effective_date", F.to_date(F.current_timestamp())) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        write_data(df=cust_df_scd, mode="overwrite", table_name=silver_dim_customers_table)
    
    # --- Dim: Products (SCD Type 2) ---
    if spark.catalog.tableExists(silver_dim_products_table):
        logger.info("Merging Products (SCD2)...")
        merge_scd_type2(
            df=prod_df, 
            table_name=silver_dim_products_table, 
            business_keys=["product_id"],
            compare_columns=["category", "sub_category", "product_name", "price_per_product"]
        )
    else:
        logger.info("Creating Products (SCD2)...")
        prod_df_scd = prod_df \
            .withColumn("effective_date", F.to_date(F.current_timestamp())) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        write_data(df=prod_df_scd, mode="overwrite", table_name=silver_dim_products_table)
    
    # --- Fact: Orders (partition overwrite by order_date) ---
    logger.info("Writing Fact table (partitioned)...")
    write_data(
        df=fact_df, 
        mode="overwrite", 
        table_name=silver_ft_orders_table, 
        partition_by=["order_date"]
    )
    
    # --- Enriched Orders (denormalized view, partitioned by order_date) ---
    logger.info("Writing Enriched Orders (partitioned)...")
    write_data(
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
        
        # Validate order schema
        required_order_cols = ["order_id", "customer_id", "product_id", "order_date", "profit"]
        if not validate_schema(df=bronze_ord, required_columns=required_order_cols):
            raise DataTransformationError("Order data missing required columns")
        
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
        
        # Cache intermediate dfs (clean versions) to reuse
        silver_cust_clean.cache()
        silver_prod_clean.cache()
        fact_df_clean.cache()
        
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
        logger.info("Optimizing tables...")
        
        opt_where = None
        if start_date and end_date:
            opt_where = f"order_date >= '{start_date}' AND order_date <= '{end_date}'"
            
        optimize_table(
            table_name=silver_ft_orders_table, 
            zorder_columns=["customer_key", "product_key"],
            where=opt_where
        )
        optimize_table(
            table_name=silver_enriched_orders_table, 
            zorder_columns=["customer_name", "category"],
            where=opt_where
        )
        
        logger.info("Silver layer complete")
        logger.info("SCD2 applied")
        logger.info("Fact table partitioned")
        logger.info("Enriched view created")
        
        # Unpersist cached DataFrames
        silver_cust_clean.unpersist()
        silver_prod_clean.unpersist()
        fact_df_clean.unpersist()
        
    except Exception as e:
        logger.error(f"Error in Silver layer processing: {e}")
        raise
