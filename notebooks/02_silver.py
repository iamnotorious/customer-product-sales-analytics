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

import sys
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

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
    """Read all bronze layer tables."""
    cust = read_data(spark=spark_session, table_name=bronze_customers_table)
    prod = read_data(spark=spark_session, table_name=bronze_products_table)
    ord_ = read_data(spark=spark_session, table_name=bronze_orders_table)
    return cust, prod, ord_

def standardize_schema(*, df: DataFrame) -> DataFrame:
    """Standardize column names to snake_case."""
    return to_snake_case(df=df)

def transform_customers(*, df: DataFrame) -> DataFrame:
    """Clean customer data and generate surrogate key."""
    cleaned = clean_dataset(
        df=df, 
        clean_text_cols=["customer_name"], 
        handle_null_cols=["country", "city", "state", "region"],
        null_fill_value="N/A"
    )
    return generate_surrogate_key(df=cleaned, key_columns=["customer_id"], sk_column_name="customer_key")

def transform_products(*, df: DataFrame) -> DataFrame:
    """Clean product data and generate surrogate key."""
    cleaned = clean_dataset(
        df=df, 
        clean_text_cols=["product_name"], 
        handle_null_cols=["category", "sub_category"],
        null_fill_value="N/A"
    )
    return generate_surrogate_key(df=cleaned, key_columns=["product_id"], sk_column_name="product_key")

def transform_orders(*, df: DataFrame) -> DataFrame:
    """Clean orders and parse dates."""
    cleaned = clean_dataset(
        df=df,
        mandatory_cols=["order_id", "customer_id", "product_id"]
    )
    parsed = parse_date_col(df=cleaned, date_col="order_date", date_format="d/M/y")
    return parse_date_col(df=parsed, date_col="ship_date", date_format="d/M/y")

def filter_orders_by_date(*, df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Filter orders by date range. If dates are empty, return all data."""
    if start_date and end_date:
        logger.info(f"Filtering orders: {start_date} to {end_date}")
        return df.filter(
            (F.col("order_date") >= start_date) & (F.col("order_date") <= end_date)
        )
    logger.info("No date filter applied — processing all orders.")
    return df

def build_fact_table(*, orders: DataFrame) -> DataFrame:
    """
    Build the fact table with surrogate keys.
    Since surrogate keys are deterministic (MD5 hash), they can be
    generated directly without joining to dimension tables.
    """
    enriched = generate_surrogate_key(df=orders, key_columns=["customer_id"], sk_column_name="customer_key")
    enriched = generate_surrogate_key(df=enriched, key_columns=["product_id"], sk_column_name="product_key")
    enriched = enriched.withColumn("profit", F.round(F.col("profit"), 2))
    
    fact_columns = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "customer_key", "product_key",
        "quantity", "price", "discount", "profit"
    ]
    return enriched.select(*[c for c in fact_columns if c in enriched.columns])

def build_enriched_orders(*, fact_df: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """
    Create denormalized enriched orders by joining fact table with dimensions.
    Contains: order info, profit (rounded), customer name & country, product category & sub-category.
    """
    # Join fact with customers on surrogate key
    enriched = join_dataframes(
        left_df=fact_df,
        right_df=customers.select("customer_key", "customer_name", "country"),
        join_on="customer_key",
        join_type="left"
    )
    # Join with products on surrogate key
    enriched = join_dataframes(
        left_df=enriched,
        right_df=products.select("product_key", "category", "sub_category"),
        join_on="product_key",
        join_type="left"
    )
    # Add year derived from order_date
    enriched = enriched.withColumn("order_year", F.date_trunc("year", F.col("order_date")))
    
    enriched_columns = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "customer_key", "product_key",
        "customer_name", "country", "category", "sub_category",
        "quantity", "price", "discount", "profit", "order_year"
    ]
    return enriched.select(*[c for c in enriched_columns if c in enriched.columns])

# COMMAND ----------

def merge_to_silver(*, cust_df: DataFrame, prod_df: DataFrame, fact_df: DataFrame, enriched_df: DataFrame):
    """
    Write Star Schema tables to Silver layer.
    - dim_customers & dim_products: SCD Type 2 (historical tracking)
    - ft_sales_ecommerce_orders: Fact table partitioned by order_date
    - enriched_orders: Denormalized view for downstream consumption
    """
    spark = SparkSession.getActiveSession()
    
    # --- Dim: Customers (SCD Type 2) ---
    if spark.catalog.tableExists(silver_dim_customers_table):
        logger.info("Applying SCD Type 2 merge for dim_customers...")
        merge_scd_type2(
            df=cust_df, 
            table_name=silver_dim_customers_table, 
            business_keys=["customer_id"],
            compare_columns=["customer_name", "country", "city", "state", "region"]
        )
    else:
        logger.info("Creating dim_customers with SCD Type 2 structure...")
        cust_df_scd = cust_df \
            .withColumn("effective_date", F.to_date(F.current_timestamp())) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        write_data(df=cust_df_scd, mode="overwrite", table_name=silver_dim_customers_table)
    
    # --- Dim: Products (SCD Type 2) ---
    if spark.catalog.tableExists(silver_dim_products_table):
        logger.info("Applying SCD Type 2 merge for dim_products...")
        merge_scd_type2(
            df=prod_df, 
            table_name=silver_dim_products_table, 
            business_keys=["product_id"],
            compare_columns=["category", "sub_category", "product_name", "price_per_product"]
        )
    else:
        logger.info("Creating dim_products with SCD Type 2 structure...")
        prod_df_scd = prod_df \
            .withColumn("effective_date", F.to_date(F.current_timestamp())) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True))
        write_data(df=prod_df_scd, mode="overwrite", table_name=silver_dim_products_table)
    
    # --- Fact: Orders (partition overwrite by order_date) ---
    logger.info("Writing ft_sales_ecommerce_orders (partitioned by order_date)...")
    write_data(
        df=fact_df, 
        mode="overwrite", 
        table_name=silver_ft_orders_table, 
        partition_by=["order_date"]
    )
    
    # --- Enriched Orders (denormalized view, partitioned by order_date) ---
    logger.info("Writing enriched_orders (partitioned by order_date)...")
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
    
    logger.info(f"Date range: start='{start_date}', end='{end_date}'")
    if not start_date or not end_date:
        logger.info("No date range specified — processing all data.")
    
    try:
        # Read Bronze
        logger.info("Reading Bronze layer data...")
        bronze_cust_raw, bronze_prod_raw, bronze_ord_raw = read_bronze_data(spark_session=spark)
        
        # Standardize
        logger.info("Standardizing schemas to snake_case...")
        bronze_cust = standardize_schema(df=bronze_cust_raw)
        bronze_prod = standardize_schema(df=bronze_prod_raw)
        bronze_ord = standardize_schema(df=bronze_ord_raw)
        
        # Validate order schema
        required_order_cols = ["order_id", "customer_id", "product_id", "order_date", "profit"]
        if not validate_schema(df=bronze_ord, required_columns=required_order_cols):
            raise DataTransformationError("Order data missing required columns")
        
        # Transform dimensions (with surrogate keys)
        logger.info("Transforming dimension data with surrogate keys...")
        silver_cust = transform_customers(df=bronze_cust)
        silver_prod = transform_products(df=bronze_prod)
        
        # Transform orders and apply date filter
        logger.info("Transforming order data...")
        silver_ord = transform_orders(df=bronze_ord)
        silver_ord = filter_orders_by_date(df=silver_ord, start_date=start_date, end_date=end_date)
        

        
        # Build fact table with surrogate keys
        logger.info("Building fact table with surrogate keys...")
        fact_df = build_fact_table(orders=silver_ord)
        
        # Build enriched orders (denormalized: joins fact + dims)
        logger.info("Building enriched orders (fact + dim joins)...")
        enriched_df = build_enriched_orders(fact_df=fact_df, customers=silver_cust, products=silver_prod)
        
        logger.info("Star Schema enrichment completed successfully")
        
        # Add audit columns
        silver_cust = add_audit_columns(df=silver_cust)
        silver_prod = add_audit_columns(df=silver_prod)
        fact_df = add_audit_columns(df=fact_df)
        enriched_df = add_audit_columns(df=enriched_df)
        
        # Write Star Schema tables + enriched view
        merge_to_silver(cust_df=silver_cust, prod_df=silver_prod, fact_df=fact_df, enriched_df=enriched_df)
        
        # Optimize tables with Z-ORDER
        logger.info("Optimizing Silver tables...")
        
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
        
        logger.info("Silver layer Star Schema completed successfully.")
        logger.info("SCD Type 2 applied to dim_customers and dim_products.")
        logger.info("ft_sales_ecommerce_orders partitioned by order_date.")
        logger.info("enriched_orders denormalized view created.")
        
    except Exception as e:
        logger.error(f"Error in Silver layer processing: {e}")
        raise
