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

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from sales_analytics.utils import get_spark_session, read_data, write_data
from sales_analytics.transformation import (
    to_snake_case, 
    clean_dataset, 
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

def read_bronze_data(*, spark_session: SparkSession):
    cust = read_data(spark=spark_session, table_name=bronze_customers_table)
    prod = read_data(spark=spark_session, table_name=bronze_products_table)
    ord_ = read_data(spark=spark_session, table_name=bronze_orders_table)
    return cust, prod, ord_

def standardize_schema(*, df: DataFrame) -> DataFrame:
    return to_snake_case(df=df)

def transform_customers(*, df: DataFrame) -> DataFrame:
    return clean_dataset(
        df=df, 
        clean_text_cols=["customer_name"], 
        handle_null_cols=["country", "city", "state", "region"],
        null_fill_value="N/A"
    )

def transform_products(*, df: DataFrame) -> DataFrame:
    return clean_dataset(
        df=df, 
        clean_text_cols=["product_name"], 
        handle_null_cols=["category", "sub_category"],
        null_fill_value="N/A"
    )

def transform_orders(*, df: DataFrame) -> DataFrame:
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

def enrich_order_data(*, orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
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

def merge_to_silver(*, cust_df: DataFrame, prod_df: DataFrame, enriched_df: DataFrame):
    """
    Incrementally merge data to Silver layer.
    - Customers & Products: SCD Type 2 (historical tracking)
    - Orders: Upsert on order_id, partitioned by order_date
    """
    from pyspark.sql import SparkSession
    from sales_analytics.utils import merge_data, merge_scd_type2
    
    spark = SparkSession.getActiveSession()
    
    # SCD Type 2: Customers (track historical changes in customer attributes)
    if spark.catalog.tableExists(silver_customers_table):
        print("Applying SCD Type 2 merge for customers (historical tracking)...")
        merge_scd_type2(
            df=cust_df, 
            table_name=silver_customers_table, 
            business_keys=["customer_id"],
            compare_columns=["customer_name", "country", "city", "state", "region"]
        )
    else:
        print("Creating customers silver table with SCD Type 2 structure...")
        from pyspark.sql.functions import lit, current_timestamp, to_date
        cust_df_scd = cust_df \
            .withColumn("effective_date", to_date(current_timestamp())) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True))
        write_data(df=cust_df_scd, mode="overwrite", table_name=silver_customers_table)
    
    # SCD Type 2: Products (track historical changes in product attributes, especially price)
    if spark.catalog.tableExists(silver_products_table):
        print("Applying SCD Type 2 merge for products (historical tracking)...")
        merge_scd_type2(
            df=prod_df, 
            table_name=silver_products_table, 
            business_keys=["product_id"],
            compare_columns=["category", "sub_category", "product_name", "price_per_product"]
        )
    else:
        print("Creating products silver table with SCD Type 2 structure...")
        from pyspark.sql.functions import lit, current_timestamp, to_date
        prod_df_scd = prod_df \
            .withColumn("effective_date", to_date(current_timestamp())) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True))
        write_data(df=prod_df_scd, mode="overwrite", table_name=silver_products_table)
    
    # Fact Table: Enriched Orders (upsert, partitioned by order_date for query performance)
    if spark.catalog.tableExists(silver_enriched_orders_table):
        print("Merging enriched orders (fact table, partitioned by order_date)...")
        merge_data(df=enriched_df, table_name=silver_enriched_orders_table, merge_keys=["order_id"])
    else:
        print("Creating enriched orders silver table (partitioned by order_date)...")
        write_data(
            df=enriched_df, 
            mode="overwrite", 
            table_name=silver_enriched_orders_table, 
            partition_by=["order_date"]
        )

# Execution
if __name__ == "__main__":
    from sales_analytics.exceptions import DataTransformationError, DataWriteError
    from sales_analytics.validation import validate_schema, check_null_percentage
    
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_ENRICHMENT_JOB")
    
    try:
        # Read
        print("Reading Bronze layer data...")
        bronze_cust_raw, bronze_prod_raw, bronze_ord_raw = read_bronze_data(spark_session=spark)
        
        # Standardize
        print("Standardizing schemas to snake_case...")
        bronze_cust = standardize_schema(df=bronze_cust_raw)
        bronze_prod = standardize_schema(df=bronze_prod_raw)
        bronze_ord = standardize_schema(df=bronze_ord_raw)
        
        # Validate schemas
        required_order_cols = ["order_id", "customer_id", "product_id", "order_date", "profit"]
        if not validate_schema(df=bronze_ord, required_columns=required_order_cols):
            raise DataTransformationError("Order data missing required columns")
        
        # Transform
        print("Transforming data...")
        silver_cust = transform_customers(df=bronze_cust)
        silver_prod = transform_products(df=bronze_prod)
        silver_ord_parsed = transform_orders(df=bronze_ord)
        
        # Validate transformed data
        print("Validating transformed data quality...")
        check_null_percentage(df=silver_ord_parsed, column="order_date", threshold=0.1)
        check_null_percentage(df=silver_ord_parsed, column="profit", threshold=0.1)
        
        # Enrich
        print("Enriching order data with customer and product information...")
        enriched_df = enrich_order_data(orders=silver_ord_parsed, customers=silver_cust, products=silver_prod)
        
        # Final validation
        print(f"Enriched dataset created with {enriched_df.count()} records")
        
        # Write with SCD Type 2 for dimensions and partitioned orders
        merge_to_silver(cust_df=silver_cust, prod_df=silver_prod, enriched_df=enriched_df)
        
        print("Silver layer transformation completed successfully. All data quality checks passed.")
        print("SCD Type 2 applied to Customers and Products for historical tracking.")
        print("Orders partitioned by order_date for optimal query performance.")
        
    except Exception as e:
        print(f"ERROR in Silver layer processing: {e}")
        raise
