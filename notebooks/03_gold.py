# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

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

logger = logging.getLogger(__name__)
from sales_analytics.utils import get_spark_session, read_data, write_data, optimize_table
from sales_analytics.aggregation import create_aggregates
from sales_analytics.transformation import add_audit_columns

# Configuration
silver_enriched_orders_table = "sales.silver.enriched_orders"
gold_profit_aggregates_table = "sales.gold.sales_ecommerce_profit_aggregates"

def read_silver_data(*, spark_session: SparkSession) -> DataFrame:
    return read_data(spark=spark_session, table_name=silver_enriched_orders_table)

def calculate_profit_aggregates(*, df: DataFrame) -> DataFrame:
    return create_aggregates(
        df=df,
        group_by_cols=["order_year", "category", "sub_category", "customer_name"],
        agg_col="profit",
        alias_col="total_profit",
        round_places=2
    )

def merge_to_gold(*, df: DataFrame):
    """Write aggregates to Gold (Partition Overwrite)."""
    logger.info("Writing Aggregates (partitioned)...")
    write_data(
        df=df, 
        mode="overwrite", 
        table_name=gold_profit_aggregates_table,
        partition_by=["order_year"]
    )

def perform_analysis_output(*, spark_session: SparkSession):
    """Output analysis via SQL."""
    logger.info("Generating Analysis")
    
    # 1. Profit by Year
    logger.info("--- Profit/Year ---")
    spark_session.sql(f"""
        SELECT order_year, round(sum(total_profit), 2) as annual_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY order_year 
        ORDER BY order_year
    """).show()
    
    # 2. Profit by Year + Product Category
    logger.info("--- Profit/Year + Category ---")
    spark_session.sql(f"""
        SELECT order_year, category, round(sum(total_profit), 2) as category_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY order_year, category 
        ORDER BY order_year, category
    """).show()
    
    # 3. Profit by Customer
    logger.info("--- Profit/Customer ---")
    spark_session.sql(f"""
        SELECT customer_name, round(sum(total_profit), 2) as customer_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY customer_name 
        ORDER BY customer_profit DESC
    """).show()
    
    # 4. Profit by Customer + Year
    logger.info("--- Profit/Customer + Year ---")
    spark_session.sql(f"""
        SELECT customer_name, order_year, round(sum(total_profit), 2) as customer_annual_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY customer_name, order_year 
        ORDER BY customer_name, order_year
    """).show()

# Execution

# COMMAND ----------

if __name__ == "__main__":
    
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")
    
    try:
        # Read
        logger.info("Reading Silver data")
        enriched_df = read_silver_data(spark_session=spark)
        logger.info("Silver data loaded")
        
        # Calculate aggregates
        # Calculate aggregates
        logger.info("Aggregating profit")
        gold_aggregates = calculate_profit_aggregates(df=enriched_df)

        
        # Add audit columns
        gold_aggregates = add_audit_columns(df=gold_aggregates)

        # Write with partitioned overwrite
        merge_to_gold(df=gold_aggregates)

        # Optimize with Z-ORDER
        logger.info("Optimizing Gold")
        optimize_table(table_name=gold_profit_aggregates_table, zorder_columns=["customer_name", "category"])
        
        # Perform Analysis
        logger.info("=" * 60)
        logger.info("ANALYSIS OUTPUTS")
        logger.info("=" * 60)
        perform_analysis_output(spark_session=spark)

        logger.info("Gold layer complete")
        logger.info("Partition overwrite applied")
        
    except Exception as e:
        logger.error(f"Error in Gold layer processing: {e}")
        raise
