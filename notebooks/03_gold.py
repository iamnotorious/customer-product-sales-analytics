# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

# COMMAND ----------

# Import libraries
import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)
from sales_analytics.utils import get_spark_session, read_data, write_data, merge_data
from sales_analytics.aggregation import create_aggregates
from sales_analytics.exceptions import DataWriteError
from sales_analytics.validation import validate_data_range
from sales_analytics.transformation import add_audit_columns

# Configuration
silver_enriched_orders_table = "sales.silver.ft_enriched_orders"
gold_profit_aggregates_table = "sales.gold.sales_ecommerce_profit_aggregates"

def read_silver_data(*, spark_session: SparkSession) -> DataFrame:
    return read_data(spark=spark_session, table_name=silver_enriched_orders_table)

def calculate_profit_aggregates(*, df: DataFrame) -> DataFrame:
    return create_aggregates(
        df=df,
        group_by_cols=["year", "category", "sub_category", "customer_name"],
        agg_col="profit",
        alias_col="total_profit",
        round_places=2
    )

def merge_to_gold(*, df: DataFrame):
    """
    Incrementally merge aggregates to Gold layer.
    For aggregate tables, we use merge to update existing aggregates and add new ones.
    """
    spark = SparkSession.getActiveSession()
    
    if spark.catalog.tableExists(gold_profit_aggregates_table):
        logger.info("Merging profit aggregates to gold layer...")
        # Merge on dimensional keys (year, category, sub_category, customer_name)
        merge_data(
            df=df, 
            table_name=gold_profit_aggregates_table, 
            merge_keys=["year", "category", "sub_category", "customer_name"],
            update_columns=["total_profit"]  # Update only the metric
        )
    else:
        logger.info("Creating profit aggregates gold table...")
        write_data(
            df=df, 
            mode="overwrite", 
            table_name=gold_profit_aggregates_table,
            partition_by=["year"]
        )

def perform_analysis_output(*, spark_session: SparkSession):
    """
    Outputs the requested aggregates using SQL on the created gold table.
    """
    logger.info("Generating Analysis Outputs...")
    
    # 1. Profit by Year
    logger.info("--- Profit by Year ---")
    spark_session.sql(f"""
        SELECT year, round(sum(total_profit), 2) as annual_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY year 
        ORDER BY year
    """).show()
    
    # 2. Profit by Year + Product Category
    logger.info("--- Profit by Year + Product Category ---")
    spark_session.sql(f"""
        SELECT year, category, round(sum(total_profit), 2) as category_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY year, category 
        ORDER BY year, category
    """).show()
    
    # 3. Profit by Customer
    logger.info("--- Profit by Customer ---")
    spark_session.sql(f"""
        SELECT customer_name, round(sum(total_profit), 2) as customer_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY customer_name 
        ORDER BY customer_profit DESC
    """).show()
    
    # 4. Profit by Customer + Year
    logger.info("--- Profit by Customer + Year ---")
    spark_session.sql(f"""
        SELECT customer_name, year, round(sum(total_profit), 2) as customer_annual_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY customer_name, year 
        ORDER BY customer_name, year
    """).show()

# Execution

# COMMAND ----------

if __name__ == "__main__":
    
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")
    
    try:
        # Read
        logger.info("Reading Silver enriched data...")
        enriched_df = read_silver_data(spark_session=spark)
        logger.info("Loaded enriched order records from Silver layer")
        
        # Calculate aggregates
        logger.info("Calculating profit aggregates...")
        gold_aggregates = calculate_profit_aggregates(df=enriched_df)
        
        # Validate aggregates
        logger.info("Validating aggregate data...")
        logger.info("Aggregate records generated successfully")
        
        # Add audit columns
        gold_aggregates = add_audit_columns(df=gold_aggregates)

        # Write with incremental merge
        merge_to_gold(df=gold_aggregates)
        
        # Perform Analysis
        logger.info("=" * 60)
        logger.info("ANALYSIS OUTPUTS")
        logger.info("=" * 60)
        perform_analysis_output(spark_session=spark)

        logger.info("Gold layer aggregation and analysis completed successfully.")
        logger.info("Incremental merge strategy applied to gold aggregates table.")
        
    except Exception as e:
        logger.error(f"Error in Gold layer processing: {e}")
        raise
