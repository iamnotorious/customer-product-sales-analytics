# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

# COMMAND ----------

# Install local package
import sys
import os

# Import libraries
from pyspark.sql import SparkSession, DataFrame
from sales_analytics.utils import get_spark_session, read_data, write_data
from sales_analytics.aggregation import create_aggregates

# Configuration
silver_enriched_orders_table = "sales.silver.sales_ecommerce_enriched_orders"
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
    from pyspark.sql import SparkSession
    from sales_analytics.utils import merge_data
    
    spark = SparkSession.getActiveSession()
    
    if spark.catalog.tableExists(gold_profit_aggregates_table):
        print("Merging profit aggregates to gold layer...")
        # Merge on dimensional keys (year, category, sub_category, customer_name)
        merge_data(
            df=df, 
            table_name=gold_profit_aggregates_table, 
            merge_keys=["year", "category", "sub_category", "customer_name"],
            update_columns=["total_profit"]  # Update only the metric
        )
    else:
        print("Creating profit aggregates gold table...")
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
    print("Generating Analysis Outputs...")
    
    # 1. Profit by Year
    print("--- Profit by Year ---")
    spark_session.sql(f"""
        SELECT year, round(sum(total_profit), 2) as annual_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY year 
        ORDER BY year
    """).show()
    
    # 2. Profit by Year + Product Category
    print("--- Profit by Year + Product Category ---")
    spark_session.sql(f"""
        SELECT year, category, round(sum(total_profit), 2) as category_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY year, category 
        ORDER BY year, category
    """).show()
    
    # 3. Profit by Customer
    print("--- Profit by Customer ---")
    spark_session.sql(f"""
        SELECT customer_name, round(sum(total_profit), 2) as customer_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY customer_name 
        ORDER BY customer_profit DESC
    """).show()
    
    # 4. Profit by Customer + Year
    print("--- Profit by Customer + Year ---")
    spark_session.sql(f"""
        SELECT customer_name, year, round(sum(total_profit), 2) as customer_annual_profit 
        FROM {gold_profit_aggregates_table} 
        GROUP BY customer_name, year 
        ORDER BY customer_name, year
    """).show()

# Execution
if __name__ == "__main__":
    from sales_analytics.exceptions import DataWriteError
    from sales_analytics.validation import validate_data_range
    
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")
    
    try:
        # Read
        print("Reading Silver enriched data...")
        enriched_df = read_silver_data(spark_session=spark)
        print(f"Loaded {enriched_df.count()} enriched order records")
        
        # Calculate aggregates
        print("Calculating profit aggregates...")
        gold_aggregates = calculate_profit_aggregates(df=enriched_df)
        
        # Validate aggregates
        print("Validating aggregate data...")
        agg_count = gold_aggregates.count()
        print(f"Generated {agg_count} aggregate records")
        
        # Write with incremental merge
        merge_to_gold(df=gold_aggregates)
        
        # Perform Analysis
        print("\n" + "="*60)
        print("ANALYSIS OUTPUTS")
        print("="*60 + "\n")
        perform_analysis_output(spark_session=spark)

        print("\nGold layer aggregation and analysis completed successfully.")
        print("Incremental merge strategy applied to gold aggregates table.")
        
    except Exception as e:
        print(f"ERROR in Gold layer processing: {e}")
        raise
