# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

# COMMAND ----------

# Install local package
import sys
import os

# Add the src directory to path
sys.path.append("/Workspace/Repos/sales_analytics/customer-product-sales-analytics/src")

from sales_ecommerce_analytics_ingestion_utils.utils import get_spark_session, read_data, write_data
from sales_ecommerce_analytics_ingestion_utils.aggregation import create_aggregates

# Configuration
silver_enriched_orders_table = "sales.silver.sales_ecommerce_enriched_orders"
gold_profit_aggregates_table = "sales.gold.sales_ecommerce_profit_aggregates"

def read_silver_data(spark_session: SparkSession) -> DataFrame:
    return read_data(spark=spark_session, table_name=silver_enriched_orders_table)

def calculate_profit_aggregates(df: DataFrame) -> DataFrame:
    return create_aggregates(
        df=df,
        group_by_cols=["year", "category", "sub_category", "customer_name"],
        agg_col="profit",
        alias_col="total_profit",
        round_places=2
    )

def write_to_gold(df: DataFrame):
    write_data(
        df=df, 
        mode="overwrite", 
        table_name=gold_profit_aggregates_table,
        partition_by=["year"]
    )

# Execution
if __name__ == "__main__":
    spark = get_spark_session(app_name="SALES_ECOMMERCE_ANALYTICS_AGGREGATION_JOB")
    
    enriched_df = read_silver_data(spark)
    gold_aggregates = calculate_profit_aggregates(enriched_df)
    write_to_gold(gold_aggregates)

    print("Aggregation Complete.")
