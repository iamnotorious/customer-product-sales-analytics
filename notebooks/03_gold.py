# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Aggregation (Gold Layer)
# MAGIC 
# MAGIC This notebook creates the aggregated tables required for reporting.

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
from pyspark.sql.functions import sum, col, round

logger = logging.getLogger(__name__)
from sales_analytics.utils import get_spark_session, write_data_to_table, optimize_table
from sales_analytics.transformation import add_audit_columns

# Configuration
silver_enriched_orders_table = "sales.silver.enriched_orders"
gold_profit_aggregates_table = "sales.gold.sales_ecommerce_profit_aggregates"

def read_silver_data(*, spark_session: SparkSession) -> DataFrame:
    return spark_session.read.table(silver_enriched_orders_table)

def calculate_profit_aggregates(*, df: DataFrame) -> DataFrame:
    group_cols = ["order_year", "category", "sub_category", "customer_name"]
    return df.groupBy(*group_cols) \
        .agg(round(sum("profit"), 2).alias("total_profit")) \
        .orderBy(*group_cols)

def merge_to_gold(*, df: DataFrame):
    """Write aggregates to Gold (Partition Overwrite)."""
    logger.info("Writing Aggregates")
    write_data_to_table(
        df=df, 
        mode="overwrite", 
        table_name=gold_profit_aggregates_table,
        partition_by=["order_year"]
    )

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
        logger.info("Aggregating profit")
        gold_aggregates = calculate_profit_aggregates(df=enriched_df)
        
        # Add audit columns
        gold_aggregates = add_audit_columns(df=gold_aggregates)

        # Write with partitioned overwrite
        merge_to_gold(df=gold_aggregates)

        # Optimize with Z-ORDER
        logger.info("Optimizing Gold")
        optimize_table(table_name=gold_profit_aggregates_table, zorder_columns=["customer_name", "category"])
        
        logger.info("Gold layer complete")

        
    except Exception as e:
        logger.error(f"Error in Gold layer processing: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

# DBTITLE 1,Profit by Year
# MAGIC %sql
# MAGIC SELECT order_year, round(sum(total_profit), 2) as annual_profit 
# MAGIC FROM sales.gold.sales_ecommerce_profit_aggregates
# MAGIC GROUP BY order_year 
# MAGIC ORDER BY order_year

# COMMAND ----------

# DBTITLE 1,Profit by Year + Category
# MAGIC %sql
# MAGIC SELECT order_year, category, round(sum(total_profit), 2) as category_profit 
# MAGIC FROM sales.gold.sales_ecommerce_profit_aggregates
# MAGIC GROUP BY order_year, category 
# MAGIC ORDER BY order_year, category

# COMMAND ----------

# DBTITLE 1,Profit by Customer
# MAGIC %sql
# MAGIC SELECT customer_name, round(sum(total_profit), 2) as customer_profit 
# MAGIC FROM sales.gold.sales_ecommerce_profit_aggregates
# MAGIC GROUP BY customer_name 
# MAGIC ORDER BY customer_profit DESC

# COMMAND ----------

# DBTITLE 1,Profit by Customer + Year
# MAGIC %sql
# MAGIC SELECT customer_name, order_year, round(sum(total_profit), 2) as customer_annual_profit 
# MAGIC FROM sales.gold.sales_ecommerce_profit_aggregates
# MAGIC GROUP BY customer_name, order_year 
# MAGIC ORDER BY customer_name, order_year
