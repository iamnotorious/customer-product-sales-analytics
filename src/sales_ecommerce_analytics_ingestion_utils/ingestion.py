from pyspark.sql import SparkSession, DataFrame
from sales_ecommerce_analytics_ingestion_utils.config import Paths, Schemas

def ingest_customers(spark: SparkSession, source_path: str = None) -> DataFrame:
    """
    Ingests Customer data from Excel.
    Note: Spark doesn't support Excel natively without 'com.crealytics.spark.excel'.
    Assuming this library is available in the Databricks cluster.
    """
    path = source_path or Paths.CUSTOMER_SOURCE
    try:
        # Using com.crealytics.spark.excel
        return spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(path)
    except Exception as e:
        print(f"Error reading Excel. If library missing, might need pandas fallback (not implemented here per PySpark requirement). Error: {e}")
        raise e

def ingest_products(spark: SparkSession, source_path: str = None) -> DataFrame:
    """
    Ingests Products data from CSV.
    """
    path = source_path or Paths.PRODUCT_SOURCE
    # CSV might have custom options like header=True
    return spark.read.format("csv") \
        .option("header", "true") \
        .schema(Schemas.PRODUCT_SCHEMA) \
        .load(path)

def ingest_orders(spark: SparkSession, source_path: str = None) -> DataFrame:
    """
    Ingests Orders data from JSON.
    """
    path = source_path or Paths.ORDER_SOURCE
    # JSON is usually multiline or line-delimited. Inspecting the file (it was a list of dicts), 
    # so multiLine option might be needed.
    return spark.read.format("json") \
        .option("multiLine", "true") \
        .schema(Schemas.ORDER_SCHEMA) \
        .load(path)
