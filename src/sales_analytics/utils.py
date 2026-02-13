from pyspark.sql import SparkSession, DataFrame
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "DatabricksApp") -> SparkSession:
    """Return a Spark session with AQE and dynamic partition overwrite enabled."""
    logger.info(f"Starting Spark: {app_name}")
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def write_data_to_table(df: DataFrame, table_format: str = "delta", mode: str = "append", table_name: str = None, partition_by: list = None):
    """Save a DataFrame as a Delta table with optional partitioning."""
    logger.info(f"Writing {table_name} (mode={mode})")
    
    writer = df.write.format(table_format).mode(mode)
    
    if partition_by:
        logger.info(f"Partitioning: {partition_by}")
        writer = writer.partitionBy(*partition_by)
        
    try:
        if table_name:
            writer.saveAsTable(table_name)
            logger.info(f"Wrote: {table_name}")
        else:
            raise ValueError("table_name must be provided")
            
    except Exception as e:
        logger.error(f"Write failed: {e}")
        raise e

def optimize_table(table_name: str, zorder_columns: list = None):
    """Run OPTIMIZE (and optional ZORDER) on a Delta table."""
    spark = SparkSession.getActiveSession()
    
    sql = f"OPTIMIZE {table_name}"
    
    logger.info(f"Optimizing: {table_name}")
        
    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        sql += f" ZORDER BY ({zorder_clause})"
        logger.info(f"Z-Ordering: {zorder_clause}")
    
    spark.sql(sql)
    logger.info(f"Optimized: {table_name}")

def merge_data(df: DataFrame, table_name: str, merge_keys: list, update_columns: list = None):
    """Upsert rows into a Delta table using MERGE on merge_keys."""
    logger.info(f"Merging into {table_name} on {merge_keys}")
    
    df.createOrReplaceTempView("merge_source")
    
    match_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
    
    if update_columns is None:
        update_columns = [col for col in df.columns if col not in merge_keys]
    
    update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
    
    all_columns = df.columns
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"source.{col}" for col in all_columns])
    
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    USING merge_source AS source
    ON {match_condition}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    try:
        spark = SparkSession.getActiveSession()
        spark.sql(merge_sql)
        logger.info(f"Merged: {table_name}")
    except Exception as e:
        logger.error(f"Merge failed: {table_name} -> {e}")
        raise e

def merge_scd_type2(df: DataFrame, table_name: str, merge_keys: list, compare_columns: list = None):
    """SCD Type 2 merge: expire changed rows and insert new versions."""
    from pyspark.sql.functions import col, lit, current_timestamp, to_date
    
    logger.info(f"SCD2: {table_name}")
    
    df_with_scd = df \
        .withColumn("effective_date", to_date(current_timestamp())) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    df_with_scd.createOrReplaceTempView("scd2_source")
    
    key_match = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
    
    if compare_columns is None:
        compare_columns = [c for c in df.columns if c not in merge_keys]
    
    compare_conditions = " OR ".join([
        f"target.{col} != source.{col} OR (target.{col} IS NULL AND source.{col} IS NOT NULL) OR (target.{col} IS NOT NULL AND source.{col} IS NULL)"
        for col in compare_columns
    ])
    
    all_columns = df_with_scd.columns
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"source.{col}" for col in all_columns])
    
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    USING scd2_source AS source
    ON {key_match} AND target.is_current = true
    WHEN MATCHED AND ({compare_conditions}) THEN
        UPDATE SET 
            target.is_current = false,
            target.end_date = current_date()
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    try:
        spark = SparkSession.getActiveSession()
        spark.sql(merge_sql)
        logger.info(f"SCD2 done: {table_name}")
    except Exception as e:
        logger.error(f"SCD2 failed: {table_name} -> {e}")
        raise e
