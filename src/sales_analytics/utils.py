from pyspark.sql import SparkSession, DataFrame
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "DatabricksApp") -> SparkSession:
    """Get configured Spark session."""
    logger.info(f"Starting Spark: {app_name}")
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.13:3.5.1_0.20.4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def read_data(spark: SparkSession, file_format: str = "delta", path: str = None, table_name: str = None, schema=None, options: dict = None) -> DataFrame:
    """Read data from file or table."""
    if table_name:
        logger.info(f"Reading table: {table_name}")
        return spark.read.table(table_name)

    logger.info(f"Reading file: {path} ({file_format})")
    reader = spark.read.format(file_format)
    
    if schema:
        reader = reader.schema(schema)
        
    if options:
        reader = reader.options(**options)
        
    try:
        df = reader.load(path)
        logger.info(f"Read complete: {path}")
        return df
    except Exception as e:
        logger.error(f"Read failed: {path} -> {e}")
        raise e

def write_data(df: DataFrame, file_format: str = "delta", mode: str = "append", path: str = None, table_name: str = None, partition_by: list = None):
    """Write DataFrame to file or table."""
    logger.info(f"Writing data (mode={mode})")
    
    writer = df.write.format(file_format).mode(mode)
    
    if partition_by:
        logger.info(f"Partitioning: {partition_by}")
        writer = writer.partitionBy(*partition_by)
        
    try:
        if table_name:
            writer.saveAsTable(table_name)
            logger.info(f"Wrote to table: {table_name}")
        elif path:
            writer.save(path)
            logger.info(f"Wrote to path: {path}")
        else:
            raise ValueError("Either path or table_name must be provided")
            
    except Exception as e:
        logger.error(f"Write failed: {e}")
        raise e

def optimize_table(table_name: str, zorder_columns: list = None, where: str = None):
    """Run OPTIMIZE on table (with optional Z-ORDER/WHERE)."""
    spark = SparkSession.getActiveSession()
    
    sql = f"OPTIMIZE {table_name}"
    
    if where:
        sql += f" WHERE {where}"
        logger.info(f"Optimizing {table_name} ({where})")
    else:
        logger.info(f"Optimizing full table: {table_name}")
        
    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        sql += f" ZORDER BY ({zorder_clause})"
        logger.info(f"Z-Ordering: {zorder_clause}")
    
    spark.sql(sql)
    logger.info(f"Optimize complete: {table_name}")

def merge_data(df: DataFrame, table_name: str, merge_keys: list, update_columns: list = None):
    """Merge (Upsert) data into Delta table."""
    logger.info(f"Merging into {table_name} keys={merge_keys}")
    
    # Create temp view for merge
    df.createOrReplaceTempView("merge_source")
    
    # Build match condition
    match_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
    
    # Build update set clause
    if update_columns is None:
        update_columns = [col for col in df.columns if col not in merge_keys]
    
    update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
    
    # Build insert values
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
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        spark.sql(merge_sql)
        logger.info(f"Merge complete: {table_name}")
    except Exception as e:
        logger.error(f"Merge failed: {table_name} -> {e}")
        raise e

def merge_scd_type2(df: DataFrame, table_name: str, business_keys: list, compare_columns: list = None):
    """Apply SCD Type 2 merge (maintains history)."""
    from pyspark.sql.functions import col, lit, current_timestamp, to_date
    from datetime import date
    
    logger.info(f"SCD2 Merge: {table_name} keys={business_keys}")
    
    # Add SCD Type 2 columns to source data
    df_with_scd = df \
        .withColumn("effective_date", to_date(current_timestamp())) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    # Create temp view
    df_with_scd.createOrReplaceTempView("scd2_source")
    
    # Build business key match condition
    key_match = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])
    
    # Build compare condition (check if any attribute changed)
    if compare_columns is None:
        compare_columns = [c for c in df.columns if c not in business_keys]
    
    # Create comparison for changes (any column different)
    compare_conditions = " OR ".join([
        f"target.{col} != source.{col} OR (target.{col} IS NULL AND source.{col} IS NOT NULL) OR (target.{col} IS NOT NULL AND source.{col} IS NULL)"
        for col in compare_columns
    ])
    
    # Build column list for insert
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
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        # Execute merge to close old records
        spark.sql(merge_sql)
        
        # Insert new versions for updated records
        # Find records that were just closed (changed)
        changed_records_sql = f"""
        SELECT source.*
        FROM scd2_source source
        INNER JOIN {table_name} target
        ON {key_match}
        WHERE target.is_current = false 
        AND target.end_date = current_date()
        AND NOT EXISTS (
            SELECT 1 FROM {table_name} t2 
            WHERE {" AND ".join([f"t2.{key} = source.{key}" for key in business_keys])}
            AND t2.is_current = true
        )
        """
        
        changed_df = spark.sql(changed_records_sql)
        
        changed_count = changed_df.count()
        if changed_count > 0:
            # Insert new current versions
            changed_df.write.format("delta").mode("append").saveAsTable(table_name)
            logger.info(f"SCD2: Inserted {changed_count} history records")
        
        logger.info(f"SCD2 complete: {table_name}")
        
    except Exception as e:
        logger.error(f"SCD2 failed: {table_name} -> {e}")
        raise e


