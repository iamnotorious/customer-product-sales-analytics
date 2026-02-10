from pyspark.sql import SparkSession, DataFrame
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "DatabricksApp") -> SparkSession:
    """
    Creates or retrieves a Spark session with optimized configurations.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        Configured SparkSession
    """
    logger.info(f"Creating Spark session: {app_name}")
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

def read_data(spark: SparkSession, file_format: str = "delta", path: str = None, table_name: str = None, schema=None, options: dict = None) -> DataFrame:
    """
    Generic function to read data from file path or managed table.
    
    Args:
        spark: SparkSession
        file_format: Format of the data (delta, parquet, csv, etc.)
        path: File path to read from
        table_name: Managed table name to read from
        schema: Optional schema to apply
        options: Optional read options
        
    Returns:
        DataFrame containing the read data
    """
    if table_name:
        logger.info(f"Reading from managed table: {table_name}")
        return spark.read.table(table_name)

    logger.info(f"Reading from path: {path} (format: {file_format})")
    reader = spark.read.format(file_format)
    
    if schema:
        reader = reader.schema(schema)
        
    if options:
        reader = reader.options(**options)
        
    try:
        df = reader.load(path)
        logger.info(f"Successfully read {df.count()} rows from {path}")
        return df
    except Exception as e:
        logger.error(f"Error reading data from {path}: {e}")
        raise e

def write_data(df: DataFrame, file_format: str = "delta", mode: str = "append", path: str = None, table_name: str = None, partition_by: list = None):
    """
    Generic function to write data to file path or managed table.
    
    Args:
        df: DataFrame to write
        file_format: Format to write (delta, parquet, etc.)
        mode: Write mode (append, overwrite, etc.)
        path: File path to write to
        table_name: Managed table name to write to
        partition_by: Optional list of columns to partition by
    """
    row_count = df.count()
    logger.info(f"Writing {row_count} rows (mode: {mode})")
    
    writer = df.write.format(file_format).mode(mode)
    
    if partition_by:
        logger.info(f"Partitioning by: {partition_by}")
        writer = writer.partitionBy(*partition_by)
        
    try:
        if table_name:
            writer.saveAsTable(table_name)
            logger.info(f"Successfully wrote to managed table: {table_name}")
        elif path:
            writer.save(path)
            logger.info(f"Data written successfully to {path}")
        else:
            raise ValueError("Either path or table_name must be provided")
            
    except Exception as e:
        print(f"Error writing data: {e}")
        raise e

def merge_data(df: DataFrame, table_name: str, merge_keys: list, update_columns: list = None):
    """
    Incrementally merge data into a Delta table using upsert logic.
    
    Args:
        df: Source DataFrame with new/updated records
        table_name: Target Delta table name
        merge_keys: List of columns to use for matching (primary/business keys)
        update_columns: Optional list of columns to update. If None, updates all columns.
    """
    logger.info(f"Merging {df.count()} rows into {table_name} using keys: {merge_keys}")
    
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
        logger.info(f"Successfully merged data into {table_name}")
    except Exception as e:
        logger.error(f"Error merging data into {table_name}: {e}")
        raise e

def merge_scd_type2(df: DataFrame, table_name: str, business_keys: list, compare_columns: list = None):
    """
    Merge data using SCD Type 2 logic to maintain historical records.
    
    Args:
        df: Source DataFrame with new/updated records
        table_name: Target Delta table name
        business_keys: List of business key columns (e.g., customer_id, product_id)
        compare_columns: Columns to compare for changes. If None, compares all non-key columns.
    
    SCD Type 2 adds:
        - effective_date: When this version became active
        - end_date: When this version expired (NULL for current)
        - is_current: Flag indicating active version
    """
    from pyspark.sql.functions import col, lit, current_timestamp, to_date
    from datetime import date
    
    logger.info(f"Applying SCD Type 2 merge to {table_name} with keys: {business_keys}")
    
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
        
        if changed_df.count() > 0:
            # Insert new current versions
            changed_df.write.format("delta").mode("append").saveAsTable(table_name)
            logger.info(f"Inserted {changed_df.count()} new versions for changed records")
        
        logger.info(f"Successfully applied SCD Type 2 to {table_name}")
        
    except Exception as e:
        logger.error(f"Error applying SCD Type 2 to {table_name}: {e}")
        raise e


