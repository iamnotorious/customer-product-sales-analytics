from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)

FORMAT_MAPPINGS = {
    "excel": "com.crealytics.spark.excel",
    "csv": "csv",
    "json": "json",
    "parquet": "parquet",
    "delta": "delta",
    "avro": "avro",
    "orc": "orc",
    "text": "text"
}

def _ingest_excel_pandas(spark: SparkSession, source_path: str, schema: StructType = None, options: dict = None) -> DataFrame:
    """
    Ingest Excel files using pyspark.pandas.read_excel.
    Reads all columns as strings to avoid mixed-type Arrow conversion errors.
    Spark schema handles type casting if provided.
    """
    import pyspark.pandas as ps

    read_options = {"dtype": str}
    if options:
        if options.get("header", "true").lower() == "true":
            read_options["header"] = 0
        else:
            read_options["header"] = None
        if "sheet" in options:
            read_options["sheet_name"] = options["sheet"]

    logger.info(f"Reading Excel via pyspark.pandas: {source_path}")
    ps_df = ps.read_excel(source_path, **read_options)
    logger.info(f"Read {len(ps_df)} rows from Excel")

    spark_df = ps_df.to_spark()

    if schema:
        from pyspark.sql.functions import col as spark_col
        for field in schema.fields:
            if field.name in spark_df.columns:
                spark_df = spark_df.withColumn(field.name, spark_col(field.name).cast(field.dataType))

    return spark_df

def ingest_file(spark: SparkSession, file_format: str, source_path: str, schema: StructType = None, options: dict = None) -> DataFrame:
    """
    Generic function to ingest data from a file source.
    
    Args:
        spark: SparkSession instance.
        file_format: Format alias (e.g., 'excel', 'csv') or full format string.
        source_path: Path to the source file.
        schema: Optional StructType schema.
        options: Optional dictionary of read options.
    """
    # Use pandas for Excel files (spark-excel JAR not available on serverless compute)
    if file_format.lower() == "excel":
        return _ingest_excel_pandas(spark=spark, source_path=source_path, schema=schema, options=options)

    actual_format = FORMAT_MAPPINGS.get(file_format.lower(), file_format)
    reader = spark.read.format(actual_format)
    
    if schema:
        reader = reader.schema(schema)
    
    if options:
        reader = reader.options(**options)
        
    try:
        return reader.load(source_path)
    except Exception as e:
        logger.error(f"Error ingesting {file_format} from {source_path}: {e}")
        raise e

