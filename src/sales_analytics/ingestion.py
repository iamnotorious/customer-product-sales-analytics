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
    Fallback: Ingest Excel files using pandas + openpyxl.
    Used when com.crealytics.spark.excel is not available (e.g., serverless compute).
    """
    import pandas as pd

    read_options = {}
    if options:
        if options.get("header", "true").lower() == "true":
            read_options["header"] = 0
        else:
            read_options["header"] = None
        if "sheet" in options:
            read_options["sheet_name"] = options["sheet"]

    logger.info(f"Falling back to pandas for Excel ingestion: {source_path}")
    pandas_df = pd.read_excel(source_path, **read_options)

    if schema:
        return spark.createDataFrame(data=pandas_df, schema=schema)
    else:
        return spark.createDataFrame(data=pandas_df)

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
    actual_format = FORMAT_MAPPINGS.get(file_format.lower(), file_format)
    reader = spark.read.format(actual_format)
    
    if schema:
        reader = reader.schema(schema)
    
    if options:
        reader = reader.options(**options)
        
    try:
        return reader.load(source_path)
    except Exception as e:
        # Fallback to pandas for Excel files if spark-excel JAR is unavailable
        if file_format.lower() == "excel":
            logger.warning(f"spark-excel failed, falling back to pandas: {e}")
            return _ingest_excel_pandas(spark=spark, source_path=source_path, schema=schema, options=options)
        logger.error(f"Error ingesting {file_format} from {source_path}: {e}")
        raise e
