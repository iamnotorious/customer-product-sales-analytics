from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

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
        print(f"Error ingesting {file_format} from {source_path}: {e}")
        raise e
