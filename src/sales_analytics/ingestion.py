from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)



def _ingest_excel_pandas(spark: SparkSession, source_path: str, schema: StructType = None, options: dict = None) -> DataFrame:
    """Ingest Excel using pyspark.pandas (converts all to string first)."""
    import pyspark.pandas as ps

    read_options = {"dtype": str}
    if options:
        if options.get("header", "true").lower() == "true":
            read_options["header"] = 0
        else:
            read_options["header"] = None
        
        if "sheet" in options:
            read_options["sheet_name"] = options["sheet"]

    logger.info(f"Reading: {source_path}")
    ps_df = ps.read_excel(source_path, **read_options)
    logger.info(f"Read {len(ps_df)} rows")

    spark_df = ps_df.to_spark()

    if schema:
        from pyspark.sql.functions import col as spark_col
        schema_col_names = [field.name for field in schema.fields]
        
        for field in schema.fields:
            if field.name in spark_df.columns:
                spark_df = spark_df.withColumn(field.name, spark_col(field.name).cast(field.dataType))
                
        spark_df = spark_df.select(*[c for c in schema_col_names if c in spark_df.columns])

    return spark_df

def ingest_file(spark: SparkSession, file_format: str, source_path: str, schema: StructType = None, options: dict = None) -> DataFrame:
    """Read data file into DataFrame."""
    if file_format.lower() == "excel":
        return _ingest_excel_pandas(spark=spark, source_path=source_path, schema=schema, options=options)

    reader = spark.read.format(file_format.lower())
    
    if schema:
        reader = reader.schema(schema)
    
    if options:
        reader = reader.options(**options)
        
    try:
        return reader.load(source_path)
    except Exception as e:
        logger.error(f"Ingest failed: {source_path} ({file_format}) -> {e}")
        raise e

