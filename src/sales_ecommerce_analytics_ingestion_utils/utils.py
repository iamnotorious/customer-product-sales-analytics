from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def get_spark_session(app_name: str = "DatabricksApp") -> SparkSession:
    """
    Gets or creates a SparkSession.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def read_data(spark: SparkSession, file_format: str, path: str, schema=None, options: dict = None) -> DataFrame:
    """
    Generic function to read data.
    """
    reader = spark.read.format(file_format)
    
    if schema:
        reader = reader.schema(schema)
        
    if options:
        reader = reader.options(**options)
        
    try:
        return reader.load(path)
    except Exception as e:
        print(f"Error reading data from {path}: {e}")
        raise e

def write_data(df: DataFrame, file_format: str, mode: str, path: str, partition_by: list = None):
    """
    Generic function to write data.
    """
    writer = df.write.format(file_format).mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        
    try:
        writer.save(path)
        print(f"Data written successfully to {path}")
    except Exception as e:
        print(f"Error writing data to {path}: {e}")
        raise e
