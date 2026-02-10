from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def get_spark_session(app_name: str = "DatabricksApp") -> SparkSession:
    """
    Gets or creates a SparkSession.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def read_data(spark: SparkSession, file_format: str = "delta", path: str = None, table_name: str = None, schema=None, options: dict = None) -> DataFrame:
    """
    Generic function to read data from file path or managed table.
    """
    if table_name:
        return spark.read.table(table_name)

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

def write_data(df: DataFrame, file_format: str = "delta", mode: str = "append", path: str = None, table_name: str = None, partition_by: list = None):
    """
    Generic function to write data to file path or managed table.
    """
    writer = df.write.format(file_format).mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        
    try:
        if table_name:
            writer.saveAsTable(table_name)
            print(f"Data written successfully to managed table {table_name}")
        elif path:
            writer.save(path)
            print(f"Data written successfully to {path}")
        else:
            raise ValueError("Either path or table_name must be provided")
            
    except Exception as e:
        print(f"Error writing data: {e}")
        raise e
