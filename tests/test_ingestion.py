import pytest
from unittest.mock import MagicMock
from pyspark.sql.types import StructType, StructField, StringType
from sales_ecommerce_analytics_ingestion_utils.ingestion import ingest_file

def test_ingest_file_format_mapping(spark):
    """
    Test that generic format aliases (excel, csv, json) are mapped correctly.
    Using mocks to avoid actual file I/O.
    """
    mock_spark = MagicMock()
    mock_read = mock_spark.read
    mock_format = mock_read.format.return_value
    mock_load = mock_format.load.return_value
    
    # Test 'excel' alias -> 'com.crealytics.spark.excel'
    ingest_file(mock_spark, "excel", "path/to/file")
    mock_read.format.assert_called_with("com.crealytics.spark.excel")
    
    # Test 'csv' alias -> 'csv'
    ingest_file(mock_spark, "csv", "path/to/file")
    mock_read.format.assert_called_with("csv")

def test_ingest_file_schema_application(spark):
    """
    Test that schema is applied if provided.
    """
    mock_spark = MagicMock()
    mock_read = mock_spark.read.format.return_value
    mock_schema = mock_read.schema
    
    test_schema = StructType([StructField("col1", StringType(), True)])
    
    ingest_file(mock_spark, "csv", "path/to/file", schema=test_schema)
    
    mock_schema.assert_called_with(test_schema)

def test_ingest_file_options_application(spark):
    """
    Test that options are applied if provided.
    """
    mock_spark = MagicMock()
    mock_read = mock_spark.read.format.return_value
    mock_options = mock_read.options
    
    test_options = {"header": "true", "delimiter": ";"}
    
    ingest_file(mock_spark, "csv", "path/to/file", options=test_options)
    
    mock_options.assert_called_with(**test_options)

