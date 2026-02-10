import pytest
from unittest.mock import MagicMock
from sales_ecommerce_analytics_ingestion_utils.utils import read_data, write_data

def test_read_data_from_table(spark):
    """
    Test reading from a managed table via table_name argument.
    """
    mock_spark = MagicMock()
    mock_read = mock_spark.read
    
    read_data(mock_spark, table_name="test_catalog.test_schema.test_table")
    
    mock_read.table.assert_called_with("test_catalog.test_schema.test_table")

def test_read_data_from_path(spark):
    """
    Test reading from a path.
    """
    mock_spark = MagicMock()
    mock_read = mock_spark.read.format.return_value
    
    read_data(mock_spark, file_format="delta", path="/tmp/test_path")
    
    mock_read.load.assert_called_with("/tmp/test_path")

def test_write_data_to_table(spark):
    """
    Test writing to a managed table via table_name argument.
    """
    mock_df = MagicMock()
    mock_write = mock_df.write.format.return_value.mode.return_value
    
    write_data(mock_df, table_name="test_catalog.test_schema.test_table")
    
    mock_write.saveAsTable.assert_called_with("test_catalog.test_schema.test_table")

def test_write_data_to_path(spark):
    """
    Test writing to a path.
    """
    mock_df = MagicMock()
    mock_write = mock_df.write.format.return_value.mode.return_value
    
    write_data(mock_df, path="/tmp/write_path")
    
    mock_write.save.assert_called_with("/tmp/write_path")

def test_write_data_partitioning(spark):
    """
    Test writing with partitioning.
    """
    mock_df = MagicMock()
    mock_write = mock_df.write.format.return_value.mode.return_value
    
    write_data(mock_df, path="/tmp/part_path", partition_by=["year", "month"])
    
    mock_write.partitionBy.assert_called_with("year", "month")
