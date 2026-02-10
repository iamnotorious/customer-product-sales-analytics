import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sales_analytics.utils import read_data, write_data

class TestReadData:
    """Test suite for read_data function."""
    
    def test_read_data_from_table(self, spark, monkeypatch):
        """Test reading from managed table."""
        table_read = []
        
        def mock_table(table_name):
            table_read.append(table_name)
            return spark.createDataFrame([], StructType([StructField("col1", StringType())]))
        
        monkeypatch.setattr(spark.read, "table", mock_table)
        
        result = read_data(spark, table_name="test_table")
        
        assert "test_table" in table_read
        assert result is not None
    
    def test_read_data_from_path(self, spark, monkeypatch):
        """Test reading from file path."""
        format_used = []
        path_loaded = []
        
        def mock_format(fmt):
            format_used.append(fmt)
            class MockReader:
                def options(self, **opts):
                    return self
                def schema(self, s):
                    return self
                def load(self, p):
                    path_loaded.append(p)
                    return spark.createDataFrame([], StructType([]))
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        read_data(spark, file_format="delta", path="/tmp/test_path")
        
        assert "delta" in format_used
        assert "/tmp/test_path" in path_loaded
    
    def test_read_data_with_schema(self, spark, monkeypatch):
        """Test reading with custom schema."""
        schema_applied = []
        
        def mock_format(fmt):
            class MockReader:
                def options(self, **opts):
                    return self
                def schema(self, s):
                    schema_applied.append(s)
                    return self
                def load(self, p):
                    return spark.createDataFrame([], s)
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        test_schema = StructType([StructField("id", StringType())])
        read_data(spark, path="/tmp/path", schema=test_schema)
        
        assert len(schema_applied) == 1

class TestWriteData:
    """Test suite for write_data function."""
    
    def test_write_data_to_table(self, spark, monkeypatch):
        """Test writing to managed table."""
        tables_written = []
        
        # Create a mock DataFrame
        df = spark.createDataFrame([("1", "test")], ["id", "value"])
        
        # Track write calls
        original_write = df.write
        
        class MockWriter:
            def format(self, fmt):
                self.fmt = fmt
                return self
            def mode(self, m):
                self.m = m
                return self
            def partitionBy(self, *cols):
                return self
            def saveAsTable(self, table_name):
                tables_written.append(table_name)
        
        monkeypatch.setattr(df, "write", MockWriter())
        
        write_data(df, table_name="test_table", mode="overwrite")
        
        assert "test_table" in tables_written
    
    def test_write_data_with_partitions(self, spark):
        """Test writing with partitions."""
        df = spark.createDataFrame([("1", "A", 100)], ["id", "category", "value"])
        
        # This is a real test since partitioning is important
        df.write.format("delta").mode("overwrite").partitionBy("category").saveAsTable("test_partition_table")
        
        result = spark.table("test_partition_table")
        assert result.count() == 1
        
        spark.sql("DROP TABLE IF EXISTS test_partition_table")

class TestUtilsErrorHandling:
    """Test error handling in utility functions."""
    
    def test_write_data_requires_destination(self, spark):
        """Test that write_data requires either path or table_name."""
        df = spark.createDataFrame([("1",)], ["id"])
        
        with pytest.raises(ValueError):
            write_data(df, mode="overwrite")  # Neither path nor table_name provided
    
    def test_read_data_error_propagates(self, spark, monkeypatch):
        """Test that read errors are propagated."""
        def mock_table(table_name):
            raise Exception("Table not found")
        
        monkeypatch.setattr(spark.read, "table", mock_table)
        
        with pytest.raises(Exception):
            read_data(spark, table_name="nonexistent_table")
