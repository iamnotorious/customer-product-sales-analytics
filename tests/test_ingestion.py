import pytest
from pyspark.sql.types import StructType, StructField, StringType
from sales_analytics.ingestion import ingest_file

class TestIngestFile:
    """Test suite for generic file ingestion."""
    
    def test_ingest_file_format_excel_mapping(self, spark, monkeypatch):
        """Test Excel format is correctly mapped to spark-excel."""
        # Track what format was actually used
        format_used = []
        
        def mock_format(fmt):
            format_used.append(fmt)
            # Return a mock reader with required methods
            class MockReader:
                def options(self, **opts):
                    return self
                def option(self, key, value):
                    return self
                def schema(self, s):
                    return self
                def load(self, path):
                    # Return empty DataFrame
                    return spark.createDataFrame([], StructType([]))
            return MockReader()
        
        # Monkeypatch the spark.read.format method
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        ingest_file(spark, "excel", "dummy_path.xlsx")
        
        assert "com.crealytics.spark.excel" in format_used
    
    def test_ingest_file_csv_format(self, spark, monkeypatch):
        """Test CSV format ingestion."""
        format_used = []
        
        def mock_format(fmt):
            format_used.append(fmt)
            class MockReader:
                def options(self, **opts):
                    return self
                def option(self, key, value):
                    return self
                def schema(self, s):
                    return self
                def load(self, path):
                    return spark.createDataFrame([], StructType([StructField("col1", StringType())]))
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        result = ingest_file(spark, "csv", "dummy.csv", options={"header": "true"})
        
        assert "csv" in format_used
        assert result is not None
    
    def test_ingest_file_json_format(self, spark, monkeypatch):
        """Test JSON format ingestion."""
        format_used = []
        
        def mock_format(fmt):
            format_used.append(fmt)
            class MockReader:
                def options(self, **opts):
                    return self
                def option(self, key, value):
                    return self
                def schema(self, s):
                    return self
                def load(self, path):
                    return spark.createDataFrame([], StructType([StructField("col1", StringType())]))
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        result = ingest_file(spark, "json", "dummy.json", options={"multiLine": "true"})
        
        assert "json" in format_used
    
    def test_ingest_file_with_schema(self, spark, monkeypatch):
        """Test that schema is applied when provided."""
        schema_applied = []
        
        def mock_format(fmt):
            class MockReader:
                def options(self, **opts):
                    return self
                def option(self, key, value):
                    return self
                def schema(self, s):
                    schema_applied.append(s)
                    return self
                def load(self, path):
                    return spark.createDataFrame([], StructType([]))
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        test_schema = StructType([StructField("test_col", StringType())])
        ingest_file(spark, "csv", "dummy.csv", schema=test_schema)
        
        assert len(schema_applied) == 1
    
    def test_ingest_file_with_options(self, spark, monkeypatch):
        """Test that options are correctly passed."""
        options_applied = []
        
        def mock_format(fmt):
            class MockReader:
                def options(self, **opts):
                    options_applied.append(opts)
                    return self
                def option(self, key, value):
                    return self
                def schema(self, s):
                    return self
                def load(self, path):
                    return spark.createDataFrame([], StructType([]))
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        test_options = {"header": "true", "delimiter": ","}
        ingest_file(spark, "csv", "dummy.csv", options=test_options)
        
        assert len(options_applied) > 0

class TestIngestFileErrorHandling:
    """Test error handling in ingestion."""
    
    def test_ingest_file_handles_missing_file(self, spark, monkeypatch):
        """Test that missing file error is handled gracefully."""
        def mock_format(fmt):
            class MockReader:
                def options(self, **opts):
                    return self
                def option(self, key, value):
                    return self
                def schema(self, s):
                    return self
                def load(self, path):
                    raise FileNotFoundError(f"File not found: {path}")
            return MockReader()
        
        monkeypatch.setattr(spark.read, "format", mock_format)
        
        with pytest.raises(FileNotFoundError):
            ingest_file(spark, "csv", "nonexistent.csv")
