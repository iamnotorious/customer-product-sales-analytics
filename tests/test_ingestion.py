import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from sales_analytics.ingestion import ingest_file
import sys
from unittest.mock import MagicMock, patch

class MockDataFrameReader:
    def __init__(self):
        self.format_args = []
        self.options_args = {}
        self.schema_arg = None
        self.load_path = None
        
    def format(self, fmt):
        self.format_args.append(fmt)
        return self
        
    def options(self, **opts):
        self.options_args.update(opts)
        return self
        
    def option(self, key, value):
        self.options_args[key] = value
        return self
        
    def schema(self, s):
        self.schema_arg = s
        return self
        
    def load(self, path):
        self.load_path = path
        # Return a mock DataFrame
        return MagicMock()

class MockSparkSession:
    def __init__(self):
        self.read = MockDataFrameReader()
    
    def createDataFrame(self, data, schema=None):
        return MagicMock()

class TestIngestFile:
    """Test suite for generic file ingestion."""
    
    def test_ingest_file_excel_pandas(self):
        """Test Excel ingestion uses pandas-on-spark."""
        mock_spark = MockSparkSession()
        
        # Mock pyspark.pandas.read_excel
        with patch('pyspark.pandas.read_excel') as mock_read_excel:
            mock_ps_df = MagicMock()
            mock_ps_df.to_spark.return_value = MagicMock()
            mock_read_excel.return_value = mock_ps_df
            
            ingest_file(mock_spark, "excel", "dummy.xlsx")
            
            mock_read_excel.assert_called_once()
            args, _ = mock_read_excel.call_args
            assert args[0] == "dummy.xlsx"

    def test_ingest_file_csv_format(self):
        """Test CSV format ingestion."""
        mock_spark = MockSparkSession()
        
        ingest_file(mock_spark, "csv", "dummy.csv", options={"header": "true"})
        
        assert "csv" in mock_spark.read.format_args
        assert mock_spark.read.options_args.get("header") == "true"
        assert mock_spark.read.load_path == "dummy.csv"
    
    def test_ingest_file_json_format(self):
        """Test JSON format ingestion."""
        mock_spark = MockSparkSession()
        
        ingest_file(mock_spark, "json", "dummy.json", options={"multiLine": "true"})
        
        assert "json" in mock_spark.read.format_args
        assert mock_spark.read.load_path == "dummy.json"
    
    def test_ingest_file_with_schema(self):
        """Test that schema is applied when provided."""
        mock_spark = MockSparkSession()
        test_schema = StructType([StructField("test_col", StringType())])
        
        ingest_file(mock_spark, "csv", "dummy.csv", schema=test_schema)
        
        assert mock_spark.read.schema_arg == test_schema
    
    def test_ingest_file_with_options(self):
        """Test that options are correctly passed."""
        mock_spark = MockSparkSession()
        test_options = {"header": "true", "delimiter": ","}
        
        ingest_file(mock_spark, "csv", "dummy.csv", options=test_options)
        
        assert mock_spark.read.options_args["delimiter"] == ","

class TestIngestFileErrorHandling:
    """Test error handling in ingestion."""
    
    def test_ingest_file_handles_missing_file(self):
        """Test that errors during load are raised."""
        mock_spark = MockSparkSession()
        # Make load raise FileNotFoundError
        mock_spark.read.load = MagicMock(side_effect=FileNotFoundError("File not found"))
        
        with pytest.raises(FileNotFoundError):
            ingest_file(mock_spark, "csv", "nonexistent.csv")
