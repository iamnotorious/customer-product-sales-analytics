import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col
from sales_analytics.transformation import (
    clean_customer_names, clean_customer_phones, clean_names, clean_phone,
    fill_missing_values, to_snake_case, join_dataframes, parse_date_col, 
    deduplicate, generate_surrogate_key
)

class TestColumnStandardization:
    """Test column name standardization."""
    
    def test_to_snake_case(self, spark):
        """Test column name conversion to snake_case."""
        data = [("Alice", "USA")]
        schema = StructType([
            StructField("Customer Name", StringType(), True),
            StructField("Country Code", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = to_snake_case(df)
        
        assert "customer_name" in result.columns
        assert "country_code" in result.columns
        assert result.first()["customer_name"] == "Alice"

class TestNameCleaning:
    """Test customer name cleaning."""
    
    def test_clean_names_corruption_patterns(self, spark):
        """Test name cleaning with real corruption patterns."""
        data = [
            ("Gary567 Hansen",),
            ("C@thy Armstrong",),
            ("Kat rina Edelman",),
            ("Mary O'Rourke",),
        ]
        schema = StructType([StructField("customer_name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = clean_customer_names(df)
        rows = result.collect()
        
        assert rows[0]["customer_name"] == "Gary Hansen"
        assert rows[1]["customer_name"] == "Cathy Armstrong"
        assert rows[2]["customer_name"] == "Katrina Edelman"
        assert rows[3]["customer_name"] == "Mary O'Rourke"


class TestPhoneCleaning:
    """Test phone number cleaning."""
    
    def test_clean_phone_formats(self, spark):
        """Test phone cleaning with various formats."""
        data = [
            ("421.580.0902x9815",),
            ("001-542-415-0246x314",),
            ("7185624866",),
            ("#ERROR!",),
        ]
        schema = StructType([StructField("phone", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = clean_customer_phones(df)
        rows = result.collect()
        
        assert rows[0]["phone"] == "(421) 580-0902 x9815"
        assert rows[1]["phone"] == "(542) 415-0246 x314"
        assert rows[2]["phone"] == "(718) 562-4866"
        assert rows[3]["phone"] is None

class TestPipelineIntegration:
    """Test end-to-end pipeline integration."""
    
    def test_customer_cleaning_pipeline(self, spark):
        """Test complete customer cleaning workflow."""
        data = [
            ("C001", "Gary567 Hansen", "421.580.0902x9815", "USA"),
            ("C002", "C@thy Armstrong", "#ERROR!", None),
        ]
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("country", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = (
            df
            .transform(clean_customer_names)
            .transform(clean_customer_phones)
            .transform(lambda df: fill_missing_values(df, ["country"], "Unknown"))
        )
        rows = result.collect()
        
        assert rows[0]["customer_name"] == "Gary Hansen"
        assert rows[0]["phone"] == "(421) 580-0902 x9815"
        assert rows[1]["customer_name"] == "Cathy Armstrong"
        assert rows[1]["phone"] is None
        assert rows[1]["country"] == "Unknown"


class TestDimensionProcessing:
    """Test dimension table processing utilities."""
    
    def test_deduplicate(self, spark):
        """Test deduplication for customer master."""
        data = [("C001", "John Doe"), ("C001", "John Doe"), ("C002", "Jane Smith")]
        df = spark.createDataFrame(data, ["customer_id", "customer_name"])
        
        result = deduplicate(df, ["customer_id"])
        
        assert result.count() == 2
    
    def test_generate_surrogate_key(self, spark):
        """Test surrogate key generation."""
        data = [("C001", "John Doe"), ("C002", "Jane Smith")]
        df = spark.createDataFrame(data, ["customer_id", "customer_name"])
        
        result = generate_surrogate_key(df, ["customer_id"], "customer_sk")
        
        assert "customer_sk" in result.columns
        assert result.select("customer_sk").distinct().count() == 2


class TestFactProcessing:
    """Test fact table processing utilities."""
    
    def test_parse_date_col(self, spark):
        """Test date parsing for order dates."""
        data = [("21/08/2016",), ("15/03/2020",)]
        df = spark.createDataFrame(data, ["order_date"])
        
        result = parse_date_col(df, date_col="order_date", date_format="d/M/y")
        
        from datetime import date
        assert result.first()["order_date"] == date(2016, 8, 21)
    
    def test_join_dataframes(self, spark):
        """Test joining orders with customers."""
        customers = spark.createDataFrame([("C001", "John Doe")], ["customer_id", "customer_name"])
        orders = spark.createDataFrame([("O001", "C001", 100.0)], ["order_id", "customer_id", "amount"])
        
        result = join_dataframes(orders, customers, join_on="customer_id", join_type="left")
        
        assert result.count() == 1
        assert "customer_name" in result.columns
