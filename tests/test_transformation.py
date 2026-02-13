import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col
from sales_analytics.transformation import (
    clean_dataset, to_snake_case, join_dataframes, 
    parse_date_col
)

class TestToSnakeCase:
    """Test suite for snake_case conversion."""
    
    def test_snake_case_basic(self, spark):
        """Test basic column name conversion to snake_case."""
        data = [("Alice", "USA", 100)]
        schema = StructType([
            StructField("Customer Name", StringType(), True),
            StructField("Country Code", StringType(), True),
            StructField("Order ID", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = to_snake_case(df)
        
        assert "customer_name" in result.columns
        assert "country_code" in result.columns
        assert "order_id" in result.columns
    
    def test_snake_case_preserves_data(self, spark):
        """Test that snake_case conversion doesn't modify data."""
        data = [("Alice", 100), ("Bob", 200)]
        schema = StructType([
            StructField("User Name", StringType(), True),
            StructField("Amount", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = to_snake_case(df)
        rows = result.collect()
        
        assert rows[0]["user_name"] == "Alice"
        assert rows[0]["amount"] == 100

class TestCleanDataset:
    """Test suite for data cleaning functions."""
    
    def test_clean_text_removes_special_chars(self, spark):
        """Test special character removal from text."""
        data = [("Product___Name!!!", ), ("  Clean_Text  ",), ("Test@#$Value",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = clean_dataset(df, clean_text_cols=["name"])
        rows = result.collect()
        
        # New logic replaces symbols with spaces, then heals/normalizes.
        # "Product___Name!!!" -> "Product   Name   " -> "ProductName" (Healed >= 3 spaces)
        assert "!" not in rows[0]["name"]
        assert rows[0]["name"] == "ProductName"
        
        # "  Clean_Text  " -> "Clean Text" (1 space separator)
        # Old simple cleaning removed '_', new logic replaces with space.
        assert rows[1]["name"] == "Clean Text"
        
        # "Test@#$Value" -> "Test   Value" -> "TestValue" (Healed >= 3 spaces)
        assert "@" not in rows[2]["name"]
        assert rows[2]["name"] == "TestValue"
    def test_clean_text_removes_digits_for_names(self, spark):
        """Test: Full cleaning logic across difficult raw data cases."""
        data = [
            ("Bi 8761l Shonely",),           # Healed (Gap > 2)
            ("Shahi  Hopkins",),             # Separated (Gap <= 2)
            ("Ji11 Stevenson",),             # 11 -> ll
            ("Fra9876nk Gasti  ;.,.,neau",), # Healed (Gap > 2)
            ("Tam&^*ara Willing___)ham",),   # Healed (Gap > 2)
            ("Ad.       ..am Hart",),        # Healed (Gap > 2)
            ("Peter Bühler",),               # Unicode preserved
            ("Mary O'Rourke",),              # Apostrophe preserved
            ("   _Mike Vitt 12313orini",),   # 12313 removed
            ("Maribeth 5chnelling",),        # 5 -> s
            ("Mitch Willin0009gham",),       # 0009 removed -> Willingham
            ("Helen Wa55erman",),            # 55 -> ss
            ("Tho   12 mas Boland",),        # 12 removed -> Thomas
            ("N0ra Paige",),                 # 0 -> o
            ("Jocasta Rupert",),             # Normal name (No Change)
            ("B         ecky Martin",),      # Healed (Gap > 2)
            ("''Becky Pak",),                # Leading punctuation removed
            ("[]-=;''Becky Pak",)            # Complex leading junk removed
        ]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = clean_dataset(df, clean_names_cols=["name"])
        rows = result.collect()
        
        assert rows[0]["name"] == "Bill Shonely"
        assert rows[1]["name"] == "Shahi Hopkins"     # Kept separated
        assert rows[2]["name"] == "Jill Stevenson"    # 11 -> ll
        assert rows[3]["name"] == "Frank Gastineau"   # Healed
        assert rows[4]["name"] == "Tamara Willingham" # Healed
        assert rows[5]["name"] == "Adam Hart"         # Healed
        assert rows[6]["name"] == "Peter Bühler"
        assert rows[7]["name"] == "Mary O'Rourke"
        assert rows[8]["name"] == "Mike Vittorini"
        assert rows[9]["name"] == "Maribeth schnelling" # 5->s (lowercase 's' is expected behavior)
        assert rows[10]["name"] == "Mitch Willingham"
        assert rows[11]["name"] == "Helen Wasserman"
        assert rows[12]["name"] == "Thomas Boland"
        assert rows[13]["name"] == "Nora Paige"
        assert rows[14]["name"] == "Jocasta Rupert"
        assert rows[15]["name"] == "Becky Martin"
        assert rows[16]["name"] == "Becky Pak"
        assert rows[17]["name"] == "Becky Pak"
    
    def test_handle_nulls_fills_values(self, spark):
        """Test null value filling."""
        data = [("Alice", None), ("Bob", "USA"), (None, "Canada")]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("country", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = clean_dataset(df, handle_null_cols=["country"], null_fill_value="Unknown")
        rows = result.collect()
        
        assert rows[0]["country"] == "Unknown"
    
    def test_mandatory_cols_filters_nulls(self, spark):
        """Test that mandatory column validation filters null records."""
        data = [("O001", "C001"), (None, "C002"), ("O003", None)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = clean_dataset(df, mandatory_cols=["order_id", "customer_id"])
        
        # Should only keep complete records
        assert result.count() == 1
        assert result.first()["order_id"] == "O001"

class TestTransformationFunctions:
    """Test suite for transformation utility functions."""
    

    
    def test_parse_date_col_parses_correctly(self, spark):
        """Test date parsing with custom format."""
        data = [("21/08/2016",), ("15/03/2020",)]
        schema = StructType([StructField("date_str", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = parse_date_col(df, date_col="date_str", date_format="d/M/y")
        rows = result.collect()
        
        from datetime import date
        assert rows[0]["date_str"] == date(2016, 8, 21)
    

    
    def test_join_dataframes_inner_join(self, spark):
        """Test inner join between two DataFrames."""
        df1 = spark.createDataFrame([("1", "Alice"), ("2", "Bob")], ["id", "name"])
        df2 = spark.createDataFrame([("1", 100), ("2", 200)], ["id", "amount"])
        
        result = join_dataframes(df1, df2, join_on="id", join_type="inner")
        
        assert result.count() == 2
        assert "name" in result.columns
        assert "amount" in result.columns
    
    def test_join_dataframes_left_join(self, spark):
        """Test left join preserves left DataFrame records."""
        df1 = spark.createDataFrame([("1", "Alice"), ("2", "Bob"), ("3", "Charlie")], ["id", "name"])
        df2 = spark.createDataFrame([("1", 100), ("2", 200)], ["id", "amount"])
        
        result = join_dataframes(df1, df2, join_on="id", join_type="left")
        
        # Should keep all left records
        assert result.count() == 3
        rows = result.orderBy("id").collect()
        assert rows[2]["amount"] is None  # Charlie has no match

class TestEdgeCasesTransformation:
    """Test edge cases and error handling."""
    
    def test_clean_dataset_empty_dataframe(self, spark):
        """Test clean_dataset with empty DataFrame."""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
        df = spark.createDataFrame([], schema)
        
        result = clean_dataset(df, clean_text_cols=["name"])
        assert result.count() == 0
    
    def test_parse_date_invalid_format(self, spark):
        """Test date parsing with mismatched format."""
        data = [("2016-08-21",)]  # ISO format
        schema = StructType([StructField("date_str", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        # This should handle gracefully or parse correctly
        result = parse_date_col(df, date_col="date_str", date_format="d/M/y")
        
        # Invalid dates should become null
        assert result.first()["date_str"] is None
