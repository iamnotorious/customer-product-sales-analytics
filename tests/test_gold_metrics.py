import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from sales_analytics.aggregation import create_aggregates

class TestCreateAggregates:
    """Test suite for aggregation functions."""
    
    def test_create_aggregates_single_group(self, spark):
        """Test aggregation with single grouping dimension."""
        data = [
            ("2023", "Electronics", 100.0),
            ("2023", "Electronics", 200.0),
            ("2023", "Furniture", 50.0)
        ]
        schema = StructType([
            StructField("year", StringType(), True),
            StructField("category", StringType(), True),
            StructField("profit", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = create_aggregates(
            df, 
            group_by_cols=["year", "category"], 
            agg_col="profit", 
            alias_col="total_profit"
        )
        
        rows = result.orderBy("category").collect()
        assert result.count() == 2
        assert rows[0]["total_profit"] == 300.0  # Electronics
        assert rows[1]["total_profit"] == 50.0   # Furniture
    
    def test_create_aggregates_multiple_dimensions(self, spark):
        """Test aggregation with multiple grouping dimensions."""
        data = [
            ("2023", "Electronics", "Phones", "Alice", 100.0),
            ("2023", "Electronics", "Phones", "Alice", 200.0),
            ("2023", "Electronics", "Laptops", "Bob", 500.0),
            ("2024", "Electronics", "Phones", "Alice", 150.0)
        ]
        schema = StructType([
            StructField("year", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("customer", StringType(), True),
            StructField("profit", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = create_aggregates(
            df,
            group_by_cols=["year", "category", "sub_category", "customer"],
            agg_col="profit",
            alias_col="total_profit"
        )
        
        assert result.count() == 3  # 3 unique combinations
        
        # Verify Alice's 2023 Phones total
        alice_2023 = result.filter(
            (result.year == "2023") & 
            (result.customer == "Alice") & 
            (result.sub_category == "Phones")
        ).first()
        assert alice_2023["total_profit"] == 300.0
    
    def test_create_aggregates_with_rounding(self, spark):
        """Test aggregation with profit rounding."""
        data = [
            ("2023", "A", 123.456),
            ("2023", "A", 234.567)
        ]
        schema = StructType([
            StructField("year", StringType(), True),
            StructField("category", StringType(), True),
            StructField("profit", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = create_aggregates(
            df,
            group_by_cols=["year", "category"],
            agg_col="profit",
            alias_col="total_profit",
            round_places=2
        )
        
        row = result.first()
        # 123.456 + 234.567 = 358.023 → 358.02
        assert row["total_profit"] == 358.02
    
    def test_create_aggregates_negative_values(self, spark):
        """Test aggregation handles negative profits."""
        data = [
            ("2023", "A", 100.0),
            ("2023", "A", -30.0),
            ("2023", "A", 50.0)
        ]
        schema = StructType([
            StructField("year", StringType(), True),
            StructField("category", StringType(), True),
            StructField("profit", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = create_aggregates(
            df,
            group_by_cols=["year", "category"],
            agg_col="profit",
            alias_col="total_profit"
        )
        
        row = result.first()
        assert row["total_profit"] == 120.0  # 100 - 30 + 50
    
    def test_create_aggregates_empty_groups(self, spark):
        """Test aggregation with dataset that has no records."""
        schema = StructType([
            StructField("year", StringType(), True),
            StructField("category", StringType(), True),
            StructField("profit", DoubleType(), True)
        ])
        df = spark.createDataFrame([], schema)
        
        result = create_aggregates(
            df,
            group_by_cols=["year", "category"],
            agg_col="profit",
            alias_col="total_profit"
        )
        
        assert result.count() == 0
    
    def test_create_aggregates_null_handling(self, spark):
        """Test aggregation correctly handles null values."""
        data = [
            ("2023", "A", 100.0),
            ("2023", "A", None),
            ("2023", "A", 50.0)
        ]
        schema = StructType([
            StructField("year", StringType(), True),
            StructField("category", StringType(), True),
            StructField("profit", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = create_aggregates(
            df,
            group_by_cols=["year", "category"],
            agg_col="profit",
            alias_col="total_profit"
        )
        
        row = result.first()
        # sum() ignores nulls
        assert row["total_profit"] == 150.0
