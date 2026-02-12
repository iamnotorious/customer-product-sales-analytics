
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import col, lit, to_date, current_timestamp
from datetime import date
from sales_analytics.utils import merge_data, merge_scd_type2
from sales_analytics.transformation import generate_surrogate_key, deduplicate, add_audit_columns

def test_merge_data_insert_new_records(spark):
    """Test merge_data inserts new records correctly."""
    # Create initial table
    data = [("1", "Alice", 100), ("2", "Bob", 200)]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").saveAsTable("test_merge_insert")
    
    # New records to insert
    new_data = [("3", "Charlie", 300)]
    new_df = spark.createDataFrame(new_data, schema)
    
    merge_data(new_df, "test_merge_insert", merge_keys=["id"])
    
    result = spark.table("test_merge_insert").orderBy("id").collect()
    assert len(result) == 3
    assert result[2]["name"] == "Charlie"
    
    spark.sql("DROP TABLE IF EXISTS test_merge_insert")

def test_merge_data_update_existing_records(spark):
    """Test merge_data updates existing records correctly."""
    data = [("1", "Alice", 100), ("2", "Bob", 200)]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").saveAsTable("test_merge_update")
    
    # Updated records
    update_data = [("1", "Alice Updated", 150)]
    update_df = spark.createDataFrame(update_data, schema)
    
    merge_data(update_df, "test_merge_update", merge_keys=["id"])
    
    result = spark.table("test_merge_update").filter(col("id") == "1").first()
    assert result["name"] == "Alice Updated"
    assert result["amount"] == 150
    
    spark.sql("DROP TABLE IF EXISTS test_merge_update")

def test_scd_type2_attribute_change(spark):
    """Test SCD Type 2 creates new version when attributes change."""
    # Create initial version
    data = [("C001", "John", "USA")]
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    df_scd = df \
        .withColumn("effective_date", to_date(lit("2023-01-01"))) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    df_scd.write.format("delta").mode("overwrite").saveAsTable("test_scd2_change")
    
    # Changed data (country changed)
    changed_data = [("C001", "John", "Canada")]
    changed_df = spark.createDataFrame(changed_data, schema)
    
    merge_scd_type2(changed_df, "test_scd2_change", business_keys=["customer_id"], compare_columns=["name", "country"])
    
    result = spark.table("test_scd2_change").orderBy("effective_date").collect()
    
    # Should have 2 versions now
    assert len(result) == 2
    # Old version should be closed
    assert result[0]["is_current"] == False
    assert result[0]["end_date"] is not None
    assert result[0]["country"] == "USA"
    # New version should be current
    assert result[1]["is_current"] == True
    assert result[1]["end_date"] is None
    assert result[1]["country"] == "Canada"
    
    spark.sql("DROP TABLE IF EXISTS test_scd2_change")

# --- New Silver Logic Tests Below ---

class TestSilverTransformations:
    """Test suite for Silver layer transformations."""
    
    def test_deduplicate_removes_dupes(self, spark):
        """Test deduplication logic."""
        data = [("1", "A"), ("1", "A"), ("2", "B")]
        schema = StructType([StructField("id", StringType(), True), StructField("val", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = deduplicate(df, key_columns=["id"])
        
        assert result.count() == 2
        
    def test_generate_surrogate_key_deterministic(self, spark):
        """Test that surrogate key generation is deterministic based on business keys."""
        data = [("C001", "USA"), ("C001", "USA")]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("country", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        result = generate_surrogate_key(df, key_columns=["id", "country"], sk_column_name="sk")
        rows = result.collect()
        
        # Both keys should be identical
        assert rows[0]["sk"] == rows[1]["sk"]
        assert len(rows[0]["sk"]) == 32  # MD5 hash length
        
    def test_add_audit_columns(self, spark):
        """Test audit column addition."""
        data = [("1",)]
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        result = add_audit_columns(df)
        
        assert "created_at" in result.columns
        assert result.first()["created_at"] is not None
