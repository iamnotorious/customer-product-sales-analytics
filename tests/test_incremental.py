import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import col, lit, to_date, current_timestamp
from datetime import date
from sales_analytics.utils import merge_data, merge_scd_type2

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

def test_merge_data_upsert_mixed(spark):
    """Test merge_data handles both inserts and updates in same operation."""
    data = [("1", "Alice", 100), ("2", "Bob", 200)]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").saveAsTable("test_merge_mixed")
    
    # Mix of update and insert
    mixed_data = [("1", "Alice Updated", 150), ("3", "Charlie", 300)]
    mixed_df = spark.createDataFrame(mixed_data, schema)
    
    merge_data(mixed_df, "test_merge_mixed", merge_keys=["id"])
    
    result = spark.table("test_merge_mixed").orderBy("id").collect()
    assert len(result) == 3
    assert result[0]["name"] == "Alice Updated"
    assert result[2]["name"] == "Charlie"
    
    spark.sql("DROP TABLE IF EXISTS test_merge_mixed")

def test_scd_type2_initial_load(spark):
    """Test SCD Type 2 creates initial records with proper metadata."""
    data = [("C001", "John", "USA"), ("C002", "Jane", "Canada")]
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Add SCD columns for initial load
    df_scd = df \
        .withColumn("effective_date", to_date(current_timestamp())) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    df_scd.write.format("delta").mode("overwrite").saveAsTable("test_scd2_initial")
    
    result = spark.table("test_scd2_initial").collect()
    assert len(result) == 2
    assert all(r["is_current"] == True for r in result)
    assert all(r["end_date"] is None for r in result)
    
    spark.sql("DROP TABLE IF EXISTS test_scd2_initial")

def test_scd_type2_no_changes(spark):
    """Test SCD Type 2 doesn't create new versions when no changes detected."""
    # Create initial table
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
    
    df_scd.write.format("delta").mode("overwrite").saveAsTable("test_scd2_nochange")
    
    # Apply same data again (no changes)
    merge_scd_type2(df, "test_scd2_nochange", business_keys=["customer_id"], compare_columns=["name", "country"])
    
    result = spark.table("test_scd2_nochange").collect()
    # Should still be 1 record since no changes
    assert len(result) == 1
    assert result[0]["is_current"] == True
    
    spark.sql("DROP TABLE IF EXISTS test_scd2_nochange")

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

def test_scd_type2_multiple_changes(spark):
    """Test SCD Type 2 handles multiple sequential changes correctly."""
    # Initial load
    data = [("P001", "Laptop", 999.99)]
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", StringType(), True)  # Using string for simplicity
    ])
    df = spark.createDataFrame(data, schema)
    
    df_scd = df \
        .withColumn("effective_date", to_date(lit("2023-01-01"))) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    df_scd.write.format("delta").mode("overwrite").saveAsTable("test_scd2_multi")
    
    # First price change
    change1 = [("P001", "Laptop", "1099.99")]
    df1 = spark.createDataFrame(change1, schema)
    merge_scd_type2(df1, "test_scd2_multi", business_keys=["product_id"], compare_columns=["name", "price"])
    
    # Second price change
    change2 = [("P001", "Laptop", "1199.99")]
    df2 = spark.createDataFrame(change2, schema)
    merge_scd_type2(df2, "test_scd2_multi", business_keys=["product_id"], compare_columns=["name", "price"])
    
    result = spark.table("test_scd2_multi").orderBy("effective_date").collect()
    
    # Should have 3 versions
    assert len(result) == 3
    assert result[0]["price"] == "999.99"
    assert result[0]["is_current"] == False
    assert result[1]["price"] == "1099.99"
    assert result[1]["is_current"] == False
    assert result[2]["price"] == "1199.99"
    assert result[2]["is_current"] == True
    
    spark.sql("DROP TABLE IF EXISTS test_scd2_multi")

