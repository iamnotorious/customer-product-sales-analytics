"""
Data validation utilities for data quality checks.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, when
import logging

logger = logging.getLogger(__name__)




def check_duplicates(df: DataFrame, key_columns: list) -> dict:
    """Count duplicate records by key."""
    total_count = df.count()
    unique_count = df.select(*key_columns).distinct().count()
    duplicate_count = total_count - unique_count
    
    result = {
        "total_count": total_count,
        "unique_count": unique_count,
        "duplicate_count": duplicate_count
    }
    
    if duplicate_count > 0:
        logger.warning(f"Duplicates: {duplicate_count} ({key_columns})")
    else:
        logger.info(f"No duplicates ({key_columns})")
    
    return result



def generate_data_quality_report(df: DataFrame, name: str = "Dataset") -> dict:
    """Create quality report (rows, cols, nulls)."""
    logger.info(f"Generating data quality report for {name}")
    
    report = {
        "dataset_name": name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "null_counts": {}
    }
    
    # Check nulls for each column
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        report["null_counts"][column] = null_count
    
    return report
