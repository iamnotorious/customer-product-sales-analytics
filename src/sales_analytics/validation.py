"""
Data validation utilities for data quality checks.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, when
import logging

logger = logging.getLogger(__name__)

def validate_schema(df: DataFrame, required_columns: list) -> bool:
    """Check if required columns exist."""
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        logger.error(f"Missing columns: {missing_cols}")
        return False
    logger.info(f"Schema validated: {len(required_columns)} columns")
    return True


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

def validate_data_range(df: DataFrame, column: str, min_value=None, max_value=None) -> bool:
    """Check if values are within range."""
    if min_value is not None:
        below_min = df.filter(col(column) < min_value).count()
        if below_min > 0:
            logger.error(f"Column '{column}' has {below_min} values below minimum {min_value}")
            return False
    
    if max_value is not None:
        above_max = df.filter(col(column) > max_value).count()
        if above_max > 0:
            logger.error(f"Column '{column}' has {above_max} values above maximum {max_value}")
            return False
    
    logger.info(f"Range validated: {column}")
    return True

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
    
    logger.info(f"Quality Report: {name} ({report['row_count']} rows)")
    return report
