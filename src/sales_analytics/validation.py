"""
Data validation utilities for data quality checks.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, when
import logging

logger = logging.getLogger(__name__)

def validate_schema(df: DataFrame, required_columns: list) -> bool:
    """
    Validate that DataFrame contains all required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if all required columns exist, False otherwise
    """
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return False
    logger.info(f"Schema validation passed. All required columns present.")
    return True

def check_null_percentage(df: DataFrame, column: str, threshold: float = 0.5) -> bool:
    """
    Check if null percentage in a column exceeds threshold.
    
    Args:
        df: DataFrame to check
        column: Column name
        threshold: Maximum allowed null percentage (0-1)
        
    Returns:
        True if null percentage is within threshold, False otherwise
    """
    total_count = df.count()
    if total_count == 0:
        logger.warning(f"DataFrame is empty")
        return True
        
    null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
    null_percentage = null_count / total_count
    
    if null_percentage > threshold:
        logger.error(f"Column '{column}' has {null_percentage:.2%} nulls (threshold: {threshold:.2%})")
        return False
    
    logger.info(f"Column '{column}' null check passed ({null_percentage:.2%} nulls)")
    return True

def check_duplicates(df: DataFrame, key_columns: list) -> dict:
    """
    Check for duplicate records based on key columns.
    
    Args:
        df: DataFrame to check
        key_columns: List of columns that define uniqueness
        
    Returns:
        Dictionary with total_count, unique_count, and duplicate_count
    """
    total_count = df.count()
    unique_count = df.select(key_columns).distinct().count()
    duplicate_count = total_count - unique_count
    
    result = {
        "total_count": total_count,
        "unique_count": unique_count,
        "duplicate_count": duplicate_count
    }
    
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate records based on {key_columns}")
    else:
        logger.info(f"No duplicates found based on {key_columns}")
    
    return result

def validate_data_range(df: DataFrame, column: str, min_value=None, max_value=None) -> bool:
    """
    Validate that numeric column values are within expected range.
    
    Args:
        df: DataFrame to check
        column: Column name
        min_value: Minimum expected value (optional)
        max_value: Maximum expected value (optional)
        
    Returns:
        True if all values are within range, False otherwise
    """
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
    
    logger.info(f"Column '{column}' range validation passed")
    return True

def generate_data_quality_report(df: DataFrame, name: str = "Dataset") -> dict:
    """
    Generate comprehensive data quality report.
    
    Args:
        df: DataFrame to analyze
        name: Name of the dataset
        
    Returns:
        Dictionary containing quality metrics
    """
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
    
    logger.info(f"Data quality report generated for {name}: {report['row_count']} rows, {report['column_count']} columns")
    return report
