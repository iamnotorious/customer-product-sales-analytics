"""Null counts, duplicate detection, and quality reporting."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, when
import logging

logger = logging.getLogger(__name__)




def check_duplicates(df: DataFrame, key_columns: list) -> dict:
    """Return total, unique, and duplicate counts for key_columns."""
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
    """Build a dict with row count, column count, and per-column null counts."""
    logger.info(f"Quality report for {name}")
    
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
