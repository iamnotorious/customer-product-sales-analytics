"""
Custom exceptions for the sales analytics pipeline.
"""

class DataQualityError(Exception):
    """Raised when data quality validation fails."""
    pass

class SchemaValidationError(DataQualityError):
    """Raised when schema validation fails."""
    pass

class DataIngestionError(Exception):
    """Raised when data ingestion fails."""
    pass

class DataTransformationError(Exception):
    """Raised when data transformation fails."""
    pass

class DataWriteError(Exception):
    """Raised when writing data fails."""
    pass
