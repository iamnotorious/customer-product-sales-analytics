from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, when, lit, coalesce, round, year, to_date

def clean_text(df: DataFrame, column_name: str) -> DataFrame:
    """
    Removes special characters from a text column, keeping alphanumeric and spaces.
    Example: "Leather___Chair!!!" -> "Leather Chair"
    """
    # Regex explanation: [^a-zA-Z0-9\s] means 'not alphanumeric or whitespace'
    return df.withColumn(column_name, regexp_replace(col(column_name), r'[^a-zA-Z0-9\s]', ''))

def handle_nulls(df: DataFrame, columns: list, default_value: str = "N/A") -> DataFrame:
    """
    Fills null values in specified string columns with a default value.
    """
    fill_dict = {c: default_value for c in columns}
    return df.fillna(fill_dict)

def to_snake_case(df: DataFrame) -> DataFrame:
    """
    Converts all column names in the DataFrame to snake_case.
    """
    for col_name in df.columns:
        new_name = col_name.strip().lower().replace(' ', '_').replace('-', '_').replace('/', '_')
        df = df.withColumnRenamed(col_name, new_name)
    return df

def clean_dataset(
    df: DataFrame, 
    clean_text_cols: list = None, 
    handle_null_cols: list = None, 
    null_fill_value: str = "N/A",
    mandatory_cols: list = None
) -> DataFrame:
    """
    Generic function to clean a dataset based on provided rules.
    """
    cleaned_df = df
    
    # 1. Remove special characters
    if clean_text_cols:
        for col_name in clean_text_cols:
            cleaned_df = clean_text(cleaned_df, col_name)
            
    # 2. Handle Nulls
    if handle_null_cols:
        cleaned_df = handle_nulls(cleaned_df, handle_null_cols, null_fill_value)
        
    # 3. Enforce Mandatory Columns
    if mandatory_cols:
        cleaned_df = cleaned_df.dropna(subset=mandatory_cols)
        
    return cleaned_df

def join_dataframes(
    left_df: DataFrame, 
    right_df: DataFrame, 
    join_on: str, 
    join_type: str = "left"
) -> DataFrame:
    """
    Generic function to join two dataframes.
    """
    return left_df.join(right_df, join_on, join_type)

def calculate_metric(df: DataFrame, metric_col: str, round_places: int = 2) -> DataFrame:
    """
    Rounds a specific metric column.
    """
    return df.withColumn(metric_col, round(col(metric_col), round_places))

# Helper for date parsing since raw data has '21/8/2016' format
from pyspark.sql.functions import to_date, year, format_number

def parse_date_col(df: DataFrame, date_col: str, date_format: str, output_col: str = None) -> DataFrame:
    """
    Parses a string date column to a proper DateType.
    Optionally adds a Year column if requested (logic moved out or kept separate).
    """
    target_col = output_col if output_col else date_col
    return df.withColumn(target_col, to_date(col(date_col), date_format))

def add_year_col(df: DataFrame, date_col: str, year_col_name: str = "year") -> DataFrame:
    """
    Adds a year column extracted from a date column.
    """
    return df.withColumn(year_col_name, year(col(date_col)))
