from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, when, lit, coalesce, round, year, to_date, current_timestamp, broadcast

def clean_text(df: DataFrame, column_name: str) -> DataFrame:
    """
    Cleans text based on refined name cleaning rules.
    1. Normalizes leetspeak (e.g. 1l->ll, 1->l).
    2. Removes numeric sequences (2+ digits).
    3. Replaces special characters/digits with spaces.
    4. Heals fragmented names (merges gaps > 2 spaces).
    5. Normalizes separators (collapses 1-2 spaces to single space).
    """
    from pyspark.sql.functions import trim
    
    # Step 0: Handle combined leetspeak (1l -> ll, 55 -> ss, 11 -> ll)
    df = df.withColumn(column_name, regexp_replace(col(column_name), "1l", "ll"))
    df = df.withColumn(column_name, regexp_replace(col(column_name), "11", "ll"))
    df = df.withColumn(column_name, regexp_replace(col(column_name), "55", "ss"))
    
    # Step 1: Remove sequences of 2 or more digits with Context-Awareness
    # Order matters to handle spaces correctly.
    
    # Case A: Digits surrounded by spaces -> Remove digits AND spaces (Merge names)
    # e.g. "Tho 12 mas" -> "Thomas"
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'\s+\d{2,}\s+', ''))
    
    # Case B: Digits preceded by space -> Remove digits AND preceding space (Merge names)
    # e.g. "Bi 876ll" -> "Bill"
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'\s+\d{2,}', ''))
    
    # Case C: Digits followed by space -> Remove digits, KEEP 1 space (Separate names)
    # e.g. "Gary567 Hansen" -> "Gary Hansen"
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'\d{2,}\s+', ' '))
    
    # Case D: Digits embedded/isolated -> Remove digits entirely
    # e.g. "Fra9876nk" -> "Frank", "5678Shirley" -> "Shirley"
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'\d{2,}', ''))
    
    # Step 2: Map isolated single digits (leetspeak)
    df = df.withColumn(column_name, regexp_replace(col(column_name), "1", "l"))
    df = df.withColumn(column_name, regexp_replace(col(column_name), "0", "o"))
    df = df.withColumn(column_name, regexp_replace(col(column_name), "5", "s"))
    
    # Rule 1: Replace SPECIAL CHARACTERS (non-alphanumeric) with a space
    # excluding single quotes
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'[^\p{L}\s\']', ' '))
    
    # Rule 2: Heuristic for Healing vs Separating (Post-processing)
    # - If gap is large (>= 3 spaces), assume it was noise -> Merge (Empty String)
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'\s{3,}', ''))
    
    # - If gap is small (1-2 spaces), assume it is a separator -> Normalize (Single Space)
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'\s+', ' '))
    
    # Final Polish: Trim whitespace AND specific leading/trailing punctuation/symbols
    # This handles "''Becky Pak" -> "Becky Pak" while keeping "O'Rourke"
    # (^[\W_]+) matches non-word chars at start, ([\W_]+$) matches at end.
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'(^[\W_]+)|([\W_]+$)', ''))
    
    return df.withColumn(column_name, trim(col(column_name)))

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

def add_audit_columns(df: DataFrame, source_file: str = None) -> DataFrame:
    """
    Adds audit columns to a DataFrame for data lineage tracking.
    
    Columns added:
        - created_at: Timestamp when the record was ingested.
        - source_file: Path of the source file (only if provided).
    """
    df = df.withColumn("created_at", current_timestamp())
    if source_file:
        df = df.withColumn("source_file", lit(source_file))
    return df

def deduplicate(df: DataFrame, key_columns: list = None) -> DataFrame:
    """
    Removes duplicate rows based on key columns.
    If key_columns is not provided, deduplicates on all columns.
    """
    if key_columns:
        return df.dropDuplicates(key_columns)
    return df.dropDuplicates()

def generate_surrogate_key(df: DataFrame, key_columns: list, sk_column_name: str = "sk") -> DataFrame:
    """
    Generates a deterministic surrogate key using MD5 hash of business key columns.
    
    Args:
        df: Input DataFrame.
        key_columns: Business key columns to hash.
        sk_column_name: Name for the surrogate key column.
    """
    from pyspark.sql.functions import md5, concat_ws
    return df.withColumn(sk_column_name, md5(concat_ws("||", *[col(c) for c in key_columns])))

def clean_dataset(
    df: DataFrame, 
    clean_text_cols: list = None, 
    clean_names_cols: list = None,
    handle_null_cols: list = None, 
    null_fill_value: str = "N/A",
    mandatory_cols: list = None
) -> DataFrame:
    """
    Generic function to clean a dataset based on provided rules.
    """
    cleaned_df = df
    
    # 1. Aggressive name cleaning (follow specific user rules)
    if clean_names_cols:
        for col_name in clean_names_cols:
            cleaned_df = clean_text(cleaned_df, col_name)

    # 2. Standard text cleaning (remove symbols, keep alphanumeric)
    if clean_text_cols:
        for col_name in clean_text_cols:
            cleaned_df = clean_text(cleaned_df, col_name)
            
    # 3. Handle Nulls
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
    join_type: str = "left",
    broadcast_right: bool = False
) -> DataFrame:
    """
    Generic function to join two dataframes.
    Optionally broadcasts the right DataFrame for Map-Side Join.
    """
    if broadcast_right:
        return left_df.join(broadcast(right_df), join_on, join_type)
    return left_df.join(right_df, join_on, join_type)



def parse_date_col(df: DataFrame, date_col: str, date_format: str, output_col: str = None) -> DataFrame:
    """
    Parses a string date column to a proper DateType.
    Expects valid date formats or environment configured to return null on error.
    """
    target_col = output_col if output_col else date_col
    return df.withColumn(target_col, to_date(col(date_col), date_format))
