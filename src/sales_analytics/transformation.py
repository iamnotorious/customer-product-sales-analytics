from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, when, lit, coalesce, round, year, to_date, current_timestamp, broadcast, concat

def clean_names(df: DataFrame, column_name: str, apply_title_case: bool = False) -> DataFrame:
    """
    Cleans text by removing unwanted characters while preserving legitimate separators.
    
    Strategy:
    1. Preserve only letters, spaces, and apostrophes
    2. Remove special chars/digits as noise (merge adjacent text)
    3. Preserve spaces that existed in original input
    4. Merge fragmented text caused by special characters and digits
    
    Args:
        df: Input DataFrame.
        column_name: Column to clean.
        apply_title_case: If True, applies proper title case (for names). Default False.
    """
    from pyspark.sql.functions import trim, udf
    from pyspark.sql.types import StringType as SparkStringType
    
    # Character substitution for word-start positions (uppercase)
    WORD_START_UPPER = {'1': 'L', '0': 'O', '5': 'S', '!': 'I', '@': 'A'}
    
    # Apply combined leetspeak patterns first
    # Examples: "Ji11 Stevenson" -> "Jill Stevenson", "Helen Wa55erman" -> "Helen Wasserman"
    for pattern, replacement in [('1l', 'll'), ('11', 'll'), ('55', 'ss')]:
        df = df.withColumn(column_name, regexp_replace(col(column_name), pattern, replacement))
    
    # Remove multi-digit sequences while preserving original spaces
    SPACE_MARKER = '__SPACE__'
    df = df.withColumn(column_name, regexp_replace(col(column_name), r' ', SPACE_MARKER))
    for pattern, replacement in [
        # "Tho   12 mas Boland" -> "Thomas Boland" (digits surrounded by spaces)
        (rf'{SPACE_MARKER}\d{{2,}}{SPACE_MARKER}', ''),
        # "Bi 876ll" -> "Bill" (digits after space)
        (rf'{SPACE_MARKER}\d{{2,}}', ''),
        # "Gary567 Hansen" -> "Gary Hansen" (digits before space - keep space)
        (rf'\d{{2,}}{SPACE_MARKER}', SPACE_MARKER),
        # "Fra9876nk" -> "Frank" (embedded digits)
        (r'\d{2,}', '')
    ]:
        df = df.withColumn(column_name, regexp_replace(col(column_name), pattern, replacement))
    df = df.withColumn(column_name, regexp_replace(col(column_name), SPACE_MARKER, ' '))
    
    # Character substitutions with case awareness
    # Examples: "N0ra Paige" -> "Nora Paige" (word start), "Willin0009gham" -> "Willingham" (mid-word)
    for char, upper in WORD_START_UPPER.items():
        df = df.withColumn(column_name, regexp_replace(col(column_name), rf'(^|\s){char}', f'$1{upper}'))
    # "C@thy Armstrong" -> "Cathy Armstrong", "Karen Dan!els" -> "Karen Daniels"
    for char, lower in [('1', 'l'), ('0', 'o'), ('5', 's'), ('!', 'i'), ('@', 'a')]:
        df = df.withColumn(column_name, regexp_replace(col(column_name), char, lower))
    
    # Remove all special characters (keep only letters, spaces, apostrophes)
    # Examples: "Pete@#$ Takahito" -> "Petea Takahito", "Mary O'Rourke" -> "Mary O'Rourke" (keeps apostrophe)
    df = df.withColumn(column_name, regexp_replace(col(column_name), r"[^\p{L}\s']", ''))
    
    # Space normalization and fragment merging
    patterns = [
        # "Gasti  neau" -> "Gastineau" (word + double-space + lowercase fragment)
        (r'([A-Za-z]+)\s{2,}([a-z])', '$1$2'),
        # "B         ecky Martin" -> "Becky Martin" (3+ spaces merge)
        (r'\s{3,}', ''),
        # "Shahi  Hopkins" -> "Shahi Hopkins" (normalize double space to single)
        (r'\s{2}', ' '),
        # "''Becky Pak" -> "Becky Pak" (remove leading/trailing apostrophes)
        (r"^'+|'+$", ''),
        # "C thy" -> "Cthy" (single uppercase + lowercase)
        (r'\b([A-Z])\s+([a-z])', '$1$2'),
        # "Dan els" -> "Danels" (lowercase + 1-2 char fragment at end)
        (r'([a-z])\s+([a-z]{1,2})\b', '$1$2'),
        # "Kat rina" -> "Katrina" (short capitalized word + lowercase fragment)
        (r'\b([A-Z][a-z]{1,2})\s+([a-z]+)', '$1$2')
    ]
    for pattern, replacement in patterns:
        df = df.withColumn(column_name, regexp_replace(col(column_name), pattern, replacement))
    
    # Clean leading/trailing spaces
    df = df.withColumn(column_name, trim(col(column_name)))
    
    # Apply title case for names if requested
    # Examples: "john DOE" -> "John Doe", "mary o'rourke" -> "Mary O'Rourke"
    if apply_title_case:
        def proper_case(text: str) -> str:
            if not text:
                return text
            import re
            result = text.title()
            # Fix apostrophe capitalization: "O'rourke" -> "O'Rourke", "D'angelo" -> "D'Angelo"
            return re.sub(r"'([a-z])", lambda m: f"'{m.group(1).upper()}", result)
        
        proper_case_udf = udf(proper_case, SparkStringType())
        df = df.withColumn(column_name, proper_case_udf(col(column_name)))
    
    return df

def clean_phone(df: DataFrame, column_name: str, remove_errors: bool = True) -> DataFrame:
    """
    Cleans phone numbers to format: (xxx) xxx-xxxx xEXT
    Handles various formats, country codes (001), extensions, and removes errors.
    
    Examples:
        "421.580.0902x9815" -> "(421) 580-0902 x9815"
        "001-542-415-0246x314" -> "(542) 415-0246 x314"
        "7185624866" -> "(718) 562-4866"
        "#ERROR!" -> None
        "-6181" -> None
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType as SparkStringType
    import re
    
    def format_phone(phone: str) -> str:
        # Remove invalid entries: #ERROR!, empty strings, negative numbers
        if not phone or phone in ('#ERROR!', '') or phone.startswith('-'):
            return None
        
        # Extract extension: "x9815" -> "9815"
        ext_match = re.search(r'x(\d+)', phone)
        ext = ext_match.group(1) if ext_match else None
        
        # Remove extension and extract digits: "421.580.0902" -> "4215800902"
        phone_no_ext = re.sub(r'x\d+', '', phone)
        digits = re.sub(r'\D', '', phone_no_ext)
        
        # Remove country code: "0015424150246" -> "5424150246"
        if digits.startswith('001'):
            digits = digits[3:]
        
        # Validate minimum length (need at least 10 digits)
        if len(digits) < 10:
            return None
        
        # Format as (xxx) xxx-xxxx
        if len(digits) == 10:
            formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        else:
            # Handle extra digits: (xxx) xxx-xxxx EXTRA
            formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:10]} {digits[10:]}"
        
        # Add extension if present
        return f"{formatted} x{ext}" if ext else formatted
    
    phone_udf = udf(format_phone, SparkStringType())
    return df.withColumn(column_name, phone_udf(col(column_name)))


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

def clean_customer_names(df: DataFrame, name_column: str = "customer_name") -> DataFrame:
    """
    Cleans customer name column by removing corruption and applying title case.
    
    Example:
        "Gary567 Hansen" -> "Gary Hansen"
        "C@thy Armstrong" -> "Cathy Armstrong"
    """
    return clean_names(df, name_column, apply_title_case=True)


def clean_customer_phones(df: DataFrame, phone_column: str = "phone") -> DataFrame:
    """
    Standardizes phone numbers to format: (xxx) xxx-xxxx xEXT
    
    Example:
        "421.580.0902x9815" -> "(421) 580-0902 x9815"
        "#ERROR!" -> None
    """
    return clean_phone(df, phone_column)


def fill_missing_values(df: DataFrame, columns: list, default_value: str = "Unknown") -> DataFrame:
    """
    Fills null values in specified columns with a default value.
    
    Args:
        df: Input DataFrame.
        columns: List of columns to fill nulls.
        default_value: Value to use for null replacement.
    
    Example:
        Input: [("John", None), ("Jane", "USA")]
        columns: ["country"]
        Output: [("John", "Unknown"), ("Jane", "USA")]
    """
    return handle_nulls(df, columns, default_value)

def join_dataframes(
    left_df: DataFrame, 
    right_df: DataFrame, 
    join_on: str, 
    join_type: str = "left",
    broadcast_right: bool = False
) -> DataFrame:
    """
    Joins two DataFrames on specified column.
    Optionally broadcasts the right DataFrame for map-side join.
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
