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

def clean_dataset(
    df: DataFrame, 
    clean_text_cols: list = None, 
    handle_null_cols: list = None, 
    null_fill_value: str = "N/A",
    mandatory_cols: list = None
) -> DataFrame:
    """
    Generic function to clean a dataset based on provided rules.
    
    Args:
        df: Input DataFrame.
        clean_text_cols: List of column names to remove special characters from.
        handle_null_cols: List of column names to fill nulls in.
        null_fill_value: Value to replace nulls with (default "N/A").
        mandatory_cols: List of columns that must not be null (rows with nulls here will be dropped).
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

def enrich_orders(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Enriches orders with customer and product information.
    Calculates Profit (rounded).
    """
    # Perform Joins
    # Left join to keep all orders even if customer/product is missing (though in clean data they shouldn't be)
    enriched = orders_df.join(customers_df, "Customer ID", "left") \
                        .join(products_df, "Product ID", "left")
    
    # Select and Rename columns as needed
    # Ensure Profit is rounded to 2 decimal places
    enriched = enriched.withColumn("Profit", round(col("Profit"), 2))
    
    # Select specific columns for the final enriched table
    final_cols = [
        "Order ID", 
        "Order Date", 
        "Profit", 
        "Customer Name", 
        "Country", 
        "Category", 
        "Sub-Category",
        "Year" # Will need to extract Year from Order Date
    ]
    
    # Add Year column
    # Assuming Order Date is in format 'dd/MM/yyyy' or similar based on raw inspection
    # pyspark.sql.functions.to_date needs format
    # Let's handle date parsing in a separate step or here
    
    return enriched

# Helper for date parsing since raw data has '21/8/2016' format
from pyspark.sql.functions import to_date, year, format_number

def parse_order_dates(df: DataFrame) -> DataFrame:
    """
    Parses 'Order Date' from string (dd/MM/yyyy) to DateType and adds 'Year'.
    """
    # Spark 3.0+ pattern for dd/MM/yyyy
    df = df.withColumn("ParsedOrderDate", to_date(col("Order Date"), "d/M/y"))
    df = df.withColumn("Year", year(col("ParsedOrderDate")))
    return df
