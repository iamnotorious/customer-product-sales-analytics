from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, round

def create_profit_aggregates(df: DataFrame) -> DataFrame:
    """
    Creates an aggregate table that shows profit by:
    - Year
    - Product Category
    - Product Sub Category
    - Customer
    """
    # Check if 'Year' column exists; if not, parse it from Order Date (assuming transformation added it)
    # The enrichment step should have handled this, but let's be safe.
    
    # Group by the specified dimensions
    # Calculate sum of Profit
    # Round to 2 decimal places
    
    agged = df.groupBy("Year", "Category", "Sub-Category", "Customer Name") \
              .agg(_sum("Profit").alias("Total Profit"))
    
    # Round the total profit
    agged = agged.withColumn("Total Profit", round(col("Total Profit"), 2))
    
    # Sort for better readability (optional but good practice)
    agged = agged.orderBy("Year", "Category", "Sub-Category", "Customer Name")
    
    return agged

def get_profit_by_year(df: DataFrame) -> DataFrame:
    return df.groupBy("Year").agg(round(_sum("Profit"), 2).alias("Total Profit")).orderBy("Year")

def get_profit_by_year_category(df: DataFrame) -> DataFrame:
    return df.groupBy("Year", "Category").agg(round(_sum("Profit"), 2).alias("Total Profit")).orderBy("Year", "Category")

def get_profit_by_customer(df: DataFrame) -> DataFrame:
    return df.groupBy("Customer Name").agg(round(_sum("Profit"), 2).alias("Total Profit")).orderBy("Customer Name")

def get_profit_by_customer_year(df: DataFrame) -> DataFrame:
    return df.groupBy("Customer Name", "Year").agg(round(_sum("Profit"), 2).alias("Total Profit")).orderBy("Customer Name", "Year")
