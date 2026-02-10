from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

class Paths:
    # Used for installing the package in editable mode via notebooks
    PROJECT_ROOT = "/Workspace/Repos/sales_analytics/customer-product-sales-analytics"
    
    BASE_DATA_DIR = "/FileStore/tables/data" # Assumed Databricks path, adjustable
    
    # Source Paths (Local mapping for reference, in DBX these would be mounted)
    CUSTOMER_SOURCE = "dbfs:/FileStore/tables/data/Customer.xlsx"
    PRODUCT_SOURCE = "dbfs:/FileStore/tables/data/Products.csv"
    ORDER_SOURCE = "dbfs:/FileStore/tables/data/Orders.json"

    # Layer Paths
    BRONZE_BASE = "dbfs:/mnt/delta/bronze"
    SILVER_BASE = "dbfs:/mnt/delta/silver"
    GOLD_BASE = "dbfs:/mnt/delta/gold"

class Schemas:
    # Defined based on inspection of Products.csv and Orders.json
    
    PRODUCT_SCHEMA = StructType([
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price per product", DoubleType(), True) # Inferred as double
    ])

    ORDER_SCHEMA = StructType([
        StructField("Row ID", IntegerType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True), # format DD/MM/YYYY needs parsing
        StructField("Ship Date", StringType(), True),  # format DD/MM/YYYY needs parsing
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True)
    ])

    # Customer schema inferred from typical domain usage
    CUSTOMER_SCHEMA = StructType([
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal Code", StringType(), True),
        StructField("Region", StringType(), True)
    ])
