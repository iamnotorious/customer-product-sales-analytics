import pytest
from pyspark.sql import SparkSession
from datetime import date

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("SalesAnalytics_Tests") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def sample_orders_data(spark):
    """Sample order records extracted from Orders.json."""
    # Real sample data from Orders.json
    data = [
        (1, "CA-2016-122581", "21/8/2016", "25/8/2016", "Standard Class", "JK-15370", "FUR-CH-10002961", 7, 573.17, 0.3, 63.69),
        (2, "CA-2017-117485", "23/9/2017", "29/9/2017", "Standard Class", "BD-11320", "TEC-AC-10004659", 4, 291.96, 0.0, 102.19),
        (3, "US-2016-157490", "6/10/2016", "7/10/2016", "First Class", "LB-16795", "OFF-BI-10002824", 4, 17.0, 0.7, -14.92),
        (4, "CA-2015-111703", "2/7/2015", "9/7/2015", "Standard Class", "KB-16315", "OFF-PA-10003349", 3, 15.55, 0.2, 5.64),
        (5, "CA-2014-108903", "3/10/2014", "3/10/2014", "Same Day", "DO-13435", "TEC-AC-10003023", 3, 142.49, 0.2, -3.0)
    ]
    columns = ["Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", 
               "Customer ID", "Product ID", "Quantity", "Price", "Discount", "Profit"]
    return spark.createDataFrame(data, columns)

@pytest.fixture
def sample_products_data(spark):
    """Sample product records extracted from Products.csv."""
    # Real sample data from Products.csv
    data = [
        ("FUR-CH-10002961", "Furniture", "Chairs", "Leather Task Chair, Black", "New York", 81.882),
        ("TEC-AC-10004659", "Technology", "Accessories", "Imation Secure+ Hardware Encrypted USB 2.0 Flash Drive; 16GB", "Oklahoma", 72.99),
        ("OFF-BI-10002824", "Office Supplies", "Binders", "Recycled Easel Ring Binders", "Colorado", 4.25),
        ("OFF-PA-10003349", "Office Supplies", "Paper", "Xerox 1957", "Florida", 5.184),
        ("TEC-AC-10003023", "Technology", "Accessories", "Logitech G105 Gaming Keyboard", "Ohio", 47.496)
    ]
    columns = ["Product ID", "Category", "Sub-Category", "Product Name", "State", "Price per product"]
    return spark.createDataFrame(data, columns)

@pytest.fixture
def sample_customers_data(spark):
    """Sample customer records created based on Customer.xlsx structure."""
    # Sample customer data matching the Excel structure
    data = [
        ("JK-15370", "Jackson Kelly", "United States", "Louisville", "Kentucky", "South"),
        ("BD-11320", "Brian Davis", "United States", "Troy", "Michigan", "East"),
        ("LB-16795", "Laura Brown", "United States", "Miami", "Florida", "South"),
        ("KB-16315", "Kevin Baker", "United States", "Seattle", "Washington", "West"),
        ("DO-13435", "David Olson", "United States", "Portland", "Oregon", "West")
    ]
    columns = ["Customer ID", "Customer Name", "Country", "City", "State", "Region"]
    return spark.createDataFrame(data, columns)

@pytest.fixture
def sample_enriched_orders_data(spark):
    """Sample enriched order data after joining orders, products, and customers."""
    # Combined data representing enriched orders for Silver layer
    data = [
        ("CA-2016-122581", "21/8/2016", "JK-15370", "Jackson Kelly", "United States", "Kentucky", "South", 
         "FUR-CH-10002961", "Furniture", "Chairs", "Leather Task Chair, Black", 81.882, 7, 573.17, 0.3, 63.69),
        ("CA-2017-117485", "23/9/2017", "BD-11320", "Brian Davis", "United States", "Michigan", "East",
         "TEC-AC-10004659", "Technology", "Accessories", "Imation Secure+ Flash Drive", 72.99, 4, 291.96, 0.0, 102.19),
        ("US-2016-157490", "6/10/2016", "LB-16795", "Laura Brown", "United States", "Florida", "South",
         "OFF-BI-10002824", "Office Supplies", "Binders", "Recycled Easel Ring Binders", 4.25, 4, 17.0, 0.7, -14.92)
    ]
    columns = ["order_id", "order_date", "customer_id", "customer_name", "country", "state", "region",
               "product_id", "category", "sub_category", "product_name", "price_per_product", 
               "quantity", "price", "discount", "profit"]
    return spark.createDataFrame(data, columns)
