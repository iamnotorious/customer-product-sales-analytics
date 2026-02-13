"""Bronze, Silver, and Gold notebook tests."""

import importlib
import sys
import os
from datetime import date
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from sales_analytics.transformation import (
    generate_surrogate_key,
)

# Add notebooks directory to path for local execution
current_dir = os.getcwd()
while True:
    if os.path.exists(os.path.join(current_dir, "notebooks")):
        sys.path.append(os.path.join(current_dir, "notebooks"))
        break
    parent = os.path.dirname(current_dir)
    if parent == current_dir:
        break
    current_dir = parent

try:
    bronze_nb = importlib.import_module("01_bronze")
    silver_nb = importlib.import_module("02_silver")
    gold_nb = importlib.import_module("03_gold")
except Exception as e:
    # Fallback for Databricks runtime where direct import is restricted
    if "Importing notebooks directly is not supported" in str(e):
        from databricks.sdk import WorkspaceClient
        # Initialize with environment auth
        w = WorkspaceClient()
        dbutils = w.dbutils

        bronze_nb = dbutils.import_notebook("01_bronze")
        silver_nb = dbutils.import_notebook("02_silver")
        gold_nb = dbutils.import_notebook("03_gold")
    else:
        raise e


# ---------------------------------------------------------------------------
# Silver Layer
# ---------------------------------------------------------------------------


CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("price_per_product", DoubleType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("ship_date", StringType(), True),
])

FACT_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("ship_mode", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("profit", DoubleType(), True),
])

GOLD_SCHEMA = StructType([
    StructField("order_year", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("profit", DoubleType(), True),
])

GOLD_AGG_SCHEMA = StructType([
    StructField("order_year", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("total_profit", DoubleType(), True),
])


class TestSilverTransformCustomers:
    """Customer cleaning, outlier removal, dedup, and surrogate key."""

    def test_cleans_names_and_generates_key(self, spark: SparkSession) -> None:

        data = [
            ("GH-14410", "Gary567 Hansen", "001-542-415-0246x314",
             "United States", "Chicago", "Illinois", "Central"),
            ("PW-19240", "Pierre Wener", "421.580.0902x9815",
             "United States", "Louisville", "Colorado", "West"),
        ]
        df = spark.createDataFrame(data, CUSTOMER_SCHEMA)


        result = silver_nb.transform_customers(df=df)
        rows = result.orderBy("customer_id").collect()


        assert len(rows) == 2
        gh_row = [r for r in rows if r["customer_id"] == "GH-14410"][0]
        assert gh_row["customer_name"] == "Gary Hansen"
        assert gh_row["phone"] == "(542) 415-0246 x314"
        pw_row = [r for r in rows if r["customer_id"] == "PW-19240"][0]
        assert pw_row["phone"] == "(421) 580-0902 x9815"
        assert "customer_key" in result.columns

    def test_fills_missing_geography(self, spark: SparkSession) -> None:
        # all geography columns null
        data = [
            ("DO-13435", "Denny Ordway", "-4531", None, None, None, None),
        ]
        df = spark.createDataFrame(data, CUSTOMER_SCHEMA)


        result = silver_nb.transform_customers(df=df)
        row = result.collect()[0]


        assert row["country"] == "N/A"
        assert row["city"] == "N/A"
        assert row["state"] == "N/A"
        assert row["region"] == "N/A"

    def test_deduplicates_by_customer_id(self, spark: SparkSession) -> None:
        # same customer_id twice
        data = [
            ("WB-21850", "William Brown", "576.093.6933x5404",
             "United States", "Anaheim", "California", "West"),
            ("WB-21850", "William Brown", "576.093.6933x5404",
             "United States", "Anaheim", "California", "West"),
        ]
        df = spark.createDataFrame(data, CUSTOMER_SCHEMA)


        result = silver_nb.transform_customers(df=df)


        assert result.count() == 1


class TestSilverTransformProducts:
    """Null category fill, dedup, and surrogate key for products."""

    def test_fills_missing_categories(self, spark: SparkSession) -> None:
        # one null sub_category, one null category
        data = [
            ("FUR-CH-10002961", "Furniture", None,
             "Leather Task Chair, Black", 81.882),
            ("TEC-AC-10004659", None, "Accessories",
             "Imation Secure+ Flash Drive", 72.99),
        ]
        df = spark.createDataFrame(data, PRODUCT_SCHEMA)


        result = silver_nb.transform_products(df=df)
        rows = result.orderBy("product_id").collect()


        assert rows[0]["sub_category"] == "N/A"
        assert rows[1]["category"] == "N/A"
        assert "product_key" in result.columns

    def test_deduplicates_by_product_id(self, spark: SparkSession) -> None:
        # same product_id, different prices
        data = [
            ("FUR-CH-10002961", "Furniture", "Chairs",
             "Leather Task Chair, Black", 81.882),
            ("FUR-CH-10002961", "Furniture", "Chairs",
             "Leather Task Chair, Black", 63.686),
        ]
        df = spark.createDataFrame(data, PRODUCT_SCHEMA)


        result = silver_nb.transform_products(df=df)


        assert result.count() == 1

    def test_preserves_all_categories(self, spark: SparkSession) -> None:
        # one product per category
        data = [
            ("FUR-CH-10002961", "Furniture", "Chairs",
             "Leather Task Chair, Black", 81.882),
            ("TEC-AC-10004659", "Technology", "Accessories",
             "Imation Flash Drive", 72.99),
            ("OFF-BI-10002824", "Office Supplies", "Binders",
             "Recycled Easel Ring Binders", 4.25),
        ]
        df = spark.createDataFrame(data, PRODUCT_SCHEMA)


        result = silver_nb.transform_products(df=df)


        assert result.count() == 3
        categories = [r["category"] for r in result.collect()]
        assert "Furniture" in categories
        assert "Technology" in categories
        assert "Office Supplies" in categories


class TestSilverTransformOrders:
    """Null-key drop and date parsing for orders."""

    def test_parses_dates(self, spark: SparkSession) -> None:

        data = [("CA-2016-122581", "JK-15370", "FUR-CH-10002961",
                 "21/8/2016", "25/8/2016")]
        df = spark.createDataFrame(data, ORDER_SCHEMA)


        result = silver_nb.transform_orders(df=df)
        row = result.collect()[0]


        assert row["order_date"] == date(2016, 8, 21)
        assert row["ship_date"] == date(2016, 8, 25)

    def test_drops_rows_with_null_keys(self, spark: SparkSession) -> None:
        # one valid row, three with a null key each
        data = [
            ("CA-2016-122581", "JK-15370", "FUR-CH-10002961",
             "21/8/2016", "25/8/2016"),
            (None, "BD-11320", "TEC-AC-10004659",
             "23/9/2017", "29/9/2017"),
            ("US-2016-157490", None, "OFF-BI-10002824",
             "6/10/2016", "7/10/2016"),
            ("CA-2015-111703", "KB-16315", None,
             "2/7/2015", "9/7/2015"),
        ]
        df = spark.createDataFrame(data, ORDER_SCHEMA)


        result = silver_nb.transform_orders(df=df)


        assert result.count() == 1
        assert result.collect()[0]["order_id"] == "CA-2016-122581"


class TestSilverFilterOrders:
    """Date-range filtering: with bounds and without."""

    def test_filters_by_2016_date_range(self, spark: SparkSession) -> None:
        # orders across 2014-2017
        data = [
            ("CA-2014-108903", date(2014, 10, 3)),
            ("CA-2016-122581", date(2016, 8, 21)),
            ("US-2016-157490", date(2016, 10, 6)),
            ("CA-2017-117485", date(2017, 9, 23)),
        ]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", DateType(), True),
        ])
        df = spark.createDataFrame(data, schema)


        result = silver_nb.filter_orders_by_date(
            df=df, start_date="2016-01-01", end_date="2016-12-31"
        )


        assert result.count() == 2

    def test_returns_all_when_no_dates(self, spark: SparkSession) -> None:

        data = [
            ("CA-2014-108903", date(2014, 10, 3)),
            ("CA-2017-117485", date(2017, 9, 23)),
        ]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", DateType(), True),
        ])
        df = spark.createDataFrame(data, schema)


        result = silver_nb.filter_orders_by_date(
            df=df, start_date="", end_date=""
        )


        assert result.count() == 2


class TestSilverBuildFactTable:
    """Surrogate keys, profit rounding, year derivation, column selection."""

    def test_generates_surrogate_keys(self, spark: SparkSession) -> None:

        data = [(
            "CA-2016-122581", date(2016, 8, 21), date(2016, 8, 25),
            "Standard Class", "JK-15370", "FUR-CH-10002961",
            7, 573.17, 0.3, 63.69,
        )]
        df = spark.createDataFrame(data, FACT_SCHEMA)


        result = silver_nb.build_fact_table(orders=df)
        row = result.collect()[0]


        assert "customer_key" in result.columns
        assert "product_key" in result.columns
        assert len(row["customer_key"]) == 32
        assert len(row["product_key"]) == 32

    def test_rounds_profit_to_two_decimals(self, spark: SparkSession) -> None:

        data = [(
            "CA-2016-122581", date(2016, 8, 21), date(2016, 8, 25),
            "Standard Class", "JK-15370", "FUR-CH-10002961",
            7, 573.17, 0.3, 63.6912,
        )]
        df = spark.createDataFrame(data, FACT_SCHEMA)


        result = silver_nb.build_fact_table(orders=df)
        row = result.collect()[0]


        assert row["profit"] == 63.69

    def test_adds_order_year(self, spark: SparkSession) -> None:

        data = [(
            "CA-2014-108903", date(2014, 10, 3), date(2014, 10, 3),
            "Same Day", "DO-13435", "TEC-AC-10003023",
            3, 142.49, 0.2, -3.0,
        )]
        df = spark.createDataFrame(data, FACT_SCHEMA)


        result = silver_nb.build_fact_table(orders=df)
        row = result.collect()[0]


        assert row["order_year"] == 2014

    def test_selects_only_fact_columns(self, spark: SparkSession) -> None:

        data = [(
            "CA-2017-117485", date(2017, 9, 23), date(2017, 9, 29),
            "Standard Class", "BD-11320", "TEC-AC-10004659",
            4, 291.96, 0.0, 102.19,
        )]
        df = spark.createDataFrame(data, FACT_SCHEMA)


        result = silver_nb.build_fact_table(orders=df)


        assert "customer_id" not in result.columns
        assert "product_id" not in result.columns
        assert "customer_key" in result.columns
        assert "product_key" in result.columns
        assert "order_id" in result.columns


class TestSilverBuildEnrichedOrders:
    """Fact + dimension join and null handling on unmatched keys."""

    def test_joins_customers_and_products(self, spark: SparkSession) -> None:
        # dims and fact share surrogate keys via generate_surrogate_key
        cust_df = spark.createDataFrame(
            [("JK-15370", "Jay Kimmel", "United States")],
            ["customer_id", "customer_name", "country"],
        )
        cust_df = generate_surrogate_key(cust_df, ["customer_id"], "customer_key")

        prod_df = spark.createDataFrame(
            [("FUR-CH-10002961", "Furniture", "Chairs")],
            ["product_id", "category", "sub_category"],
        )
        prod_df = generate_surrogate_key(prod_df, ["product_id"], "product_key")

        # Build fact table from an order
        order_df = spark.createDataFrame(
            [(
                "CA-2016-122581", date(2016, 8, 21), date(2016, 8, 25),
                "Standard Class", "JK-15370", "FUR-CH-10002961",
                7, 573.17, 0.3, 63.69,
            )],
            FACT_SCHEMA,
        )
        fact_df = silver_nb.build_fact_table(orders=order_df)


        result = silver_nb.build_enriched_orders(
            fact_df=fact_df, customers=cust_df, products=prod_df
        )
        row = result.collect()[0]


        assert row["customer_name"] == "Jay Kimmel"
        assert row["country"] == "United States"
        assert row["category"] == "Furniture"
        assert row["sub_category"] == "Chairs"
        assert row["order_year"] == 2016
        assert row["profit"] == 63.69

    def test_fills_nulls_from_failed_joins(self, spark: SparkSession) -> None:
        # order IDs that don't match any dimension
        cust_df = spark.createDataFrame(
            [("JK-15370", "Jay Kimmel", "United States")],
            ["customer_id", "customer_name", "country"],
        )
        cust_df = generate_surrogate_key(cust_df, ["customer_id"], "customer_key")

        prod_df = spark.createDataFrame(
            [("FUR-CH-10002961", "Furniture", "Chairs")],
            ["product_id", "category", "sub_category"],
        )
        prod_df = generate_surrogate_key(prod_df, ["product_id"], "product_key")

        # Fact with non-matching IDs
        order_df = spark.createDataFrame(
            [(
                "XX-9999-000000", date(2016, 1, 1), date(2016, 1, 5),
                "Standard Class", "UNKNOWN-CUST", "UNKNOWN-PROD",
                1, 10.0, 0.0, 5.0,
            )],
            FACT_SCHEMA,
        )
        fact_df = silver_nb.build_fact_table(orders=order_df)


        result = silver_nb.build_enriched_orders(
            fact_df=fact_df, customers=cust_df, products=prod_df
        )
        row = result.collect()[0]


        assert row["customer_name"] == "N/A"
        assert row["country"] == "N/A"
        assert row["category"] == "N/A"
        assert row["sub_category"] == "N/A"
        

class TestGoldAggregation:
    """Profit grouping by year, category, sub-category, and customer."""

    def test_aggregates_profit_by_groups(self, spark: SparkSession) -> None:
        # two rows in same group to verify summing
        data = [
            (2016, "Office Supplies", "Binders", "William Brown", 39.87),
            (2016, "Office Supplies", "Binders", "William Brown", 15.50),
            (2016, "Furniture", "Tables", "William Brown", 111.52),
            (2016, "Furniture", "Chairs", "Jay Kimmel", 63.69),
        ]
        df = spark.createDataFrame(data, GOLD_SCHEMA)


        result = gold_nb.calculate_profit_aggregates(df=df)
        rows = result.collect()


        assert result.count() == 3
        wb_binders = [
            r for r in rows
            if r["customer_name"] == "William Brown"
            and r["sub_category"] == "Binders"
        ][0]
        assert wb_binders["total_profit"] == 55.37

    def test_handles_negative_profit(self, spark: SparkSession) -> None:
        # one negative, one positive profit
        data = [
            (2016, "Office Supplies", "Binders", "Laurel Beltran", -14.92),
            (2017, "Technology", "Accessories", "Bill Donatelli", 102.19),
        ]
        df = spark.createDataFrame(data, GOLD_SCHEMA)


        result = gold_nb.calculate_profit_aggregates(df=df)
        rows = result.collect()


        assert result.count() == 2
        lb_row = [r for r in rows if r["customer_name"] == "Laurel Beltran"][0]
        assert lb_row["total_profit"] == -14.92

    def test_orders_by_group_columns(self, spark: SparkSession) -> None:

        data = [
            (2017, "Technology", "Accessories", "Bill Donatelli", 102.19),
            (2014, "Technology", "Accessories", "Denny Ordway", -3.0),
            (2016, "Furniture", "Chairs", "Jay Kimmel", 63.69),
        ]
        df = spark.createDataFrame(data, GOLD_SCHEMA)


        result = gold_nb.calculate_profit_aggregates(df=df)
        rows = result.collect()


        assert rows[0]["order_year"] == 2014
        assert rows[-1]["order_year"] == 2017


class TestGoldSqlQueries:
    """SQL aggregates: by year, year+category, customer, customer+year."""

    @pytest.fixture(autouse=True)
    def _setup_gold_view(self, spark: SparkSession) -> None:
        """Seed profit_aggregates temp view for SQL tests."""
        data = [
            (2016, "Office Supplies", "Binders", "William Brown", 39.87),
            (2016, "Furniture", "Tables", "William Brown", 111.52),
            (2016, "Technology", "Accessories", "William Brown", 25.19),
            (2017, "Office Supplies", "Art", "William Brown", 3.04),
            (2016, "Furniture", "Chairs", "Jay Kimmel", 63.69),
            (2016, "Office Supplies", "Binders", "Laurel Beltran", -14.92),
            (2017, "Technology", "Accessories", "Bill Donatelli", 102.19),
        ]
        df = spark.createDataFrame(data, GOLD_AGG_SCHEMA)
        df.createOrReplaceTempView("profit_aggregates")

    def test_profit_by_year(self, spark: SparkSession) -> None:

        result = spark.sql("""
            SELECT order_year, ROUND(SUM(total_profit), 2) as annual_profit
            FROM profit_aggregates
            GROUP BY order_year
            ORDER BY order_year
        """)
        rows = result.collect()


        # 2016: 39.87 + 111.52 + 25.19 + 63.69 + (-14.92) = 225.35
        # 2017: 3.04 + 102.19 = 105.23
        assert len(rows) == 2
        assert rows[0]["order_year"] == 2016
        assert rows[0]["annual_profit"] == 225.35
        assert rows[1]["order_year"] == 2017
        assert rows[1]["annual_profit"] == 105.23

    def test_profit_by_year_and_category(self, spark: SparkSession) -> None:

        result = spark.sql("""
            SELECT order_year, category,
                   ROUND(SUM(total_profit), 2) as category_profit
            FROM profit_aggregates
            GROUP BY order_year, category
            ORDER BY order_year, category
        """)
        rows = result.collect()


        # 2016: Furniture=111.52+63.69=175.21, Office Supplies=39.87+(-14.92)=24.95, Technology=25.19
        # 2017: Office Supplies=3.04, Technology=102.19
        assert len(rows) == 5
        fur_2016 = [r for r in rows if r["order_year"] == 2016
                    and r["category"] == "Furniture"][0]
        assert fur_2016["category_profit"] == 175.21
        os_2016 = [r for r in rows if r["order_year"] == 2016
                   and r["category"] == "Office Supplies"][0]
        assert os_2016["category_profit"] == 24.95

    def test_profit_by_customer(self, spark: SparkSession) -> None:

        result = spark.sql("""
            SELECT customer_name,
                   ROUND(SUM(total_profit), 2) as customer_profit
            FROM profit_aggregates
            GROUP BY customer_name
            ORDER BY customer_profit DESC
        """)
        rows = result.collect()


        # William Brown: 39.87+111.52+25.19+3.04 = 179.62
        # Bill Donatelli: 102.19
        # Jay Kimmel: 63.69
        # Laurel Beltran: -14.92
        assert len(rows) == 4
        assert rows[0]["customer_name"] == "William Brown"
        assert rows[0]["customer_profit"] == 179.62
        assert rows[-1]["customer_name"] == "Laurel Beltran"
        assert rows[-1]["customer_profit"] == -14.92

    def test_profit_by_customer_and_year(self, spark: SparkSession) -> None:

        result = spark.sql("""
            SELECT customer_name, order_year,
                   ROUND(SUM(total_profit), 2) as customer_annual_profit
            FROM profit_aggregates
            GROUP BY customer_name, order_year
            ORDER BY customer_name, order_year
        """)
        rows = result.collect()


        # William Brown/2016: 39.87+111.52+25.19 = 176.58
        # William Brown/2017: 3.04
        assert len(rows) == 5
        wb_2016 = [r for r in rows if r["customer_name"] == "William Brown"
                   and r["order_year"] == 2016][0]
        assert wb_2016["customer_annual_profit"] == 176.58
        wb_2017 = [r for r in rows if r["customer_name"] == "William Brown"
                   and r["order_year"] == 2017][0]
        assert wb_2017["customer_annual_profit"] == 3.04


class TestGoldMerge:
    """Gold table write with partition by order_year."""

    def test_writes_to_gold_table(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        mock_write = MagicMock()
        monkeypatch.setattr(gold_nb, "write_data_to_table", mock_write)
        mock_df = MagicMock(spec=DataFrame)


        gold_nb.merge_to_gold(df=mock_df)


        mock_write.assert_called_once_with(
            df=mock_df,
            mode="overwrite",
            table_name="sales.gold.sales_ecommerce_profit_aggregates",
            partition_by=["order_year"],
        )
