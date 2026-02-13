"""Delta I/O utility tests (mocked)."""

import pytest
from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from sales_analytics.utils import (
    merge_data,
    merge_scd_type2,
    optimize_table,
    write_data_to_table,
)


class TestWriteDataToTable:
    """Delta write: format, mode, partitioning, and error paths."""

    def test_writes_delta_to_named_table(self) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer


        write_data_to_table(df=mock_df, mode="overwrite", table_name="test_tbl")


        mock_df.write.format.assert_called_once_with("delta")
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.saveAsTable.assert_called_once_with("test_tbl")

    def test_applies_partitioning(self) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer


        write_data_to_table(
            df=mock_df,
            mode="overwrite",
            table_name="test_tbl",
            partition_by=["order_date"],
        )


        mock_writer.partitionBy.assert_called_once_with("order_date")
        mock_writer.saveAsTable.assert_called_once_with("test_tbl")

    def test_raises_when_no_table_name(self) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer


        with pytest.raises(ValueError, match="table_name must be provided"):
            write_data_to_table(df=mock_df, mode="overwrite")

    def test_propagates_write_error(self) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.side_effect = RuntimeError("Spark write error")


        with pytest.raises(RuntimeError, match="Spark write error"):
            write_data_to_table(df=mock_df, mode="overwrite", table_name="tbl")


class TestOptimizeTable:
    """OPTIMIZE SQL generation with optional ZORDER clause."""

    @patch("sales_analytics.utils.SparkSession")
    def test_runs_optimize_sql(self, mock_spark_cls: MagicMock) -> None:

        mock_spark = MagicMock()
        mock_spark_cls.getActiveSession.return_value = mock_spark


        optimize_table(table_name="db.schema.table")


        mock_spark.sql.assert_called_once_with("OPTIMIZE db.schema.table")

    @patch("sales_analytics.utils.SparkSession")
    def test_adds_zorder_clause(self, mock_spark_cls: MagicMock) -> None:

        mock_spark = MagicMock()
        mock_spark_cls.getActiveSession.return_value = mock_spark


        optimize_table(
            table_name="db.schema.table", zorder_columns=["col_a", "col_b"]
        )


        expected = "OPTIMIZE db.schema.table ZORDER BY (col_a, col_b)"
        mock_spark.sql.assert_called_once_with(expected)


class TestMergeData:
    """MERGE INTO SQL: match condition and update/insert clauses."""

    @patch("sales_analytics.utils.SparkSession")
    def test_generates_merge_sql(self, mock_spark_cls: MagicMock) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = ["id", "name", "value"]
        mock_spark = MagicMock()
        mock_spark_cls.getActiveSession.return_value = mock_spark


        merge_data(df=mock_df, table_name="target", merge_keys=["id"])


        mock_df.createOrReplaceTempView.assert_called_once_with("merge_source")
        sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO target" in sql
        assert "target.id = source.id" in sql
        assert "target.name = source.name" in sql

    @patch("sales_analytics.utils.SparkSession")
    def test_respects_custom_update_columns(self, mock_spark_cls: MagicMock) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = ["id", "name", "value"]
        mock_spark = MagicMock()
        mock_spark_cls.getActiveSession.return_value = mock_spark


        merge_data(
            df=mock_df,
            table_name="target",
            merge_keys=["id"],
            update_columns=["name"],
        )


        sql = mock_spark.sql.call_args[0][0]
        assert "target.name = source.name" in sql
        assert "target.value = source.value" not in sql


class TestMergeScdType2:
    """SCD2 merge integration test with actual tables."""

    @pytest.mark.xfail(reason="works on databricks but not on local spark")
    def test_scd2_execution_on_table(self, spark: SparkSession) -> None:
        import uuid
        import pyspark.sql.functions as F
        from pyspark.sql.types import StringType, StructField, StructType, BooleanType, DateType
        
        unique_id = str(uuid.uuid4()).replace("-", "_")
        table_name = f"test_scd2_{unique_id}"
        
        # Schema for the table (must include SCD columns)
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("effective_date", DateType(), True),
            StructField("end_date", DateType(), True),
            StructField("is_current", BooleanType(), True)
        ])
        
        # 1. Create existing table
        initial_data = [
            ("1", "Alice", date(2020, 1, 1), None, True),
        ]
        spark.createDataFrame(initial_data, schema).write.format("delta").saveAsTable(table_name)
        
        try:
            # 2. Update "Alice" to "Alice Cooper"
            input_schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ])
            update_data = [("1", "Alice Cooper")]
            update_df = spark.createDataFrame(update_data, input_schema)
            
            # Run the utility function
            merge_scd_type2(
                df=update_df,
                table_name=table_name,
                merge_keys=["id"],
                compare_columns=["name"]
            )
            
            # 3. Verify
            final_df = spark.table(table_name)
            rows = final_df.collect()
            
            assert len(rows) == 2, f"Expected 2 rows (1 current, 1 history), found {len(rows)}"
            
            alice_old = [r for r in rows if r["name"] == "Alice"][0]
            alice_new = [r for r in rows if r["name"] == "Alice Cooper"][0]
            
            assert alice_old["is_current"] == False
            assert alice_old["end_date"] is not None
            
            assert alice_new["is_current"] == True
            assert alice_new["end_date"] is None

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
