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
    """SCD2 merge: expire old rows and insert new versions."""

    @patch("sales_analytics.utils.SparkSession")
    def test_generates_scd2_merge_sql(self, mock_spark_cls: MagicMock) -> None:

        mock_df = MagicMock(spec=DataFrame)
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = [
            "id", "name", "effective_date", "end_date", "is_current"
        ]
        mock_spark = MagicMock()
        mock_spark_cls.getActiveSession.return_value = mock_spark


        merge_scd_type2(
            df=mock_df,
            table_name="dim_table",
            merge_keys=["id"],
            compare_columns=["name"],
        )


        mock_df.createOrReplaceTempView.assert_called_once_with("scd2_source")
        sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO dim_table" in sql
        assert "target.is_current = true" in sql
        assert "target.is_current = false" in sql
