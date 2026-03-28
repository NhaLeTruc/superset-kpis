"""
Unit tests for PostgreSQL writer module.

Tests write_to_postgres function, including repartition behaviour
and JDBC write call construction.
"""

from unittest.mock import MagicMock, patch

import pytest


FAKE_JDBC_URL = "jdbc:postgresql://localhost:5432/testdb"
FAKE_PROPS = {"user": "test", "password": "secret", "driver": "org.postgresql.Driver"}


class TestWriteToPostgres:
    """Tests for write_to_postgres() function."""

    def test_basic_write_calls_jdbc_with_table(self, spark):
        """
        GIVEN: A DataFrame and a table name with no optional args
        WHEN: write_to_postgres() is called
        THEN: df.write.jdbc is called with the table name and connection props
        """
        from src.config.postgres.writer import write_to_postgres

        df = spark.createDataFrame([(1, "a")], ["id", "val"])

        mock_writer = MagicMock()
        with (
            patch(
                "src.config.postgres.writer.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(df, "write", mock_writer),
        ):
            write_to_postgres(df, "daily_active_users")

        mock_writer.jdbc.assert_called_once()
        call_kwargs = mock_writer.jdbc.call_args.kwargs
        assert call_kwargs["url"] == FAKE_JDBC_URL
        assert call_kwargs["table"] == "daily_active_users"
        assert call_kwargs["mode"] == "append"

    def test_mode_is_forwarded_to_jdbc(self, spark):
        """
        GIVEN: mode="overwrite" is passed
        WHEN: write_to_postgres() is called
        THEN: df.write.jdbc receives mode="overwrite"
        """
        from src.config.postgres.writer import write_to_postgres

        df = spark.createDataFrame([(1,)], ["id"])

        mock_writer = MagicMock()
        with (
            patch(
                "src.config.postgres.writer.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(df, "write", mock_writer),
        ):
            write_to_postgres(df, "power_users", mode="overwrite")

        call_kwargs = mock_writer.jdbc.call_args.kwargs
        assert call_kwargs["mode"] == "overwrite"

    def test_num_partitions_none_does_not_repartition(self, spark):
        """
        GIVEN: num_partitions=None (the default)
        WHEN: write_to_postgres() is called
        THEN: df.repartition is NOT called — the DataFrame is written as-is
        """
        from src.config.postgres.writer import write_to_postgres

        df = spark.createDataFrame([(1,)], ["id"])

        mock_writer = MagicMock()
        with (
            patch(
                "src.config.postgres.writer.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(df, "write", mock_writer),
            patch.object(df, "repartition") as mock_repartition,
        ):
            write_to_postgres(df, "daily_active_users", num_partitions=None)

        mock_repartition.assert_not_called()

    def test_num_partitions_set_triggers_repartition(self, spark):
        """
        GIVEN: num_partitions=4
        WHEN: write_to_postgres() is called
        THEN: df.repartition(4) is called before the JDBC write
        """
        from src.config.postgres.writer import write_to_postgres

        df = spark.createDataFrame([(1,)], ["id"])
        repartitioned_df = spark.createDataFrame([(1,)], ["id"])

        with (
            patch(
                "src.config.postgres.writer.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(df, "repartition", return_value=repartitioned_df) as mock_repartition,
            patch.object(repartitioned_df, "write") as mock_writer,
        ):
            write_to_postgres(df, "daily_active_users", num_partitions=4)

        mock_repartition.assert_called_once_with(4)
        mock_writer.jdbc.assert_called_once()

    def test_reraises_exception_on_jdbc_failure(self, spark):
        """
        GIVEN: df.write.jdbc raises an exception
        WHEN: write_to_postgres() is called
        THEN: The exception propagates to the caller
        """
        from src.config.postgres.writer import write_to_postgres

        df = spark.createDataFrame([(1,)], ["id"])

        mock_writer = MagicMock()
        mock_writer.jdbc.side_effect = RuntimeError("connection refused")

        with (
            patch(
                "src.config.postgres.writer.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(df, "write", mock_writer),
            pytest.raises(RuntimeError, match="connection refused"),
        ):
            write_to_postgres(df, "daily_active_users")
