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

    def test_basic_write_calls_jdbc_with_table(self):
        """
        GIVEN: A DataFrame and a table name with no optional args
        WHEN: write_to_postgres() is called
        THEN: df.write.jdbc is called with the table name and connection props
        """
        from src.config.postgres.writer import write_to_postgres

        mock_df = MagicMock()

        with patch(
            "src.config.postgres.writer.get_postgres_connection_props",
            return_value=(FAKE_JDBC_URL, FAKE_PROPS),
        ):
            write_to_postgres(mock_df, "daily_active_users")

        mock_df.write.jdbc.assert_called_once()
        call_kwargs = mock_df.write.jdbc.call_args.kwargs
        assert call_kwargs["url"] == FAKE_JDBC_URL
        assert call_kwargs["table"] == "daily_active_users"
        assert call_kwargs["mode"] == "append"

    def test_mode_is_forwarded_to_jdbc(self):
        """
        GIVEN: mode="overwrite" is passed
        WHEN: write_to_postgres() is called
        THEN: df.write.jdbc receives mode="overwrite"
        """
        from src.config.postgres.writer import write_to_postgres

        mock_df = MagicMock()

        with patch(
            "src.config.postgres.writer.get_postgres_connection_props",
            return_value=(FAKE_JDBC_URL, FAKE_PROPS),
        ):
            write_to_postgres(mock_df, "power_users", mode="overwrite")

        call_kwargs = mock_df.write.jdbc.call_args.kwargs
        assert call_kwargs["mode"] == "overwrite"

    def test_num_partitions_none_does_not_repartition(self):
        """
        GIVEN: num_partitions=None (the default)
        WHEN: write_to_postgres() is called
        THEN: df.repartition is NOT called — the DataFrame is written as-is

        Verifies the `if num_partitions is not None:` guard (not `if num_partitions:`),
        which prevents accidental repartition when num_partitions=0 is passed.
        """
        from src.config.postgres.writer import write_to_postgres

        mock_df = MagicMock()

        with patch(
            "src.config.postgres.writer.get_postgres_connection_props",
            return_value=(FAKE_JDBC_URL, FAKE_PROPS),
        ):
            write_to_postgres(mock_df, "daily_active_users", num_partitions=None)

        mock_df.repartition.assert_not_called()

    def test_num_partitions_set_triggers_repartition(self):
        """
        GIVEN: num_partitions=4
        WHEN: write_to_postgres() is called
        THEN: df.repartition(4) is called before the JDBC write
        """
        from src.config.postgres.writer import write_to_postgres

        mock_df = MagicMock()
        mock_repartitioned = MagicMock()
        mock_df.repartition.return_value = mock_repartitioned

        with patch(
            "src.config.postgres.writer.get_postgres_connection_props",
            return_value=(FAKE_JDBC_URL, FAKE_PROPS),
        ):
            write_to_postgres(mock_df, "daily_active_users", num_partitions=4)

        mock_df.repartition.assert_called_once_with(4)
        mock_repartitioned.write.jdbc.assert_called_once()

    def test_reraises_exception_on_jdbc_failure(self):
        """
        GIVEN: df.write.jdbc raises an exception
        WHEN: write_to_postgres() is called
        THEN: The exception propagates to the caller
        """
        from src.config.postgres.writer import write_to_postgres

        mock_df = MagicMock()
        mock_df.write.jdbc.side_effect = RuntimeError("connection refused")

        with patch(  # noqa: SIM117
            "src.config.postgres.writer.get_postgres_connection_props",
            return_value=(FAKE_JDBC_URL, FAKE_PROPS),
        ):
            with pytest.raises(RuntimeError, match="connection refused"):
                write_to_postgres(mock_df, "daily_active_users")
