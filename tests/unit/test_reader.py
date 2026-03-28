"""
Unit tests for PostgreSQL reader module.

Tests read_from_postgres, execute_sql, and get_table_row_count functions.
"""

from unittest.mock import patch

import pytest


FAKE_JDBC_URL = "jdbc:postgresql://localhost:5432/testdb"
FAKE_PROPS = {"user": "test", "password": "secret", "driver": "org.postgresql.Driver"}


class TestReadFromPostgres:
    """Tests for read_from_postgres() function."""

    def test_basic_read_calls_jdbc_with_table(self, spark):
        """
        GIVEN: A table name with no partition or predicate args
        WHEN: read_from_postgres() is called
        THEN: spark.read.jdbc is called with the table name and connection props
        """
        from src.config.postgres.reader import read_from_postgres

        mock_df = spark.createDataFrame([(1,)], ["id"])

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=mock_df) as mock_jdbc,
        ):
            result = read_from_postgres(spark, "daily_active_users")

        mock_jdbc.assert_called_once_with(
            url=FAKE_JDBC_URL, table="daily_active_users", properties=FAKE_PROPS
        )
        assert result is mock_df

    def test_partitioned_read_calls_jdbc_with_partition_args(self, spark):
        """
        GIVEN: All partition parameters provided
        WHEN: read_from_postgres() is called with partition_column
        THEN: spark.read.jdbc is called with column/lowerBound/upperBound/numPartitions
        """
        from src.config.postgres.reader import read_from_postgres

        mock_df = spark.createDataFrame([(1,)], ["id"])

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=mock_df) as mock_jdbc,
        ):
            result = read_from_postgres(
                spark,
                "user_interactions",
                partition_column="interaction_id",
                num_partitions=8,
                lower_bound=0,
                upper_bound=10_000_000,
            )

        mock_jdbc.assert_called_once_with(
            url=FAKE_JDBC_URL,
            table="user_interactions",
            column="interaction_id",
            lowerBound=0,
            upperBound=10_000_000,
            numPartitions=8,
            properties=FAKE_PROPS,
        )
        assert result is mock_df

    def test_predicate_pushdown_calls_jdbc_with_predicates(self, spark):
        """
        GIVEN: A predicate_pushdown filter string
        WHEN: read_from_postgres() is called
        THEN: spark.read.jdbc is called with predicates list
        """
        from src.config.postgres.reader import read_from_postgres

        mock_df = spark.createDataFrame([(1,)], ["id"])
        predicate = "date >= '2023-01-01'"

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=mock_df) as mock_jdbc,
        ):
            result = read_from_postgres(spark, "daily_active_users", predicate_pushdown=predicate)

        mock_jdbc.assert_called_once_with(
            url=FAKE_JDBC_URL,
            table="daily_active_users",
            predicates=[predicate],
            properties=FAKE_PROPS,
        )
        assert result is mock_df

    def test_raises_when_partition_column_given_without_bounds(self, spark):
        """
        GIVEN: partition_column provided but num_partitions/lower_bound/upper_bound are missing
        WHEN: read_from_postgres() is called
        THEN: ValueError is raised before any JDBC call
        """
        from src.config.postgres.reader import read_from_postgres

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc") as mock_jdbc,
            pytest.raises(ValueError, match="partitioned reads"),
        ):
            read_from_postgres(spark, "user_interactions", partition_column="id")

        mock_jdbc.assert_not_called()

    def test_raises_when_partition_column_given_without_upper_bound(self, spark):
        """
        GIVEN: partition_column + num_partitions + lower_bound but no upper_bound
        WHEN: read_from_postgres() is called
        THEN: ValueError is raised
        """
        from src.config.postgres.reader import read_from_postgres

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            pytest.raises(ValueError, match="partitioned reads"),
        ):
            read_from_postgres(
                spark,
                "user_interactions",
                partition_column="id",
                num_partitions=4,
                lower_bound=0,
            )


class TestExecuteSql:
    """Tests for execute_sql() function."""

    def test_wraps_query_as_subquery_in_jdbc_call(self, spark):
        """
        GIVEN: A SQL query string
        WHEN: execute_sql() is called
        THEN: spark.read.jdbc is called with query wrapped as subquery alias
        """
        from src.config.postgres.reader import execute_sql

        mock_df = spark.createDataFrame([(5,)], ["cnt"])
        sql = "SELECT COUNT(*) as cnt FROM bounce_rates"

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=mock_df) as mock_jdbc,
        ):
            result = execute_sql(spark, sql)

        mock_jdbc.assert_called_once_with(
            url=FAKE_JDBC_URL,
            table=f"({sql}) as query",
            properties=FAKE_PROPS,
        )
        assert result is mock_df


class TestGetTableRowCount:
    """Tests for get_table_row_count() function."""

    def test_returns_count_from_jdbc_result(self, spark):
        """
        GIVEN: A valid table name
        WHEN: get_table_row_count() is called
        THEN: Returns the integer count from the JDBC count query
        """
        from src.config.postgres.reader import get_table_row_count

        count_df = spark.createDataFrame([(42,)], ["count"])

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=count_df),
        ):
            result = get_table_row_count(spark, "daily_active_users")

        assert result == 42

    def test_returns_zero_when_result_is_empty(self, spark):
        """
        GIVEN: A valid table name but JDBC returns no rows
        WHEN: get_table_row_count() is called
        THEN: Returns 0 instead of raising
        """
        from src.config.postgres.reader import get_table_row_count

        empty_df = spark.createDataFrame([], spark.createDataFrame([(0,)], ["count"]).schema)

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=empty_df),
        ):
            result = get_table_row_count(spark, "daily_active_users")

        assert result == 0

    def test_raises_for_invalid_table_name(self, spark):
        """
        GIVEN: A table name containing SQL-injection-style characters
        WHEN: get_table_row_count() is called
        THEN: ValueError is raised before any JDBC call is made
        """
        from src.config.postgres.reader import get_table_row_count

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc") as mock_jdbc,
            pytest.raises(ValueError, match="Invalid table name"),
        ):
            get_table_row_count(spark, "users; DROP TABLE users--")

        mock_jdbc.assert_not_called()

    def test_raises_for_table_name_starting_with_digit(self, spark):
        """
        GIVEN: A table name starting with a digit
        WHEN: get_table_row_count() is called
        THEN: ValueError is raised
        """
        from src.config.postgres.reader import get_table_row_count

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            pytest.raises(ValueError, match="Invalid table name"),
        ):
            get_table_row_count(spark, "123table")

    @pytest.mark.parametrize(
        "valid_name",
        ["daily_active_users", "user_interactions", "etl_job_runs", "a", "_private_table"],
    )
    def test_accepts_valid_table_names(self, spark, valid_name):
        """
        GIVEN: A syntactically valid table name
        WHEN: get_table_row_count() is called
        THEN: No ValueError is raised from the name validation
        """
        from src.config.postgres.reader import get_table_row_count

        count_df = spark.createDataFrame([(0,)], ["count"])

        with (
            patch(
                "src.config.postgres.reader.get_postgres_connection_props",
                return_value=(FAKE_JDBC_URL, FAKE_PROPS),
            ),
            patch.object(spark.read, "jdbc", return_value=count_df),
        ):
            result = get_table_row_count(spark, valid_name)

        assert result == 0
