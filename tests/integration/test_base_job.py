"""
Integration tests for BaseAnalyticsJob abstract class.

Tests the shared infrastructure methods (validate_dataframe, write_to_database,
get_table_mapping defaults) that every job inherits.
"""

import importlib.util
import os

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType


def _load_job(filename: str, class_name: str):
    """Load a job class from a digit-prefixed module file."""
    path = os.path.join(os.path.dirname(__file__), "..", "..", "src", "jobs", filename)
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)


class TestValidateDataframe:
    """Tests for BaseAnalyticsJob.validate_dataframe()."""

    def test_raises_on_missing_required_column(self, spark):
        """
        GIVEN: A DataFrame missing the 'timestamp' column
        WHEN: validate_dataframe() is called with timestamp in required_columns
        THEN: Raises ValueError naming the missing column
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        df = spark.createDataFrame([("u001", 1000)], ["user_id", "duration_ms"])

        with pytest.raises(ValueError, match="timestamp"):
            job.validate_dataframe(df, required_columns=["user_id", "timestamp"])

    def test_returns_empty_warnings_when_valid(self, spark):
        """
        GIVEN: A DataFrame that satisfies all required columns with no NULLs
        WHEN: validate_dataframe() is called
        THEN: Returns an empty warnings list
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        df = spark.createDataFrame([("u001", 1000)], ["user_id", "duration_ms"])

        warnings = job.validate_dataframe(df, required_columns=["user_id", "duration_ms"])
        assert warnings == []

    def test_returns_warning_for_null_in_not_null_column(self, spark):
        """
        GIVEN: A DataFrame with a NULL in the user_id column
        WHEN: validate_dataframe() is called with user_id in not_null_columns
        THEN: Returns a non-empty warnings list mentioning NULLs
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=True),
                StructField("duration_ms", LongType(), nullable=True),
            ]
        )
        df = spark.createDataFrame([(None, 1000), ("u001", 2000)], schema=schema)

        warnings = job.validate_dataframe(
            df,
            required_columns=["user_id", "duration_ms"],
            not_null_columns=["user_id"],
        )
        assert len(warnings) > 0
        assert any("NULL" in w or "null" in w.lower() for w in warnings)

    def test_raises_when_neither_schema_nor_columns_provided(self, spark):
        """
        GIVEN: No schema and no required_columns argument
        WHEN: validate_dataframe() is called
        THEN: Raises ValueError
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        df = spark.createDataFrame([("u001",)], ["user_id"])

        with pytest.raises(ValueError):
            job.validate_dataframe(df)


class TestGetTableMapping:
    """Tests for the default and overridden get_table_mapping() behavior."""

    def test_base_default_returns_none(self):
        """
        GIVEN: A minimal concrete subclass that does not override get_table_mapping()
        WHEN: get_table_mapping() is called
        THEN: Returns None (base class signals no DB writes)
        """
        import argparse

        from pyspark.sql import DataFrame

        from src.jobs.base_job import BaseAnalyticsJob

        class _MinimalJob(BaseAnalyticsJob):
            def get_argument_parser(self) -> argparse.ArgumentParser:
                return argparse.ArgumentParser()

            def compute_metrics(self) -> dict[str, DataFrame]:
                return {}

        job = _MinimalJob(job_name="test", job_type="analytics")
        assert job.get_table_mapping() is None

    def test_data_processing_job_returns_none(self):
        """
        GIVEN: DataProcessingJob (ETL → Parquet, not DB)
        WHEN: get_table_mapping() is called
        THEN: Returns None
        """
        from src.jobs import DataProcessingJob

        assert DataProcessingJob().get_table_mapping() is None

    def test_engagement_job_returns_expected_tables(self):
        """
        GIVEN: UserEngagementJob (writes to DB)
        WHEN: get_table_mapping() is called
        THEN: Returns dict containing dau, mau, and cohort_retention keys
        """
        UserEngagementJob = _load_job("02_user_engagement.py", "UserEngagementJob")

        mapping = UserEngagementJob().get_table_mapping()
        assert mapping is not None
        assert "dau" in mapping
        assert "mau" in mapping
        assert "cohort_retention" in mapping

    def test_performance_job_returns_expected_tables(self):
        """
        GIVEN: PerformanceMetricsJob (writes to DB)
        WHEN: get_table_mapping() is called
        THEN: Returns dict containing performance_by_version, device_performance,
              device_correlation, and performance_anomalies keys
        """
        from src.jobs import PerformanceMetricsJob

        mapping = PerformanceMetricsJob().get_table_mapping()
        assert mapping is not None
        assert "performance_by_version" in mapping
        assert "device_performance" in mapping
        assert "device_correlation" in mapping
        assert "performance_anomalies" in mapping


class TestWriteToDatabase:
    """Tests for BaseAnalyticsJob.write_to_database()."""

    def test_raises_when_postgres_not_configured(self, spark):
        """
        GIVEN: POSTGRES_PASSWORD not set in environment
        WHEN: write_to_database() is called
        THEN: The underlying connection error propagates — write silently skipping is not allowed

        write_to_postgres() calls get_postgres_connection_props(), which raises
        ValueError when the password env var is absent. This test verifies that
        write_to_database() does not swallow that error.
        """
        from unittest.mock import patch

        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()
        df = spark.createDataFrame([(1,)], ["id"])
        metrics = {"performance_by_version": df}
        table_mapping = {"performance_by_version": "performance_by_version"}

        with (
            patch(
                "src.config.database_config.get_postgres_connection_props",
                side_effect=ValueError("POSTGRES_PASSWORD not set"),
            ),
            pytest.raises(ValueError, match="POSTGRES_PASSWORD"),
        ):
            job.write_to_database(metrics, table_mapping)

    def test_skips_metrics_not_in_table_mapping(self, spark, capsys):
        """
        GIVEN: metrics dict contains a key that has no entry in table_mapping
        WHEN: write_to_database() is called
        THEN: The unmapped metric is skipped with a warning printed to stdout;
              no exception is raised for the skipped metric
        """
        from unittest.mock import patch

        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()
        df = spark.createDataFrame([(1,)], ["id"])
        metrics = {"known_metric": df, "unknown_metric": df}
        table_mapping = {"known_metric": "some_table"}

        with patch("src.config.database_config.write_to_postgres") as mock_write:
            job.write_to_database(metrics, table_mapping)

        # known_metric written once; unknown_metric skipped
        assert mock_write.call_count == 1
        captured = capsys.readouterr()
        assert "unknown_metric" in captured.out
