"""
Unit tests for monitoring reporting and formatting functions.

Tests format_monitoring_summary, log_monitoring_summary, and with_monitoring decorator.
"""

import logging

import pytest


class TestFormatMonitoringSummary:
    """Test suite for format_monitoring_summary function."""

    def test_includes_job_name(self, spark):
        """Test that summary includes the job name."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        summary = format_monitoring_summary(context, "My Test Job")

        assert "My Test Job" in summary

    def test_includes_record_counts(self, spark):
        """Test that summary includes record counts."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        context["record_counter"].add(1000)
        context["skipped_records"].add(50)

        summary = format_monitoring_summary(context, "Test Job")

        assert "1,000" in summary  # Formatted with commas
        assert "50" in summary

    def test_includes_skipped_percentage(self, spark):
        """Test that summary calculates skipped percentage."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        context["record_counter"].add(900)
        context["skipped_records"].add(100)  # 10% skipped

        summary = format_monitoring_summary(context, "Test Job")

        # Should show percentage
        assert "10.00%" in summary or "10%" in summary

    def test_includes_data_quality_errors(self, spark):
        """Test that summary includes data quality error breakdown."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        context["record_counter"].add(1000)
        context["data_quality_errors"].add({"null_user_id": 5, "invalid_timestamp": 3})

        summary = format_monitoring_summary(context, "Test Job")

        assert "null_user_id" in summary
        assert "invalid_timestamp" in summary

    def test_includes_partition_skew_info(self, spark):
        """Test that summary includes partition skew information."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        context["record_counter"].add(1000)
        context["partition_skew"].add(
            {"max_partition_size": 500, "min_partition_size": 100, "partition_count": 4}
        )

        summary = format_monitoring_summary(context, "Test Job")

        assert "500" in summary  # Max partition size
        assert "100" in summary  # Min partition size
        assert "4" in summary  # Partition count

    def test_handles_zero_records(self, spark):
        """Test that summary handles zero records gracefully."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        # Don't add any records

        summary = format_monitoring_summary(context, "Test Job")

        # Should not raise and should include zeros
        assert "0" in summary

    def test_skew_ratio_warning(self, spark):
        """Test that high skew ratio triggers warning."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        context["record_counter"].add(1000)
        # High skew: 1000 / 100 = 10x
        context["partition_skew"].add(
            {"max_partition_size": 1000, "min_partition_size": 100, "partition_count": 2}
        )

        summary = format_monitoring_summary(context, "Test Job")

        assert "WARNING" in summary or "skew" in summary.lower()


class TestLogMonitoringSummary:
    """Test suite for log_monitoring_summary function."""

    def test_logs_to_logger(self, spark, caplog):
        """Test that summary is logged to logger."""
        from src.utils.monitoring.context import create_monitoring_context
        from src.utils.monitoring.reporting import log_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")
        context["record_counter"].add(500)

        with caplog.at_level(logging.INFO):
            log_monitoring_summary(context, "Test Job")

        # Check that something was logged
        assert len(caplog.records) > 0


class TestWithMonitoringDecorator:
    """Test suite for with_monitoring decorator."""

    def test_decorator_logs_start(self, caplog):
        """Test that decorator logs operation start."""
        from src.utils.monitoring.reporting import with_monitoring

        @with_monitoring("test_operation")
        def sample_function():
            return 42

        with caplog.at_level(logging.INFO):
            result = sample_function()

        assert result == 42
        assert any("Starting" in record.message for record in caplog.records)
        assert any("test_operation" in record.message for record in caplog.records)

    def test_decorator_logs_completion(self, caplog):
        """Test that decorator logs operation completion."""
        from src.utils.monitoring.reporting import with_monitoring

        @with_monitoring("test_operation")
        def sample_function():
            return "success"

        with caplog.at_level(logging.INFO):
            sample_function()

        assert any("Completed" in record.message for record in caplog.records)

    def test_decorator_logs_exception(self, caplog):
        """Test that decorator logs exceptions."""
        from src.utils.monitoring.reporting import with_monitoring

        @with_monitoring("failing_operation")
        def failing_function():
            raise ValueError("Test error")

        with caplog.at_level(logging.ERROR), pytest.raises(ValueError):
            failing_function()

        assert any("Failed" in record.message for record in caplog.records)

    def test_decorator_preserves_function_return(self):
        """Test that decorator preserves function return value."""
        from src.utils.monitoring.reporting import with_monitoring

        @with_monitoring("test_operation")
        def function_with_return():
            return {"key": "value", "count": 42}

        result = function_with_return()

        assert result == {"key": "value", "count": 42}

    def test_decorator_preserves_function_args(self):
        """Test that decorator preserves function arguments."""
        from src.utils.monitoring.reporting import with_monitoring

        @with_monitoring("test_operation")
        def function_with_args(a, b, c=None):
            return (a, b, c)

        result = function_with_args(1, 2, c="three")

        assert result == (1, 2, "three")

    def test_decorator_reraises_exception(self):
        """Test that decorator re-raises exceptions after logging."""
        from src.utils.monitoring.reporting import with_monitoring

        @with_monitoring("test_operation")
        def failing_function():
            raise RuntimeError("Original error")

        with pytest.raises(RuntimeError, match="Original error"):
            failing_function()
