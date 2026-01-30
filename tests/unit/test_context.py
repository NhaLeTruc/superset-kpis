"""
Unit tests for monitoring context management.

Tests the create_monitoring_context function and accumulator initialization.
"""


class TestCreateMonitoringContext:
    """Test suite for create_monitoring_context function."""

    def test_creates_all_accumulators(self, spark):
        """Test that all required accumulators are created."""
        from src.utils.monitoring.context import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        assert "record_counter" in context
        assert "skipped_records" in context
        assert "data_quality_errors" in context
        assert "partition_skew" in context

    def test_record_counter_initialized_to_zero(self, spark):
        """Test that record_counter starts at 0."""
        from src.utils.monitoring.context import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        assert context["record_counter"].value == 0

    def test_skipped_records_initialized_to_zero(self, spark):
        """Test that skipped_records starts at 0."""
        from src.utils.monitoring.context import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        assert context["skipped_records"].value == 0

    def test_data_quality_errors_initialized_empty(self, spark):
        """Test that data_quality_errors starts as empty dict."""
        from src.utils.monitoring.context import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        assert context["data_quality_errors"].value == {}

    def test_partition_skew_initialized_correctly(self, spark):
        """Test that partition_skew has correct initial state."""
        from src.utils.monitoring.accumulators import _MIN_PARTITION_SENTINEL
        from src.utils.monitoring.context import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        skew_value = context["partition_skew"].value
        assert skew_value["max_partition_size"] == 0
        assert skew_value["min_partition_size"] == _MIN_PARTITION_SENTINEL
        assert skew_value["partition_count"] == 0

    def test_accumulators_can_be_updated(self, spark):
        """Test that accumulators can be updated after creation."""
        from src.utils.monitoring.context import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        # Update record counter
        context["record_counter"].add(100)
        assert context["record_counter"].value == 100

        # Update skipped records
        context["skipped_records"].add(5)
        assert context["skipped_records"].value == 5

        # Update data quality errors
        context["data_quality_errors"].add({"null_user_id": 3})
        assert context["data_quality_errors"].value == {"null_user_id": 3}

    def test_multiple_contexts_are_independent(self, spark):
        """Test that multiple contexts don't share accumulators."""
        from src.utils.monitoring.context import create_monitoring_context

        context1 = create_monitoring_context(spark.sparkContext, "job1")
        context2 = create_monitoring_context(spark.sparkContext, "job2")

        context1["record_counter"].add(100)
        context2["record_counter"].add(50)

        # Values should be independent
        assert context1["record_counter"].value == 100
        assert context2["record_counter"].value == 50

    def test_context_with_different_job_names(self, spark):
        """Test context creation with various job names."""
        from src.utils.monitoring.context import create_monitoring_context

        # Should not raise for any valid job name
        context1 = create_monitoring_context(spark.sparkContext, "data_processing")
        context2 = create_monitoring_context(spark.sparkContext, "user_engagement")
        context3 = create_monitoring_context(spark.sparkContext, "Job With Spaces")

        assert context1 is not None
        assert context2 is not None
        assert context3 is not None
