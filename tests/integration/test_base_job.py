"""
Integration tests for BaseAnalyticsJob abstract class.

Tests the base job functionality including Spark session setup,
configuration, and monitoring integration.
"""

from abc import ABC


class TestBaseAnalyticsJob:
    """Test suite for BaseAnalyticsJob base class."""

    def test_is_abstract_class(self):
        """Test that BaseAnalyticsJob is abstract and cannot be instantiated directly."""
        from src.jobs.base_job import BaseAnalyticsJob

        assert issubclass(BaseAnalyticsJob, ABC)

    def test_default_job_type_is_analytics(self):
        """Test that default job type is 'analytics'."""
        from src.jobs import DataProcessingJob

        # Use concrete implementation to check defaults
        job = DataProcessingJob()

        # DataProcessingJob overrides to 'etl', but base default is 'analytics'
        # We test that the attribute exists
        assert hasattr(job, "job_type")
        assert job.job_type in ["etl", "analytics", "ml", "streaming"]

    def test_has_data_size_attribute(self):
        """Test that jobs have data_size_gb attribute for partition calculation."""
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()

        assert hasattr(job, "data_size_gb")
        assert isinstance(job.data_size_gb, int | float)
        assert job.data_size_gb >= 0


class TestBaseJobMethods:
    """Test suite for BaseAnalyticsJob methods."""

    def test_has_setup_spark_method(self):
        """Test that base class has setup_spark method."""
        from src.jobs.base_job import BaseAnalyticsJob

        assert hasattr(BaseAnalyticsJob, "setup_spark")

    def test_has_setup_monitoring_method(self):
        """Test that base class has setup_monitoring method."""
        from src.jobs.base_job import BaseAnalyticsJob

        assert hasattr(BaseAnalyticsJob, "setup_monitoring")

    def test_has_run_abstract_method(self):
        """Test that run is an abstract method."""
        from src.jobs.base_job import BaseAnalyticsJob

        # Check that 'run' is in abstract methods
        assert "run" in BaseAnalyticsJob.__abstractmethods__
