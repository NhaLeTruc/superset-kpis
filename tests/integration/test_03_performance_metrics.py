"""
Integration tests for Job 03: Performance Metrics.

Tests the performance metrics calculation job including latency analysis
and response time aggregations.
"""


class TestPerformanceMetricsJob:
    """Test suite for PerformanceMetricsJob."""

    def test_job_initializes_correctly(self):
        """Test that job class can be instantiated."""
        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()

        assert job.job_name == "Performance Metrics"
        assert job.job_type == "analytics"

    def test_argument_parser_has_required_args(self):
        """Test that argument parser includes required arguments."""
        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()
        parser = job.get_argument_parser()

        # Parse with required args
        args = parser.parse_args(
            [
                "--enriched-path",
                "/data/enriched.parquet",
            ]
        )

        assert args.enriched_path == "/data/enriched.parquet"

    def test_write_to_db_flag_defaults_false(self):
        """Test that write-to-db flag defaults to False."""
        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()
        parser = job.get_argument_parser()

        args = parser.parse_args(
            [
                "--enriched-path",
                "/data/enriched.parquet",
            ]
        )

        assert args.write_to_db is False
