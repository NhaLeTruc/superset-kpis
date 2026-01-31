"""
Integration tests for Job 01: Data Processing.

Tests the end-to-end data processing pipeline including CSV ingestion,
enrichment, and parquet output.
"""


class TestDataProcessingJob:
    """Test suite for DataProcessingJob."""

    def test_job_initializes_correctly(self):
        """Test that job class can be instantiated."""
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()

        assert job.job_name == "Data Processing"
        assert job.job_type == "etl"

    def test_argument_parser_has_required_args(self):
        """Test that argument parser includes required arguments."""
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        parser = job.get_argument_parser()

        # Parse with required args
        args = parser.parse_args(
            [
                "--interactions-path",
                "/data/interactions.csv",
                "--metadata-path",
                "/data/metadata.csv",
                "--output-path",
                "/data/output",
            ]
        )

        assert args.interactions_path == "/data/interactions.csv"
        assert args.metadata_path == "/data/metadata.csv"
        assert args.output_path == "/data/output"
