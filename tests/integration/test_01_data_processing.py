"""
Integration tests for Job 01: Data Processing.

Tests the enrichment pipeline — join strategy selection, hot-key salting,
partition column injection, and argument parser configuration.
"""

from datetime import datetime

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


INTERACTIONS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("duration_ms", LongType(), nullable=False),
    ]
)

METADATA_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("country", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
    ]
)


class TestDataProcessingJob:
    """Smoke tests for DataProcessingJob initialization and argument parsing."""

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

    def test_partition_by_date_column_configured(self):
        """
        GIVEN: DataProcessingJob
        WHEN: Instantiated
        THEN: partition_by includes "date" so enriched Parquet is date-partitioned
        """
        from src.jobs import DataProcessingJob
        from src.schemas.columns import COL_DATE

        job = DataProcessingJob()
        assert COL_DATE in job.partition_by


class TestEnrichInteractions:
    """Tests for DataProcessingJob.enrich_interactions()."""

    def test_metadata_columns_appear_in_output(self, spark):
        """
        GIVEN: interactions for u001, u002 and metadata for both
        WHEN: enrich_interactions() is called
        THEN: Output contains metadata columns (country, device_type)
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        job.spark = spark

        interactions = spark.createDataFrame(
            [
                ("u001", datetime(2023, 1, 1, 10, 0), 5000),
                ("u002", datetime(2023, 1, 1, 11, 0), 3000),
            ],
            INTERACTIONS_SCHEMA,
        )
        metadata = spark.createDataFrame(
            [("u001", "US", "iPad"), ("u002", "UK", "iPhone")],
            METADATA_SCHEMA,
        )

        result = job.enrich_interactions(interactions, metadata)

        assert "country" in result.columns
        assert "device_type" in result.columns
        assert result.count() == 2

    def test_left_join_preserves_unmatched_interactions(self, spark):
        """
        GIVEN: u003 has interactions but no metadata entry
        WHEN: enrich_interactions() is called (left join)
        THEN: u003's row is retained with NULL metadata fields
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        job.spark = spark

        interactions = spark.createDataFrame(
            [
                ("u001", datetime(2023, 1, 1, 10, 0), 5000),
                ("u003", datetime(2023, 1, 1, 11, 0), 3000),
            ],
            INTERACTIONS_SCHEMA,
        )
        metadata = spark.createDataFrame(
            [("u001", "US", "iPad")],
            METADATA_SCHEMA,
        )

        result = job.enrich_interactions(interactions, metadata)

        assert result.count() == 2
        u003_rows = result.filter("user_id = 'u003'").collect()
        assert len(u003_rows) == 1
        assert u003_rows[0]["country"] is None

    def test_processing_timestamp_added(self, spark):
        """
        GIVEN: interactions DataFrame
        WHEN: enrich_interactions() is called
        THEN: Output includes processing_timestamp column
        """
        from src.jobs import DataProcessingJob
        from src.schemas.columns import COL_PROCESSING_TIMESTAMP

        job = DataProcessingJob()
        job.spark = spark

        interactions = spark.createDataFrame(
            [("u001", datetime(2023, 1, 1, 10, 0), 5000)],
            INTERACTIONS_SCHEMA,
        )
        metadata = spark.createDataFrame(
            [("u001", "US", "iPad")],
            METADATA_SCHEMA,
        )

        result = job.enrich_interactions(interactions, metadata)
        assert COL_PROCESSING_TIMESTAMP in result.columns

    def test_skewed_data_all_rows_preserved(self, spark):
        """
        GIVEN: u001 has 200 interactions (hot key); u002 has 2 (normal)
        WHEN: enrich_interactions() is called
        THEN: All 202 rows appear in output — hot-key salting must not drop data
        """
        from src.jobs import DataProcessingJob

        job = DataProcessingJob()
        job.spark = spark

        hot_key_rows = [("u001", datetime(2023, 1, 1, 10, 0), i) for i in range(200)]
        normal_rows = [
            ("u002", datetime(2023, 1, 1, 11, 0), 3000),
            ("u002", datetime(2023, 1, 1, 12, 0), 4000),
        ]
        interactions = spark.createDataFrame(hot_key_rows + normal_rows, INTERACTIONS_SCHEMA)
        metadata = spark.createDataFrame(
            [("u001", "US", "iPad"), ("u002", "UK", "iPhone")],
            METADATA_SCHEMA,
        )

        result = job.enrich_interactions(interactions, metadata)
        assert result.count() == 202
