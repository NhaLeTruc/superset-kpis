"""
Integration tests for Job 03: Performance Metrics.

Tests the performance metrics pipeline — percentile calculations,
device correlation, anomaly detection, and argument parser configuration.
"""

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ENRICHED_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("app_version", StringType(), nullable=False),
        StructField("device_type", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("duration_ms", LongType(), nullable=False),
    ]
)


def _make_enriched_df(spark, n_normal: int = 100, n_outliers: int = 0):
    """Helper: create a minimal enriched interactions DataFrame."""
    import random

    random.seed(42)
    rows = []
    for i in range(n_normal):
        rows.append(
            (
                f"u{i % 20:03d}",
                "1.0.0",
                "iPad" if i % 2 == 0 else "iPhone",
                datetime(2023, 1, (i % 28) + 1, 10, 0, 0),
                1000 + random.randint(-100, 100),
            )
        )
    for j in range(n_outliers):
        rows.append(
            (
                f"u{j:03d}",
                "1.0.0",
                "iPad",
                datetime(2023, 1, 29 + j, 10, 0, 0),
                50000,  # Far above baseline — detected as anomaly
            )
        )
    return spark.createDataFrame(rows, ENRICHED_SCHEMA)


class TestPerformanceMetricsJob:
    """Smoke tests for PerformanceMetricsJob initialization and argument parsing."""

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
        args = parser.parse_args(["--enriched-path", "/data/enriched.parquet"])
        assert args.enriched_path == "/data/enriched.parquet"

    def test_write_to_db_flag_defaults_false(self):
        """Test that write-to-db flag defaults to False."""
        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()
        parser = job.get_argument_parser()
        args = parser.parse_args(["--enriched-path", "/data/enriched.parquet"])
        assert args.write_to_db is False

    def test_table_mapping_covers_all_four_outputs(self):
        """
        GIVEN: PerformanceMetricsJob
        WHEN: get_table_mapping() is called
        THEN: Returns entries for all four output tables
        """
        from src.jobs import PerformanceMetricsJob

        mapping = PerformanceMetricsJob().get_table_mapping()
        assert set(mapping.keys()) == {
            "performance_by_version",
            "device_performance",
            "device_correlation",
            "performance_anomalies",
        }


class TestRenamePercentileColumns:
    """Tests for PerformanceMetricsJob._rename_percentile_columns()."""

    def test_renames_p50_p95_p99_to_duration_ms_names(self, spark):
        """
        GIVEN: DataFrame with raw p50, p95, p99 columns
        WHEN: _rename_percentile_columns() is called
        THEN: Columns are renamed to p50_duration_ms, p95_duration_ms, p99_duration_ms
              and the original short names are gone
        """
        from src.jobs import PerformanceMetricsJob
        from src.schemas.columns import (
            COL_P50_DURATION_MS,
            COL_P95_DURATION_MS,
            COL_P99_DURATION_MS,
        )

        job = PerformanceMetricsJob()
        df = spark.createDataFrame([(100.0, 200.0, 300.0)], ["p50", "p95", "p99"])

        result = job._rename_percentile_columns(df)

        assert COL_P50_DURATION_MS in result.columns
        assert COL_P95_DURATION_MS in result.columns
        assert COL_P99_DURATION_MS in result.columns
        assert "p50" not in result.columns
        assert "p95" not in result.columns
        assert "p99" not in result.columns

    def test_preserves_non_percentile_columns(self, spark):
        """
        GIVEN: DataFrame with p50/p95/p99 plus additional columns
        WHEN: _rename_percentile_columns() is called
        THEN: Non-percentile columns are unchanged
        """
        from src.jobs import PerformanceMetricsJob

        job = PerformanceMetricsJob()
        df = spark.createDataFrame(
            [("v1.0", "2023-01-01", 100.0, 200.0, 300.0)],
            ["app_version", "metric_date", "p50", "p95", "p99"],
        )

        result = job._rename_percentile_columns(df)
        assert "app_version" in result.columns
        assert "metric_date" in result.columns


class TestPerformanceMetricsPipeline:
    """Pipeline output-contract tests that exercise the transform layer."""

    def test_percentile_output_has_duration_ms_column_names(self, spark):
        """
        GIVEN: Enriched interactions with app_version and duration_ms
        WHEN: percentile calculation + column rename runs (as Job 3 does)
        THEN: Output columns match the DB schema names (p50_duration_ms, etc.)
        """
        from src.config.constants import DEFAULT_PERCENTILES
        from src.jobs import PerformanceMetricsJob
        from src.schemas.columns import (
            COL_APP_VERSION,
            COL_DURATION_MS,
            COL_METRIC_DATE,
            COL_P50_DURATION_MS,
            COL_P95_DURATION_MS,
            COL_P99_DURATION_MS,
        )
        from src.transforms.performance import calculate_percentiles

        df = _make_enriched_df(spark).withColumn(COL_METRIC_DATE, F.to_date(F.col("timestamp")))

        result = PerformanceMetricsJob()._rename_percentile_columns(
            calculate_percentiles(
                df=df,
                value_column=COL_DURATION_MS,
                group_by_columns=[COL_APP_VERSION, COL_METRIC_DATE],
                percentiles=DEFAULT_PERCENTILES,
            )
        )

        assert COL_P50_DURATION_MS in result.columns
        assert COL_P95_DURATION_MS in result.columns
        assert COL_P99_DURATION_MS in result.columns
        assert result.count() > 0

    def test_anomaly_detection_finds_outliers(self, spark):
        """
        GIVEN: 100 normal interactions + 2 extreme outliers (~50x baseline)
        WHEN: detect_anomalies_statistical() runs with z_threshold=3.0
        THEN: At least 2 anomaly rows are returned with correct columns
        """
        from src.schemas.columns import COL_APP_VERSION, COL_DURATION_MS
        from src.transforms.performance import detect_anomalies_statistical

        df = _make_enriched_df(spark, n_normal=100, n_outliers=2).withColumn(
            "metric_date", F.to_date(F.col("timestamp"))
        )

        result = detect_anomalies_statistical(
            df=df,
            value_column=COL_DURATION_MS,
            z_threshold=3.0,
            group_by_columns=[COL_APP_VERSION, "metric_date"],
        )

        assert result.count() >= 2
        assert "z_score" in result.columns
        assert "anomaly_type" in result.columns
