"""
Integration tests for pipeline edge cases and error handling.

Tests data lineage and error handling scenarios.
"""

from datetime import datetime

from pyspark.sql import functions as F


class TestPipelineEdgeCases:
    """Edge case and error handling tests for pipeline."""

    def test_pipeline_data_lineage(
        self, spark, test_data_paths, sample_interactions_data, sample_metadata_data
    ):
        """Test data flows correctly through entire pipeline without loss."""
        from src.transforms.engagement import calculate_dau
        from src.transforms.join import identify_hot_keys, optimized_join
        from src.transforms.session import calculate_session_metrics

        # Track record counts through pipeline
        original_count = sample_interactions_data.count()

        # After enrichment
        hot_keys = identify_hot_keys(sample_interactions_data, "user_id")
        enriched_df = optimized_join(
            sample_interactions_data, sample_metadata_data, "user_id", "inner", hot_keys
        )
        enriched_count = enriched_df.count()

        assert enriched_count == original_count, (
            f"Enrichment should not lose data: {original_count} -> {enriched_count}"
        )

        # Session metrics should be created from interactions
        session_metrics_df = calculate_session_metrics(
            sample_interactions_data, session_timeout="1800 seconds"
        )
        session_count = session_metrics_df.count()

        assert session_count > 0, f"Should create sessions from {original_count} interactions"

        # DAU should have users from original data
        dau_df = calculate_dau(sample_interactions_data)
        dau_users = dau_df.agg(F.sum("daily_active_users")).collect()[0][0]

        # Total unique users across all days should match or be close to original unique users
        original_users = sample_interactions_data.select("user_id").distinct().count()
        assert dau_users >= original_users, (
            f"DAU should account for all users: {original_users} users, DAU total: {dau_users}"
        )

    def test_pipeline_error_handling(self, spark, test_data_paths):
        """Test pipeline handles data quality issues gracefully."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.utils.data_quality import detect_nulls, detect_outliers
        from src.utils.monitoring import create_monitoring_context

        # Create data with quality issues
        schema = StructType(
            [
                StructField("interaction_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("action_type", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("duration_ms", IntegerType(), True),
            ]
        )

        bad_data = [
            ("int_1", "user_1", "open", datetime(2024, 1, 1), 100),  # Good
            ("int_2", None, "edit", datetime(2024, 1, 2), 200),  # Null user
            ("int_3", "user_3", "save", datetime(2024, 1, 3), -50),  # Negative duration
            ("int_4", "user_4", None, datetime(2024, 1, 4), 150),  # Null action
            ("int_5", "user_5", "close", None, 120),  # Null timestamp
        ]

        df = spark.createDataFrame(bad_data, schema)

        # Create monitoring
        monitoring = create_monitoring_context(spark.sparkContext, "error_handling")

        # Detect issues
        null_report = detect_nulls(df, ["user_id", "action_type", "timestamp"])
        assert null_report.count() > 0, "Should detect null values"

        outliers = detect_outliers(
            df.filter(F.col("duration_ms").isNotNull()),
            "duration_ms",
            method="threshold",
            threshold_min=0,
            threshold_max=10000,
        )
        assert outliers.count() > 0, "Should detect outliers"

        # Track bad records
        bad_count = null_report.count() + outliers.count()
        monitoring["skipped_records"].add(bad_count)

        # Filter to clean data
        clean_df = df.filter(
            F.col("user_id").isNotNull()
            & F.col("action_type").isNotNull()
            & F.col("timestamp").isNotNull()
            & (F.col("duration_ms") > 0)
        )

        clean_count = clean_df.count()
        monitoring["record_counter"].add(clean_count)

        # Verify monitoring tracked the issues
        assert monitoring["record_counter"].value == 1, "Should have 1 clean record"
        assert monitoring["skipped_records"].value > 0, "Should have tracked skipped records"
