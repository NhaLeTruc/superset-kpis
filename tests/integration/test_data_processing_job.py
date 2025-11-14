"""
Integration tests for the data processing job (Job 01).

Tests the complete data processing pipeline including:
- Data loading from raw sources
- Data quality validation
- Hot key detection and salting
- Optimized joins with skew handling
- Writing enriched data to Parquet
"""
import pytest
import os
from pyspark.sql import functions as F
from datetime import datetime


class TestDataProcessingJob:
    """Integration tests for data processing job."""

    def test_data_processing_pipeline_basic(self, spark, test_data_paths,
                                           sample_interactions_data, sample_metadata_data):
        """Test complete data processing pipeline with basic data."""
        # Setup: Write test data to input paths
        sample_interactions_data.write.mode("overwrite").parquet(
            test_data_paths["raw_interactions"]
        )
        sample_metadata_data.write.mode("overwrite").parquet(
            test_data_paths["raw_metadata"]
        )

        # Import required functions
        from src.transforms.join_transforms import identify_hot_keys, optimized_join
        from src.utils.data_quality import validate_schema, detect_nulls
        from src.schemas.interactions_schema import INTERACTIONS_SCHEMA
        from src.schemas.metadata_schema import METADATA_SCHEMA

        # Step 1: Load data
        interactions_df = spark.read.parquet(test_data_paths["raw_interactions"])
        metadata_df = spark.read.parquet(test_data_paths["raw_metadata"])

        # Step 2: Validate schemas
        validate_schema(interactions_df, INTERACTIONS_SCHEMA, strict=False)
        validate_schema(metadata_df, METADATA_SCHEMA, strict=False)

        # Step 3: Detect nulls
        null_report_interactions = detect_nulls(
            interactions_df,
            ["user_id", "interaction_id", "timestamp"]
        )
        null_report_metadata = detect_nulls(metadata_df, ["user_id"])

        # Should have no nulls in test data
        assert null_report_interactions.count() == 0, "Should have no null interactions"
        assert null_report_metadata.count() == 0, "Should have no null metadata"

        # Step 4: Identify hot keys (though our test data is uniform)
        hot_keys = identify_hot_keys(interactions_df, "user_id", threshold_percentile=0.95)

        # Step 5: Perform optimized join
        enriched_df = optimized_join(
            large_df=interactions_df,
            small_df=metadata_df,
            join_key="user_id",
            hot_keys_df=hot_keys,
            broadcast_threshold_mb=10,
            salt_factor=5
        )

        # Verify results
        assert enriched_df.count() == 1000, "Should have all 1000 interactions enriched"

        # Verify join added metadata columns
        expected_columns = ["interaction_id", "user_id", "action_type", "timestamp",
                          "duration_ms", "country", "device_type", "subscription_type"]
        for col in expected_columns:
            assert col in enriched_df.columns, f"Missing column: {col}"

        # Verify no data loss
        original_count = interactions_df.count()
        enriched_count = enriched_df.count()
        assert enriched_count == original_count, f"Data loss: {original_count} -> {enriched_count}"

        # Step 6: Write to output
        output_path = os.path.join(test_data_paths["processed"], "enriched_interactions")
        enriched_df.write.mode("overwrite").partitionBy("country").parquet(output_path)

        # Verify output exists
        assert os.path.exists(output_path), "Output path should exist"

        # Verify can read back
        result_df = spark.read.parquet(output_path)
        assert result_df.count() == enriched_count, "Should be able to read back same count"

    def test_data_processing_with_skewed_data(self, spark, test_data_paths,
                                             sample_skewed_interactions, sample_metadata_data):
        """Test data processing with highly skewed data (power users)."""
        from src.transforms.join_transforms import identify_hot_keys, optimized_join

        # Create metadata for all users (power + normal)
        power_user_metadata = []
        for i in range(10):
            power_user_metadata.append((f"power_user_{i}", "US", "iOS", "premium",
                                       datetime(2023, 1, 1), "1.0.0"))

        normal_user_metadata = []
        for i in range(90):
            normal_user_metadata.append((f"normal_user_{i}", "UK", "Android", "free",
                                        datetime(2023, 6, 1), "1.0.0"))

        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        metadata_schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("country", StringType(), False),
            StructField("device_type", StringType(), False),
            StructField("subscription_type", StringType(), False),
            StructField("registration_date", TimestampType(), False),
            StructField("app_version", StringType(), False),
        ])

        metadata_df = spark.createDataFrame(
            power_user_metadata + normal_user_metadata,
            metadata_schema
        )

        # Identify hot keys (should find power users)
        hot_keys = identify_hot_keys(
            sample_skewed_interactions,
            "user_id",
            threshold_percentile=0.90  # Top 10% should be power users
        )

        hot_key_count = hot_keys.count()
        assert hot_key_count > 0, "Should identify hot keys in skewed data"
        assert hot_key_count <= 10, f"Should identify ~10 power users, got {hot_key_count}"

        # Perform optimized join with salting
        enriched_df = optimized_join(
            large_df=sample_skewed_interactions,
            small_df=metadata_df,
            join_key="user_id",
            hot_keys_df=hot_keys,
            broadcast_threshold_mb=1,  # Force salting by lowering threshold
            salt_factor=10
        )

        # Verify all records joined successfully
        expected_count = sample_skewed_interactions.count()
        actual_count = enriched_df.count()
        assert actual_count == expected_count, \
            f"Expected {expected_count} records, got {actual_count}"

        # Verify power users are properly enriched
        power_user_records = enriched_df.filter(
            F.col("user_id").like("power_user_%")
        ).count()
        assert power_user_records == 9000, f"Expected 9000 power user records, got {power_user_records}"

        # Verify premium subscription for power users
        premium_count = enriched_df.filter(
            (F.col("user_id").like("power_user_%")) &
            (F.col("subscription_type") == "premium")
        ).count()
        assert premium_count == 9000, "All power users should have premium subscription"

    def test_data_quality_validation(self, spark, test_data_paths):
        """Test data quality validation catches issues."""
        from src.utils.data_quality import detect_nulls, detect_outliers
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        # Create data with quality issues
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ])

        bad_data = [
            ("user_1", "open_note", 100, datetime(2024, 1, 1)),  # good
            (None, "edit_note", 200, datetime(2024, 1, 2)),      # null user_id
            ("user_3", "save_note", -50, datetime(2024, 1, 3)),  # negative duration
            ("user_4", "close_note", 999999, datetime(2024, 1, 4)),  # outlier duration
            ("user_5", None, 150, datetime(2024, 1, 5)),         # null action_type
            ("user_6", "open_note", 120, None),                  # null timestamp
        ]

        df = spark.createDataFrame(bad_data, schema)

        # Detect nulls
        null_report = detect_nulls(df, ["user_id", "action_type", "timestamp"])
        assert null_report.count() == 3, "Should detect 3 columns with nulls"

        # Detect outliers in duration_ms
        outliers = detect_outliers(
            df.filter(F.col("duration_ms").isNotNull()),
            "duration_ms",
            method="threshold",
            min_value=0,
            max_value=10000
        )

        outlier_count = outliers.count()
        assert outlier_count == 1, f"Should detect 1 outlier, found {outlier_count}"

        # Verify the outlier is the 999999 value
        outlier_values = outliers.select("duration_ms").collect()
        assert outlier_values[0]["duration_ms"] == 999999

    def test_partitioned_output(self, spark, test_data_paths,
                               sample_interactions_data, sample_metadata_data):
        """Test that output is correctly partitioned by date."""
        from src.transforms.join_transforms import optimized_join, identify_hot_keys

        # Add date column to interactions
        interactions_with_date = sample_interactions_data.withColumn(
            "date", F.to_date(F.col("timestamp"))
        )

        # Perform join
        hot_keys = identify_hot_keys(interactions_with_date, "user_id")
        enriched_df = optimized_join(
            interactions_with_date,
            sample_metadata_data,
            "user_id",
            hot_keys
        )

        # Write with partitioning
        output_path = os.path.join(test_data_paths["output"], "partitioned_data")
        enriched_df.write.mode("overwrite").partitionBy("date").parquet(output_path)

        # Verify partitions were created
        result_df = spark.read.parquet(output_path)
        assert result_df.count() == enriched_df.count()

        # Verify partition pruning works
        specific_date = interactions_with_date.select("date").first()["date"]
        filtered_df = result_df.filter(F.col("date") == specific_date)
        assert filtered_df.count() > 0, "Should be able to filter by partition"

    def test_empty_input_handling(self, spark, test_data_paths):
        """Test pipeline handles empty input gracefully."""
        from src.utils.data_quality import validate_schema
        from src.schemas.interactions_schema import INTERACTIONS_SCHEMA

        # Create empty DataFrame with correct schema
        empty_df = spark.createDataFrame([], INTERACTIONS_SCHEMA)

        # Should validate successfully even if empty
        validate_schema(empty_df, INTERACTIONS_SCHEMA, strict=False)

        # Count should be zero
        assert empty_df.count() == 0, "Empty DataFrame should have count of 0"

    def test_monitoring_integration(self, spark, sample_interactions_data, sample_metadata_data):
        """Test monitoring accumulators track processing metrics."""
        from src.utils.monitoring import create_monitoring_context, format_monitoring_summary
        from src.transforms.join_transforms import identify_hot_keys, optimized_join

        # Create monitoring context
        context = create_monitoring_context(spark.sparkContext, "test_data_processing")

        # Track record counts
        interaction_count = sample_interactions_data.count()
        context["record_counter"].add(interaction_count)

        # Perform join
        hot_keys = identify_hot_keys(sample_interactions_data, "user_id")
        enriched_df = optimized_join(
            sample_interactions_data,
            sample_metadata_data,
            "user_id",
            hot_keys
        )

        # Verify monitoring tracked records
        assert context["record_counter"].value == interaction_count

        # Generate summary
        summary = format_monitoring_summary(context, "Data Processing Test")
        assert "Data Processing Test" in summary
        assert str(interaction_count) in summary.replace(",", "")

    def test_performance_no_timeout(self, spark, sample_interactions_data, sample_metadata_data):
        """Test that processing completes in reasonable time."""
        import time
        from src.transforms.join_transforms import identify_hot_keys, optimized_join

        start_time = time.time()

        # Perform complete pipeline
        hot_keys = identify_hot_keys(sample_interactions_data, "user_id")
        enriched_df = optimized_join(
            sample_interactions_data,
            sample_metadata_data,
            "user_id",
            hot_keys
        )

        # Force execution
        result_count = enriched_df.count()

        elapsed_time = time.time() - start_time

        # Should complete in under 30 seconds for 1000 records
        assert elapsed_time < 30, f"Processing took {elapsed_time:.2f}s, expected < 30s"
        assert result_count == 1000, "Should process all records"
