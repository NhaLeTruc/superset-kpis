"""
End-to-end integration tests for basic pipeline execution.

Tests the complete data pipeline from raw data ingestion through
all transformation jobs to final analytics outputs.
"""
import pytest
import os
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class TestBasicPipeline:
    """Basic end-to-end pipeline tests."""

    def test_complete_pipeline_execution(self, spark, test_data_paths,
                                        sample_interactions_data, sample_metadata_data):
        """Test complete pipeline from raw data to analytics outputs."""
        # Setup: Write raw data
        sample_interactions_data.write.mode("overwrite").parquet(
            test_data_paths["raw_interactions"]
        )
        sample_metadata_data.write.mode("overwrite").parquet(
            test_data_paths["raw_metadata"]
        )

        # Import all required functions
        from src.transforms.join import identify_hot_keys, optimized_join
        from src.transforms.engagement import (
            calculate_dau, calculate_mau, calculate_stickiness,
            identify_power_users, calculate_cohort_retention
        )
        from src.transforms.performance import (
            calculate_percentiles, calculate_device_correlation,
            detect_anomalies_statistical
        )
        from src.transforms.session import (
            sessionize_interactions, calculate_session_metrics,
            calculate_bounce_rate
        )
        from src.utils.monitoring import create_monitoring_context, format_monitoring_summary

        # Create monitoring context for entire pipeline
        monitoring = create_monitoring_context(spark.sparkContext, "e2e_pipeline")

        # ================================================================
        # Job 1: Data Processing
        # ================================================================
        print("\n" + "="*60)
        print("📊 JOB 1: Data Processing")
        print("="*60)

        # Load raw data
        interactions_df = spark.read.parquet(test_data_paths["raw_interactions"])
        metadata_df = spark.read.parquet(test_data_paths["raw_metadata"])

        monitoring["record_counter"].add(interactions_df.count())

        # Hot key detection
        hot_keys = identify_hot_keys(interactions_df, "user_id", threshold_percentile=0.95)

        # Optimized join
        enriched_df = optimized_join(
            interactions_df, metadata_df, "user_id", "inner", hot_keys
        )

        # Write enriched data
        enriched_path = os.path.join(test_data_paths["processed"], "enriched")
        enriched_df.write.mode("overwrite").parquet(enriched_path)

        # Verify Job 1 output
        assert os.path.exists(enriched_path), "Job 1 output should exist"
        job1_count = spark.read.parquet(enriched_path).count()
        assert job1_count == 1000, f"Job 1 should output 1000 records, got {job1_count}"

        print(f"✅ Job 1 complete: {job1_count} enriched records")

        # ================================================================
        # Job 2: User Engagement
        # ================================================================
        print("\n" + "="*60)
        print("📊 JOB 2: User Engagement Analytics")
        print("="*60)

        # Calculate DAU
        dau_df = calculate_dau(interactions_df)
        dau_count = dau_df.count()
        assert dau_count > 0, "Should calculate DAU"

        # Calculate MAU
        mau_df = calculate_mau(interactions_df)
        mau_count = mau_df.count()
        assert mau_count > 0, "Should calculate MAU"

        # Calculate stickiness
        stickiness_df = calculate_stickiness(dau_df, mau_df)
        stickiness_count = stickiness_df.count()
        assert stickiness_count > 0, "Should calculate stickiness"

        # Identify power users
        power_users_df = identify_power_users(
            interactions_df, metadata_df, percentile=0.90
        )
        power_users_count = power_users_df.count()
        assert power_users_count > 0, "Should identify power users"

        # Calculate cohort retention
        cohort_retention_df = calculate_cohort_retention(
            interactions_df, metadata_df,
            cohort_period="week",
            retention_weeks=4
        )
        cohort_count = cohort_retention_df.count()
        assert cohort_count >= 0, "Should calculate cohort retention"

        print(f"✅ Job 2 complete:")
        print(f"   - DAU: {dau_count} records")
        print(f"   - MAU: {mau_count} records")
        print(f"   - Stickiness: {stickiness_count} records")
        print(f"   - Power Users: {power_users_count} records")
        print(f"   - Cohort Retention: {cohort_count} records")

        # ================================================================
        # Job 3: Performance Metrics
        # ================================================================
        print("\n" + "="*60)
        print("📊 JOB 3: Performance Metrics")
        print("="*60)

        # Calculate percentiles by app version
        percentiles_df = calculate_percentiles(
            enriched_df,
            value_column="duration_ms",
            group_by_columns=["app_version"],
            percentiles=[0.5, 0.95, 0.99]
        )
        percentiles_count = percentiles_df.count()
        assert percentiles_count > 0, "Should calculate percentiles"

        # Calculate device correlation
        device_corr_df = calculate_device_correlation(interactions_df, metadata_df)
        device_count = device_corr_df.count()
        assert device_count > 0, "Should calculate device correlation"

        # Detect anomalies
        anomalies_df = detect_anomalies_statistical(
            interactions_df,
            value_column="duration_ms",
            threshold=3.0
        )
        anomaly_count = anomalies_df.count()

        print(f"✅ Job 3 complete:")
        print(f"   - Percentiles: {percentiles_count} records")
        print(f"   - Device Correlation: {device_count} records")
        print(f"   - Anomalies Detected: {anomaly_count} records")

        # ================================================================
        # Job 4: Session Analysis
        # ================================================================
        print("\n" + "="*60)
        print("📊 JOB 4: Session Analysis")
        print("="*60)

        # Sessionize interactions
        sessionized_df = sessionize_interactions(
            interactions_df,
            session_timeout_seconds=1800
        )
        session_interaction_count = sessionized_df.count()
        assert session_interaction_count == 1000, "All interactions should be sessionized"

        # Calculate session metrics
        session_metrics_df = calculate_session_metrics(sessionized_df)
        session_count = session_metrics_df.count()
        assert session_count > 0, "Should calculate session metrics"

        # Calculate bounce rate
        bounce_rate_df = calculate_bounce_rate(session_metrics_df)
        bounce_count = bounce_rate_df.count()
        assert bounce_count > 0, "Should calculate bounce rate"

        print(f"✅ Job 4 complete:")
        print(f"   - Sessions Created: {session_count}")
        print(f"   - Bounce Rate Records: {bounce_count}")

        # ================================================================
        # Pipeline Summary
        # ================================================================
        print("\n" + "="*60)
        print("📊 PIPELINE SUMMARY")
        print("="*60)

        summary = format_monitoring_summary(monitoring, "E2E Pipeline")
        print(summary)

        # Final verification
        assert job1_count == 1000, "Job 1: Data processing"
        assert dau_count > 0, "Job 2: DAU calculation"
        assert mau_count > 0, "Job 2: MAU calculation"
        assert stickiness_count > 0, "Job 2: Stickiness calculation"
        assert power_users_count > 0, "Job 2: Power users identification"
        assert percentiles_count > 0, "Job 3: Percentile calculation"
        assert device_count > 0, "Job 3: Device correlation"
        assert session_count > 0, "Job 4: Session metrics"
        assert bounce_count > 0, "Job 4: Bounce rate"

        print("✅ All pipeline jobs completed successfully!")

    def test_pipeline_with_large_dataset(self, spark, test_data_paths):
        """Test pipeline with larger dataset (10,000 records)."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
        from src.transforms.join import identify_hot_keys, optimized_join
        from src.transforms.engagement import calculate_dau, calculate_mau

        # Generate larger dataset
        interaction_schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        metadata_schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("country", StringType(), False),
            StructField("device_type", StringType(), False),
            StructField("subscription_type", StringType(), False),
            StructField("registration_date", TimestampType(), False),
            StructField("app_version", StringType(), False),
        ])

        # Generate 10,000 interactions from 1,000 users
        base_date = datetime(2024, 1, 1)
        interactions = []
        for i in range(10000):
            interactions.append((
                f"int_{i}",
                f"user_{i % 1000}",
                ["open_note", "edit_note", "save_note", "close_note"][i % 4],
                base_date + timedelta(hours=i % 720),
                100 + (i % 5000)
            ))

        # Generate metadata for 1,000 users
        metadata = []
        for i in range(1000):
            metadata.append((
                f"user_{i}",
                ["US", "UK", "CA", "AU", "DE"][i % 5],
                ["iOS", "Android", "Web"][i % 3],
                ["free", "premium", "enterprise"][i % 3],
                base_date - timedelta(days=i),
                "1.0.0"
            ))

        interactions_df = spark.createDataFrame(interactions, interaction_schema)
        metadata_df = spark.createDataFrame(metadata, metadata_schema)

        # Run subset of pipeline
        hot_keys = identify_hot_keys(interactions_df, "user_id")
        enriched_df = optimized_join(interactions_df, metadata_df, "user_id", "inner", hot_keys)
        assert enriched_df.count() == 10000, "Should enrich all 10,000 records"

        dau_df = calculate_dau(interactions_df)
        assert dau_df.count() > 0, "Should calculate DAU for large dataset"

        mau_df = calculate_mau(interactions_df)
        assert mau_df.count() > 0, "Should calculate MAU for large dataset"
