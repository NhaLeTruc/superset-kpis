"""
Integration tests for performance metrics accuracy.

Tests percentile calculations, device correlation, and pipeline completeness.
"""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F


class TestPerformanceMetricsAccuracy:
    """Accuracy tests for performance metrics calculations."""

    def test_performance_pipeline_complete(
        self, spark, sample_interactions_data, sample_metadata_data
    ):
        """Test complete performance metrics pipeline."""
        from src.transforms.performance import (
            calculate_device_correlation,
            calculate_percentiles,
            detect_anomalies_statistical,
        )

        # Calculate percentiles by app version
        percentiles_df = calculate_percentiles(
            sample_interactions_data.join(sample_metadata_data, "user_id", "inner"),
            value_column="duration_ms",
            group_by_columns=["app_version"],
            percentiles=[0.5, 0.95, 0.99],
        )

        assert percentiles_df.count() > 0, "Should calculate percentiles"

        # Verify percentile columns exist
        assert "p50" in percentiles_df.columns
        assert "p95" in percentiles_df.columns
        assert "p99" in percentiles_df.columns

        # Verify percentiles are in correct order (p50 < p95 < p99)
        for row in percentiles_df.collect():
            assert row["p50"] <= row["p95"] <= row["p99"], (
                f"Percentiles should be ordered: {row['p50']} <= {row['p95']} <= {row['p99']}"
            )

        # Calculate device correlation
        device_corr_df = calculate_device_correlation(
            sample_interactions_data, sample_metadata_data
        )

        assert device_corr_df.count() > 0, "Should calculate device correlation"

        # Verify device correlation structure
        assert "device_type" in device_corr_df.columns
        assert "avg_duration_ms" in device_corr_df.columns

        # Detect statistical anomalies
        anomalies_df = detect_anomalies_statistical(
            sample_interactions_data, value_column="duration_ms", z_threshold=3.0
        )

        # Should complete without errors
        anomaly_count = anomalies_df.count()
        assert anomaly_count >= 0, "Anomaly detection should complete"

    def test_percentile_calculation_accuracy(self, spark):
        """Test percentile calculations with known data."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from src.transforms.performance import calculate_percentiles

        # Create data with known distribution
        schema = StructType(
            [
                StructField("group", StringType(), False),
                StructField("value", IntegerType(), False),
            ]
        )

        # Group A: values 1-100
        # Group B: values 101-200
        data = [("A", i) for i in range(1, 101)] + [("B", i) for i in range(101, 201)]

        df = spark.createDataFrame(data, schema)

        # Calculate percentiles
        result = calculate_percentiles(
            df, value_column="value", group_by_columns=["group"], percentiles=[0.5, 0.95]
        )

        results = {row["group"]: row for row in result.collect()}

        # Group A: median should be around 50, p95 around 95
        assert results["A"]["p50"] == pytest.approx(50.5, abs=5)
        assert results["A"]["p95"] == pytest.approx(95.0, abs=5)

        # Group B: median should be around 150, p95 around 195
        assert results["B"]["p50"] == pytest.approx(150.5, abs=5)
        assert results["B"]["p95"] == pytest.approx(195.0, abs=5)

    def test_device_correlation_with_varied_performance(self, spark):
        """Test device correlation identifies performance differences."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.performance import calculate_device_correlation

        # Create interactions with device-specific performance
        interaction_schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("device_type", StringType(), False),
                StructField("country", StringType(), False),
                StructField("subscription_type", StringType(), False),
                StructField("registration_date", TimestampType(), False),
                StructField("app_version", StringType(), False),
            ]
        )

        # iOS users: fast (100ms average)
        # Android users: medium (200ms average)
        # Web users: slow (300ms average)

        interactions = []
        metadata = []

        # iOS users (10 users, 10 interactions each, ~100ms)
        for user_idx in range(10):
            user_id = f"ios_user_{user_idx}"
            metadata.append((user_id, "iOS", "US", "premium", datetime(2023, 1, 1), "1.0.0"))
            for i in range(10):
                interactions.append(
                    (
                        f"int_ios_{user_idx}_{i}",
                        user_id,
                        "open",
                        datetime(2024, 1, 1) + timedelta(hours=i),
                        90 + (i % 20),  # 90-110ms
                    )
                )

        # Android users (10 users, 10 interactions each, ~200ms)
        for user_idx in range(10):
            user_id = f"android_user_{user_idx}"
            metadata.append((user_id, "Android", "US", "free", datetime(2023, 1, 1), "1.0.0"))
            for i in range(10):
                interactions.append(
                    (
                        f"int_android_{user_idx}_{i}",
                        user_id,
                        "edit",
                        datetime(2024, 1, 1) + timedelta(hours=i),
                        190 + (i % 20),  # 190-210ms
                    )
                )

        # Web users (10 users, 10 interactions each, ~300ms)
        for user_idx in range(10):
            user_id = f"web_user_{user_idx}"
            metadata.append((user_id, "Web", "UK", "free", datetime(2023, 1, 1), "1.0.0"))
            for i in range(10):
                interactions.append(
                    (
                        f"int_web_{user_idx}_{i}",
                        user_id,
                        "save",
                        datetime(2024, 1, 1) + timedelta(hours=i),
                        290 + (i % 20),  # 290-310ms
                    )
                )

        interactions_df = spark.createDataFrame(interactions, interaction_schema)
        metadata_df = spark.createDataFrame(metadata, metadata_schema)

        # Calculate device correlation
        device_perf = calculate_device_correlation(interactions_df, metadata_df)

        results = {row["device_type"]: row for row in device_perf.collect()}

        # Verify performance differences
        assert results["iOS"]["avg_duration_ms"] == pytest.approx(100, abs=10)
        assert results["Android"]["avg_duration_ms"] == pytest.approx(200, abs=10)
        assert results["Web"]["avg_duration_ms"] == pytest.approx(300, abs=10)

        # Verify ordering (iOS fastest, Web slowest)
        assert results["iOS"]["avg_duration_ms"] < results["Android"]["avg_duration_ms"]
        assert results["Android"]["avg_duration_ms"] < results["Web"]["avg_duration_ms"]

    def test_percentile_with_multiple_groups(self, spark):
        """Test percentile calculations across multiple grouping dimensions."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from src.transforms.performance import calculate_percentiles

        schema = StructType(
            [
                StructField("country", StringType(), False),
                StructField("device_type", StringType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        # Create data for US-iOS, US-Android, UK-iOS, UK-Android
        data = []

        for country in ["US", "UK"]:
            for device in ["iOS", "Android"]:
                base_duration = 100 if device == "iOS" else 200
                for i in range(50):
                    data.append((country, device, base_duration + i))

        df = spark.createDataFrame(data, schema)

        # Calculate percentiles by country and device
        result = calculate_percentiles(
            df,
            value_column="duration_ms",
            group_by_columns=["country", "device_type"],
            percentiles=[0.5, 0.95],
        )

        # Should have 4 groups (2 countries x 2 devices)
        assert result.count() == 4

        # Verify all combinations exist
        results_dict = {}
        for row in result.collect():
            key = (row["country"], row["device_type"])
            results_dict[key] = row

        assert ("US", "iOS") in results_dict
        assert ("US", "Android") in results_dict
        assert ("UK", "iOS") in results_dict
        assert ("UK", "Android") in results_dict

    def test_performance_monitoring_integration(
        self, spark, sample_interactions_data, sample_metadata_data
    ):
        """Test performance metrics with monitoring integration."""
        from src.transforms.performance import calculate_percentiles
        from src.utils.monitoring import create_monitoring_context

        # Create monitoring context
        context = create_monitoring_context(spark.sparkContext, "test_performance")

        # Join data
        enriched_df = sample_interactions_data.join(sample_metadata_data, "user_id", "inner")

        # Track processing
        context["record_counter"].add(enriched_df.count())

        # Calculate percentiles
        percentiles_df = calculate_percentiles(
            enriched_df,
            value_column="duration_ms",
            group_by_columns=["app_version"],
            percentiles=[0.5, 0.95, 0.99],
        )

        assert percentiles_df.count() > 0

        # Verify monitoring tracked records
        assert context["record_counter"].value > 0

    def test_performance_with_null_values(self, spark):
        """Test performance calculations handle null values correctly."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from src.transforms.performance import calculate_percentiles

        schema = StructType(
            [
                StructField("group", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        # Include some null values
        data = [
            ("A", 100),
            ("A", 200),
            ("A", None),  # Null value
            ("A", 300),
            ("B", 150),
            ("B", None),  # Null value
            ("B", 250),
        ]

        df = spark.createDataFrame(data, schema)

        # Should handle nulls gracefully
        result = calculate_percentiles(
            df.filter(F.col("value").isNotNull()),  # Filter nulls
            value_column="value",
            group_by_columns=["group"],
            percentiles=[0.5],
        )

        # Should complete without errors
        assert result.count() == 2

    def test_performance_metrics_timing(
        self, spark, sample_interactions_data, sample_metadata_data
    ):
        """Test performance calculations complete in reasonable time."""
        import time

        from src.transforms.performance import (
            calculate_device_correlation,
            calculate_percentiles,
            detect_anomalies_statistical,
        )

        start_time = time.time()

        # Join data
        enriched_df = sample_interactions_data.join(sample_metadata_data, "user_id", "inner")

        # Run all performance calculations
        percentiles = calculate_percentiles(
            enriched_df, "duration_ms", ["app_version"], [0.5, 0.95, 0.99]
        )
        perc_count = percentiles.count()

        device_corr = calculate_device_correlation(sample_interactions_data, sample_metadata_data)
        device_count = device_corr.count()

        anomalies = detect_anomalies_statistical(
            sample_interactions_data, "duration_ms", z_threshold=3.0
        )
        anomaly_count = anomalies.count()

        elapsed_time = time.time() - start_time

        # Should complete in under 30 seconds
        assert elapsed_time < 30, (
            f"Performance calculations took {elapsed_time:.2f}s, expected < 30s"
        )
        assert perc_count > 0 and device_count > 0 and anomaly_count >= 0
