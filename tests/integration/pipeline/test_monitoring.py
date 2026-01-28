"""
Integration tests for pipeline monitoring and performance.

Tests performance benchmarks, idempotency, and comprehensive monitoring.
"""

import time

from pyspark.sql import functions as F


class TestPipelineMonitoring:
    """Pipeline monitoring and performance tests."""

    def test_pipeline_performance_benchmark(
        self, spark, sample_interactions_data, sample_metadata_data
    ):
        """Benchmark complete pipeline performance."""
        from src.transforms.engagement import calculate_dau, calculate_mau
        from src.transforms.join import identify_hot_keys, optimized_join
        from src.transforms.performance import calculate_percentiles
        from src.transforms.session import calculate_session_metrics

        start_time = time.time()

        # Run all major transformations
        hot_keys = identify_hot_keys(sample_interactions_data, "user_id")
        enriched = optimized_join(
            sample_interactions_data, sample_metadata_data, "user_id", "inner", hot_keys
        )
        enriched.count()  # Force execution

        dau = calculate_dau(sample_interactions_data)
        dau.count()

        mau = calculate_mau(sample_interactions_data)
        mau.count()

        percentiles = calculate_percentiles(
            enriched, "duration_ms", ["app_version"], [0.5, 0.95, 0.99]
        )
        percentiles.count()

        session_metrics = calculate_session_metrics(sample_interactions_data)
        session_metrics.count()

        elapsed_time = time.time() - start_time

        # Complete pipeline should finish in under 60 seconds for 1000 records
        assert elapsed_time < 60, (
            f"Pipeline took {elapsed_time:.2f}s, expected < 60s for 1000 records"
        )

        print(f"\n✅ Pipeline benchmark: {elapsed_time:.2f}s for 1000 records")

    def test_pipeline_idempotency(
        self, spark, test_data_paths, sample_interactions_data, sample_metadata_data
    ):
        """Test pipeline produces consistent results when run multiple times."""
        from src.transforms.engagement import calculate_dau
        from src.transforms.join import identify_hot_keys, optimized_join

        # Run pipeline twice
        results_1 = {}
        results_2 = {}

        for _i, results in enumerate([results_1, results_2]):
            # Data processing
            hot_keys = identify_hot_keys(sample_interactions_data, "user_id")
            enriched = optimized_join(
                sample_interactions_data, sample_metadata_data, "user_id", "inner", hot_keys
            )
            results["enriched_count"] = enriched.count()

            # Engagement metrics
            dau = calculate_dau(sample_interactions_data)
            results["dau_count"] = dau.count()
            results["total_dau"] = dau.agg(F.sum("daily_active_users")).collect()[0][0]

        # Results should be identical
        assert results_1["enriched_count"] == results_2["enriched_count"], (
            "Enriched counts should match"
        )
        assert results_1["dau_count"] == results_2["dau_count"], "DAU counts should match"
        assert results_1["total_dau"] == results_2["total_dau"], "Total DAU should match"

    def test_pipeline_with_monitoring_comprehensive(
        self, spark, sample_interactions_data, sample_metadata_data
    ):
        """Test complete pipeline with comprehensive monitoring."""
        from src.transforms.engagement import calculate_dau, calculate_mau
        from src.transforms.join import identify_hot_keys, optimized_join
        from src.transforms.performance import calculate_percentiles
        from src.transforms.session import calculate_session_metrics
        from src.utils.monitoring import create_monitoring_context, format_monitoring_summary

        # Create monitoring
        monitoring = create_monitoring_context(spark.sparkContext, "comprehensive_test")

        # Track initial record count
        initial_count = sample_interactions_data.count()
        monitoring["record_counter"].add(initial_count)

        # Run pipeline with monitoring
        hot_keys = identify_hot_keys(sample_interactions_data, "user_id")
        enriched = optimized_join(
            sample_interactions_data, sample_metadata_data, "user_id", "inner", hot_keys
        )

        dau = calculate_dau(sample_interactions_data)
        mau = calculate_mau(sample_interactions_data)

        percentiles = calculate_percentiles(
            enriched, "duration_ms", ["app_version"], [0.5, 0.95, 0.99]
        )

        session_metrics = calculate_session_metrics(sample_interactions_data)

        # Generate monitoring summary
        summary = format_monitoring_summary(monitoring, "Comprehensive Pipeline Test")

        # Verify summary contains expected information
        assert "Comprehensive Pipeline Test" in summary
        assert str(initial_count) in summary.replace(",", "")

        # Verify all transformations completed
        assert enriched.count() > 0
        assert dau.count() > 0
        assert mau.count() > 0
        assert percentiles.count() > 0
        assert session_metrics.count() > 0

        print("\n" + summary)
