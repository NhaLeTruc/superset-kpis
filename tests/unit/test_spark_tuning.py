"""
Unit tests for Spark job-specific tuning functions.

Tests partition calculation and job-specific configuration.
"""


class TestCalculateOptimalPartitions:
    """Test suite for calculate_optimal_partitions function."""

    def test_returns_positive_integer(self, spark):
        """Test that function returns a positive partition count."""
        from src.config.spark_tuning import calculate_optimal_partitions

        partitions = calculate_optimal_partitions(spark, data_size_gb=1)

        assert isinstance(partitions, int)
        assert partitions > 0

    def test_minimum_equals_cluster_cores(self, spark):
        """Test that minimum partitions equals cluster core count."""
        from src.config.spark_tuning import calculate_optimal_partitions

        # With 0 data, should still return at least total_cores
        partitions = calculate_optimal_partitions(spark, data_size_gb=0)

        # Should be at least 2 (minimum sensible core count)
        assert partitions >= 2

    def test_scales_with_data_size(self, spark):
        """Test that partitions increase with larger data size."""
        from src.config.spark_tuning import calculate_optimal_partitions

        small_partitions = calculate_optimal_partitions(spark, data_size_gb=1)
        large_partitions = calculate_optimal_partitions(spark, data_size_gb=100)

        assert large_partitions >= small_partitions

    def test_respects_partition_size_parameter(self, spark):
        """Test that smaller partition size yields more partitions."""
        from src.config.spark_tuning import calculate_optimal_partitions

        # 10GB with 128MB partitions vs 256MB partitions
        more_partitions = calculate_optimal_partitions(
            spark, data_size_gb=10, partition_size_mb=128
        )
        fewer_partitions = calculate_optimal_partitions(
            spark, data_size_gb=10, partition_size_mb=256
        )

        assert more_partitions >= fewer_partitions


class TestConfigureJobSpecificSettings:
    """Test suite for configure_job_specific_settings function."""

    def test_etl_sets_shuffle_partitions(self, spark):
        """Test that ETL job type sets shuffle partitions."""
        from src.config.spark_tuning import configure_job_specific_settings

        configure_job_specific_settings(spark, "etl", data_size_gb=1)

        partitions = spark.conf.get("spark.sql.shuffle.partitions")
        assert partitions is not None
        assert int(partitions) > 0

    def test_analytics_sets_higher_broadcast_threshold(self, spark):
        """Test that analytics job type sets 200MB broadcast threshold."""
        from src.config.spark_tuning import configure_job_specific_settings

        configure_job_specific_settings(spark, "analytics", data_size_gb=1)

        threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        assert threshold == "200MB"

    def test_ml_sets_storage_fraction(self, spark):
        """Test that ML job type sets higher storage fraction."""
        from src.config.spark_tuning import configure_job_specific_settings

        configure_job_specific_settings(spark, "ml", data_size_gb=1)

        storage_fraction = spark.conf.get("spark.memory.storageFraction")
        assert storage_fraction == "0.5"

    def test_unknown_job_type_does_not_raise(self, spark):
        """Test that unknown job type doesn't raise an error."""
        from src.config.spark_tuning import configure_job_specific_settings

        # Should not raise
        configure_job_specific_settings(spark, "unknown_type", data_size_gb=1)
