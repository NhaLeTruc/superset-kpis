"""
Unit tests for percentile and correlation analysis functions.

Tests calculate_percentiles and calculate_device_correlation functions.
"""

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class TestCalculatePercentiles:
    """Test suite for calculate_percentiles function."""

    def test_basic_percentile_calculation(self, spark):
        """Test basic percentile calculation with simple data."""
        from src.transforms.performance.percentiles import calculate_percentiles

        # Create test data with known distribution
        data = [("group_a", i * 100) for i in range(1, 101)]
        df = spark.createDataFrame(data, ["group", "value"])

        result = calculate_percentiles(
            df,
            value_column="value",
            group_by_columns=["group"],
            percentiles=[0.50, 0.95, 0.99],
        )

        # Verify output columns
        assert "count" in result.columns
        assert "avg_value" in result.columns
        assert "stddev_value" in result.columns
        assert "p50" in result.columns
        assert "p95" in result.columns
        assert "p99" in result.columns

        row = result.collect()[0]
        assert row["count"] == 100

    def test_multiple_groups(self, spark):
        """Test percentile calculation across multiple groups."""
        from src.transforms.performance.percentiles import calculate_percentiles

        data = [
            ("group_a", 100),
            ("group_a", 200),
            ("group_b", 500),
            ("group_b", 600),
        ]
        df = spark.createDataFrame(data, ["group", "value"])

        result = calculate_percentiles(df, value_column="value", group_by_columns=["group"])

        # Should have 2 rows (one per group)
        assert result.count() == 2

        # Verify averages
        group_a = result.filter("group = 'group_a'").collect()[0]
        group_b = result.filter("group = 'group_b'").collect()[0]

        assert group_a["avg_value"] == 150.0
        assert group_b["avg_value"] == 550.0

    def test_multiple_group_by_columns(self, spark):
        """Test grouping by multiple columns."""
        from src.transforms.performance.percentiles import calculate_percentiles

        data = [
            ("a", "x", 100),
            ("a", "x", 200),
            ("a", "y", 300),
            ("b", "x", 400),
        ]
        df = spark.createDataFrame(data, ["dim1", "dim2", "value"])

        result = calculate_percentiles(df, value_column="value", group_by_columns=["dim1", "dim2"])

        # Should have 3 groups: (a,x), (a,y), (b,x)
        assert result.count() == 3

    def test_custom_percentiles(self, spark):
        """Test custom percentile values."""
        from src.transforms.performance.percentiles import calculate_percentiles

        data = [("g", i) for i in range(1, 101)]
        df = spark.createDataFrame(data, ["group", "value"])

        result = calculate_percentiles(
            df,
            value_column="value",
            group_by_columns=["group"],
            percentiles=[0.10, 0.25, 0.75, 0.90],
        )

        # Verify custom percentile columns exist
        assert "p10" in result.columns
        assert "p25" in result.columns
        assert "p75" in result.columns
        assert "p90" in result.columns

    def test_raises_error_for_missing_value_column(self, spark):
        """Test that ValueError is raised for non-existent value column."""
        from src.transforms.performance.percentiles import calculate_percentiles

        data = [("a", 100)]
        df = spark.createDataFrame(data, ["group", "value"])

        with pytest.raises(ValueError, match="not found"):
            calculate_percentiles(df, value_column="nonexistent", group_by_columns=["group"])

    def test_raises_error_for_missing_group_column(self, spark):
        """Test that ValueError is raised for non-existent group column."""
        from src.transforms.performance.percentiles import calculate_percentiles

        data = [("a", 100)]
        df = spark.createDataFrame(data, ["group", "value"])

        with pytest.raises(ValueError, match="not found"):
            calculate_percentiles(df, value_column="value", group_by_columns=["nonexistent"])

    def test_raises_error_for_invalid_percentile(self, spark):
        """Test that ValueError is raised for percentiles outside [0, 1]."""
        from src.transforms.performance.percentiles import calculate_percentiles

        data = [("a", 100)]
        df = spark.createDataFrame(data, ["group", "value"])

        with pytest.raises(ValueError, match="must be between 0 and 1"):
            calculate_percentiles(
                df, value_column="value", group_by_columns=["group"], percentiles=[1.5]
            )


class TestCalculateDeviceCorrelation:
    """Test suite for calculate_device_correlation function."""

    def test_basic_device_correlation(self, spark):
        """Test basic device correlation calculation."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        # Create interactions with device_type column
        data = [
            ("u001", "iPad", 1000),
            ("u001", "iPad", 1200),
            ("u002", "iPhone", 2000),
            ("u002", "iPhone", 2500),
            ("u003", "Android", 1500),
        ]
        interactions_df = spark.createDataFrame(data, ["user_id", "device_type", "duration_ms"])

        result = calculate_device_correlation(interactions_df)

        # Verify output columns
        assert "device_type" in result.columns
        assert "avg_duration_ms" in result.columns
        assert "p95_duration_ms" in result.columns
        assert "total_interactions" in result.columns
        assert "unique_users" in result.columns
        assert "interactions_per_user" in result.columns
        assert "f_statistic" in result.columns
        assert "eta_squared" in result.columns

    def test_with_metadata_join(self, spark):
        """Test device correlation when joining with metadata."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        # Interactions without device_type
        interactions_data = [
            ("u001", 1000),
            ("u001", 1200),
            ("u002", 2000),
        ]
        interactions_df = spark.createDataFrame(interactions_data, ["user_id", "duration_ms"])

        # Metadata with device_type
        metadata_data = [
            ("u001", "iPad"),
            ("u002", "iPhone"),
        ]
        metadata_df = spark.createDataFrame(metadata_data, ["user_id", "device_type"])

        result = calculate_device_correlation(interactions_df, metadata_df)

        # Should have 2 device types
        assert result.count() == 2

    def test_sorted_by_avg_duration_desc(self, spark):
        """Test that results are sorted by avg_duration_ms descending."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        data = [
            ("u001", "slow_device", 5000),
            ("u002", "fast_device", 100),
            ("u003", "medium_device", 2000),
        ]
        interactions_df = spark.createDataFrame(data, ["user_id", "device_type", "duration_ms"])

        result = calculate_device_correlation(interactions_df)

        rows = result.collect()
        # First row should be the slowest device
        assert rows[0]["device_type"] == "slow_device"
        # Last row should be the fastest device
        assert rows[-1]["device_type"] == "fast_device"

    def test_raises_error_when_device_type_missing(self, spark):
        """Test ValueError when device_type not in interactions and no metadata."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        # Interactions without device_type
        data = [("u001", 1000)]
        interactions_df = spark.createDataFrame(data, ["user_id", "duration_ms"])

        with pytest.raises(ValueError, match="device_type"):
            calculate_device_correlation(interactions_df, metadata_df=None)

    def test_interactions_per_user_calculation(self, spark):
        """Test that interactions_per_user is calculated correctly."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        # 4 interactions from 2 users = 2.0 interactions per user
        data = [
            ("u001", "iPad", 1000),
            ("u001", "iPad", 1100),
            ("u002", "iPad", 1200),
            ("u002", "iPad", 1300),
        ]
        interactions_df = spark.createDataFrame(data, ["user_id", "device_type", "duration_ms"])

        result = calculate_device_correlation(interactions_df)

        row = result.collect()[0]
        assert row["total_interactions"] == 4
        assert row["unique_users"] == 2
        assert row["interactions_per_user"] == 2.0

    def test_eta_squared_range(self, spark):
        """Test that eta_squared is in valid range [0, 1]."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        # Create data with different device performances
        data = [
            ("u001", "fast", 100),
            ("u002", "fast", 150),
            ("u003", "slow", 1000),
            ("u004", "slow", 1100),
        ]
        interactions_df = spark.createDataFrame(data, ["user_id", "device_type", "duration_ms"])

        result = calculate_device_correlation(interactions_df)

        row = result.collect()[0]
        eta_sq = row["eta_squared"]

        # eta_squared should be between 0 and 1
        assert eta_sq is not None
        assert 0.0 <= eta_sq <= 1.0

    def test_empty_dataframe_handling(self, spark):
        """Test that empty DataFrame is handled gracefully."""
        from src.transforms.performance.percentiles import calculate_device_correlation

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("duration_ms", IntegerType(), True),
            ]
        )
        empty_df = spark.createDataFrame([], schema)

        result = calculate_device_correlation(empty_df)

        # Should return empty DataFrame with correct schema
        assert result.count() == 0
        assert "device_type" in result.columns
