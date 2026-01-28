"""
Unit tests for join optimization functions.

Tests hot key detection and salting techniques for mitigating data skew
in distributed joins.
"""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class TestIdentifyHotKeys:
    """Test suite for identify_hot_keys function."""

    def test_identifies_skewed_keys(self, spark):
        """Test that highly frequent keys are identified as hot."""
        from src.transforms.join.optimization import identify_hot_keys

        # Create skewed data: key "hot" appears 100 times, others appear once
        hot_data = [("hot", i) for i in range(100)]
        normal_data = [(f"normal_{i}", i) for i in range(10)]
        data = hot_data + normal_data

        df = spark.createDataFrame(data, ["key", "value"])

        # Identify hot keys (top 1%)
        hot_keys = identify_hot_keys(df, "key", threshold_percentile=0.90)

        # "hot" key should be identified
        hot_key_values = [r["key"] for r in hot_keys.collect()]
        assert "hot" in hot_key_values

    def test_returns_count_column(self, spark):
        """Test that result includes count column."""
        from src.transforms.join.optimization import identify_hot_keys

        data = [("a", 1)] * 50 + [("b", 2)] * 5
        df = spark.createDataFrame(data, ["key", "value"])

        hot_keys = identify_hot_keys(df, "key", threshold_percentile=0.50)

        # Verify count column exists
        assert "count" in hot_keys.columns

        # "a" should have count of 50
        result = hot_keys.filter("key = 'a'").collect()
        if result:
            assert result[0]["count"] == 50

    def test_raises_error_for_missing_column(self, spark):
        """Test that ValueError is raised for non-existent column."""
        from src.transforms.join.optimization import identify_hot_keys

        data = [("a", 1), ("b", 2)]
        df = spark.createDataFrame(data, ["key", "value"])

        with pytest.raises(ValueError, match="not found"):
            identify_hot_keys(df, "nonexistent_column")

    def test_uniform_distribution_returns_empty(self, spark):
        """Test that uniform distribution returns no hot keys."""
        from src.transforms.join.optimization import identify_hot_keys

        # All keys appear exactly once
        data = [(f"key_{i}", i) for i in range(100)]
        df = spark.createDataFrame(data, ["key", "value"])

        hot_keys = identify_hot_keys(df, "key", threshold_percentile=0.99)

        # No keys should be identified as hot
        assert hot_keys.count() == 0

    def test_custom_threshold(self, spark):
        """Test custom threshold percentile."""
        from src.transforms.join.optimization import identify_hot_keys

        # Create data with varying frequencies
        data = (
            [("frequent", i) for i in range(50)]
            + [("medium", i) for i in range(20)]
            + [("rare", i) for i in range(5)]
        )
        df = spark.createDataFrame(data, ["key", "value"])

        # With low threshold, more keys should be identified
        hot_keys_low = identify_hot_keys(df, "key", threshold_percentile=0.30)
        hot_keys_high = identify_hot_keys(df, "key", threshold_percentile=0.90)

        # Lower threshold should identify more or equal keys
        assert hot_keys_low.count() >= hot_keys_high.count()


class TestApplySalting:
    """Test suite for apply_salting function."""

    def test_adds_salt_column(self, spark):
        """Test that salt column is added to DataFrame."""
        from src.transforms.join.optimization import apply_salting

        # Create data
        data = [("hot", 1), ("hot", 2), ("normal", 3)]
        df = spark.createDataFrame(data, ["key", "value"])

        # Create hot keys DataFrame
        hot_keys_df = spark.createDataFrame([("hot", 2)], ["key", "count"])

        result = apply_salting(df, hot_keys_df, "key", salt_factor=5)

        # Verify salt column exists
        assert "salt" in result.columns
        assert "key_salted" in result.columns

    def test_hot_keys_get_random_salt(self, spark):
        """Test that hot keys receive random salt values."""
        from src.transforms.join.optimization import apply_salting

        # Create data with hot key
        data = [("hot", i) for i in range(100)]
        df = spark.createDataFrame(data, ["key", "value"])

        hot_keys_df = spark.createDataFrame([("hot", 100)], ["key", "count"])

        result = apply_salting(df, hot_keys_df, "key", salt_factor=10)

        # Salt values should be in range [0, salt_factor-1]
        salt_values = [r["salt"] for r in result.select("salt").distinct().collect()]
        for salt in salt_values:
            assert 0 <= salt < 10

    def test_normal_keys_get_zero_salt(self, spark):
        """Test that non-hot keys get salt value of 0."""
        from src.transforms.join.optimization import apply_salting

        data = [("normal", 1), ("normal", 2)]
        df = spark.createDataFrame(data, ["key", "value"])

        hot_keys_df = spark.createDataFrame([("hot", 100)], ["key", "count"])

        result = apply_salting(df, hot_keys_df, "key", salt_factor=10)

        # Normal keys should have salt = 0
        normal_rows = result.collect()
        for row in normal_rows:
            assert row["salt"] == 0

    def test_salted_column_format(self, spark):
        """Test that salted column has correct format key_salt."""
        from src.transforms.join.optimization import apply_salting

        data = [("mykey", 1)]
        df = spark.createDataFrame(data, ["key", "value"])

        hot_keys_df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("key", StringType(), True),
                    StructField("count", IntegerType(), True),
                ]
            ),
        )

        result = apply_salting(df, hot_keys_df, "key", salt_factor=5)

        # Should have format "mykey_0"
        salted_value = result.collect()[0]["key_salted"]
        assert salted_value == "mykey_0"

    def test_raises_error_for_invalid_salt_factor(self, spark):
        """Test that ValueError is raised for salt_factor < 2."""
        from src.transforms.join.optimization import apply_salting

        data = [("a", 1)]
        df = spark.createDataFrame(data, ["key", "value"])
        hot_keys_df = spark.createDataFrame([("a", 1)], ["key", "count"])

        with pytest.raises(ValueError, match="salt_factor must be >= 2"):
            apply_salting(df, hot_keys_df, "key", salt_factor=1)

    def test_raises_error_for_missing_column(self, spark):
        """Test that ValueError is raised for missing key column."""
        from src.transforms.join.optimization import apply_salting

        data = [("a", 1)]
        df = spark.createDataFrame(data, ["key", "value"])
        hot_keys_df = spark.createDataFrame([("a", 1)], ["key", "count"])

        with pytest.raises(ValueError, match="not found"):
            apply_salting(df, hot_keys_df, "nonexistent", salt_factor=5)


class TestExplodeForSalting:
    """Test suite for explode_for_salting function."""

    def test_explodes_hot_keys(self, spark):
        """Test that hot keys are exploded to match salt factor."""
        from src.transforms.join.optimization import explode_for_salting

        # Single hot key row
        data = [("hot", "metadata1")]
        df = spark.createDataFrame(data, ["key", "data"])

        hot_keys_df = spark.createDataFrame([("hot", 100)], ["key", "count"])

        result = explode_for_salting(df, hot_keys_df, "key", salt_factor=10)

        # Should have 10 rows (one for each salt value)
        assert result.count() == 10

        # Salt values should cover 0-9
        salt_values = sorted([r["salt"] for r in result.select("salt").collect()])
        assert salt_values == list(range(10))

    def test_non_hot_keys_not_exploded(self, spark):
        """Test that non-hot keys appear only once with salt=0."""
        from src.transforms.join.optimization import explode_for_salting

        data = [("normal", "data1")]
        df = spark.createDataFrame(data, ["key", "data"])

        hot_keys_df = spark.createDataFrame([("hot", 100)], ["key", "count"])

        result = explode_for_salting(df, hot_keys_df, "key", salt_factor=10)

        # Should have 1 row
        assert result.count() == 1

        # Salt should be 0
        row = result.collect()[0]
        assert row["salt"] == 0
        assert row["key_salted"] == "normal_0"

    def test_mixed_hot_and_normal_keys(self, spark):
        """Test explosion with both hot and normal keys."""
        from src.transforms.join.optimization import explode_for_salting

        data = [
            ("hot", "hot_data"),
            ("normal", "normal_data"),
        ]
        df = spark.createDataFrame(data, ["key", "data"])

        hot_keys_df = spark.createDataFrame([("hot", 100)], ["key", "count"])

        result = explode_for_salting(df, hot_keys_df, "key", salt_factor=5)

        # Should have 5 (hot) + 1 (normal) = 6 rows
        assert result.count() == 6

        # Verify hot key exploded
        hot_count = result.filter("key = 'hot'").count()
        assert hot_count == 5

        # Verify normal key not exploded
        normal_count = result.filter("key = 'normal'").count()
        assert normal_count == 1

    def test_preserves_all_columns(self, spark):
        """Test that all original columns are preserved after explosion."""
        from src.transforms.join.optimization import explode_for_salting

        data = [("hot", "value1", 100, 1.5)]
        df = spark.createDataFrame(data, ["key", "str_col", "int_col", "float_col"])

        hot_keys_df = spark.createDataFrame([("hot", 50)], ["key", "count"])

        result = explode_for_salting(df, hot_keys_df, "key", salt_factor=3)

        # All original columns should be present
        assert "key" in result.columns
        assert "str_col" in result.columns
        assert "int_col" in result.columns
        assert "float_col" in result.columns

        # Values should be preserved in all exploded rows
        for row in result.collect():
            assert row["str_col"] == "value1"
            assert row["int_col"] == 100
            assert row["float_col"] == 1.5

    def test_raises_error_for_missing_column(self, spark):
        """Test that ValueError is raised for missing key column."""
        from src.transforms.join.optimization import explode_for_salting

        data = [("a", 1)]
        df = spark.createDataFrame(data, ["key", "value"])
        hot_keys_df = spark.createDataFrame([("a", 1)], ["key", "count"])

        with pytest.raises(ValueError, match="not found"):
            explode_for_salting(df, hot_keys_df, "nonexistent", salt_factor=5)
