"""
Unit tests for statistical anomaly detection functions.

Tests the Z-score based anomaly detection with iterative baseline refinement.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType


class TestCalculateZscore:
    """Test suite for _calculate_zscore helper function."""

    def test_normal_zscore_calculation(self, spark):
        """Test Z-score calculation with normal values."""
        from src.transforms.performance.anomalies import _calculate_zscore

        data = [(100.0, 50.0, 10.0)]  # value=100, mean=50, stddev=10 -> z=5
        df = spark.createDataFrame(data, ["value", "mean", "stddev"])

        result = df.withColumn(
            "zscore",
            _calculate_zscore(F.col("value"), F.col("mean"), F.col("stddev")),
        ).collect()[0]

        assert result["zscore"] == 5.0

    def test_zero_stddev_returns_zero(self, spark):
        """Test that zero standard deviation returns Z-score of 0."""
        from src.transforms.performance.anomalies import _calculate_zscore

        data = [(100.0, 50.0, 0.0)]  # stddev=0
        df = spark.createDataFrame(data, ["value", "mean", "stddev"])

        result = df.withColumn(
            "zscore",
            _calculate_zscore(F.col("value"), F.col("mean"), F.col("stddev")),
        ).collect()[0]

        assert result["zscore"] == 0.0

    def test_null_stddev_returns_zero(self, spark):
        """Test that null standard deviation returns Z-score of 0."""
        from src.transforms.performance.anomalies import _calculate_zscore

        data = [(100.0, 50.0, None)]
        schema = StructType(
            [
                StructField("value", DoubleType(), False),
                StructField("mean", DoubleType(), False),
                StructField("stddev", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = df.withColumn(
            "zscore",
            _calculate_zscore(F.col("value"), F.col("mean"), F.col("stddev")),
        ).collect()[0]

        assert result["zscore"] == 0.0


class TestDetectAnomaliesStatistical:
    """Test suite for detect_anomalies_statistical function."""

    def test_detects_high_anomalies(self, spark):
        """Test detection of values significantly above the mean."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        # Normal values around 100, one extreme outlier at 1000
        data = [(i, 100 + (i % 10)) for i in range(100)] + [(100, 1000)]
        df = spark.createDataFrame(data, ["id", "value"])

        result = detect_anomalies_statistical(df, "value", z_threshold=3.0)

        # Should detect the 1000 value as an anomaly
        assert result.count() >= 1

        anomalies = result.collect()
        values = [r["value"] for r in anomalies]
        assert 1000 in values

        # Should be marked as "high" anomaly
        high_anomaly = result.filter(F.col("value") == 1000).collect()[0]
        assert high_anomaly["anomaly_type"] == "high"

    def test_detects_low_anomalies(self, spark):
        """Test detection of values significantly below the mean."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        # Normal values around 100, one extreme outlier at -100
        data = [(i, 100 + (i % 10)) for i in range(100)] + [(100, -100)]
        df = spark.createDataFrame(data, ["id", "value"])

        result = detect_anomalies_statistical(df, "value", z_threshold=3.0)

        # Should detect the -100 value as an anomaly
        low_anomaly = result.filter(F.col("value") == -100).collect()
        assert len(low_anomaly) == 1
        assert low_anomaly[0]["anomaly_type"] == "low"

    def test_no_anomalies_in_uniform_data(self, spark):
        """Test that uniform data produces no anomalies."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        # All values are identical (stddev = 0)
        data = [(i, 100) for i in range(50)]
        df = spark.createDataFrame(data, ["id", "value"])

        result = detect_anomalies_statistical(df, "value", z_threshold=3.0)

        assert result.count() == 0

    def test_grouped_anomaly_detection(self, spark):
        """Test anomaly detection with group_by_columns."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        # Two groups with different baselines
        data = (
            # Group A: normal values around 100
            [("A", i, 100 + (i % 5)) for i in range(50)]
            + [("A", 50, 500)]  # Anomaly for group A
            # Group B: normal values around 1000
            + [("B", i, 1000 + (i % 5)) for i in range(50)]
            + [("B", 50, 50)]  # Anomaly for group B (low)
        )
        df = spark.createDataFrame(data, ["group", "id", "value"])

        result = detect_anomalies_statistical(
            df, "value", z_threshold=3.0, group_by_columns=["group"]
        )

        # Should detect anomalies in both groups
        assert result.count() >= 2

        group_a_anomalies = result.filter(F.col("group") == "A").collect()
        group_b_anomalies = result.filter(F.col("group") == "B").collect()

        assert len(group_a_anomalies) >= 1
        assert len(group_b_anomalies) >= 1

    def test_returns_zscore_column(self, spark):
        """Test that result includes z_score column."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        data = [(i, 100) for i in range(50)] + [(50, 1000)]
        df = spark.createDataFrame(data, ["id", "value"])

        result = detect_anomalies_statistical(df, "value", z_threshold=3.0)

        assert "z_score" in result.columns
        assert "anomaly_type" in result.columns
        assert "baseline_mean" in result.columns
        assert "baseline_stddev" in result.columns

    def test_custom_threshold(self, spark):
        """Test that custom z_threshold works correctly."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        # Create data with moderate outliers
        data = [(i, 100 + (i % 10)) for i in range(100)] + [(100, 200)]
        df = spark.createDataFrame(data, ["id", "value"])

        # With high threshold, fewer anomalies
        result_high = detect_anomalies_statistical(df, "value", z_threshold=5.0)

        # With low threshold, more anomalies
        result_low = detect_anomalies_statistical(df, "value", z_threshold=1.5)

        assert result_low.count() >= result_high.count()

    def test_preserves_original_columns(self, spark):
        """Test that original DataFrame columns are preserved."""
        from src.transforms.performance.anomalies import detect_anomalies_statistical

        data = [("user1", "iPad", 100), ("user1", "iPad", 1000)]
        df = spark.createDataFrame(data, ["user_id", "device", "value"])

        result = detect_anomalies_statistical(df, "value", z_threshold=2.0)

        # Original columns should be present
        assert "user_id" in result.columns
        assert "device" in result.columns
        assert "value" in result.columns
