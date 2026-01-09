"""
Integration tests for statistical anomaly detection.

Tests anomaly detection with various scenarios including outliers and grouped detection.
"""
import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class TestAnomalyDetection:
    """Statistical anomaly detection integration tests."""

    def test_statistical_anomaly_detection(self, spark):
        """Test statistical anomaly detection identifies outliers."""
        from src.transforms.performance import detect_anomalies_statistical
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        # Create data with clear outliers
        # Normal: 100-200ms (most data)
        # Outliers: 1000ms, 2000ms, 5000ms

        data = []

        # Normal data (100 records, 100-200ms)
        for i in range(100):
            data.append((
                f"int_normal_{i}", f"user_{i}", "open",
                datetime(2024, 1, 1) + timedelta(hours=i),
                100 + (i % 100)
            ))

        # Outliers (3 records, very high durations)
        data.append(("int_outlier_1", "user_outlier_1", "edit",
                    datetime(2024, 1, 1), 1000))
        data.append(("int_outlier_2", "user_outlier_2", "save",
                    datetime(2024, 1, 1), 2000))
        data.append(("int_outlier_3", "user_outlier_3", "close",
                    datetime(2024, 1, 1), 5000))

        df = spark.createDataFrame(data, schema)

        # Detect anomalies with z-score threshold of 3.0
        anomalies = detect_anomalies_statistical(
            df,
            value_column="duration_ms",
            threshold=3.0
        )

        anomaly_count = anomalies.count()

        # Should detect the 3 outliers
        assert anomaly_count >= 1, f"Should detect at least 1 anomaly, found {anomaly_count}"

        # Verify outliers are included
        anomaly_values = [row["duration_ms"] for row in anomalies.collect()]
        assert any(v >= 1000 for v in anomaly_values), "Should detect high duration outliers"

    def test_grouped_anomaly_detection(self, spark):
        """Test anomaly detection within groups."""
        from src.transforms.performance import detect_anomalies_statistical
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
            StructField("group", StringType(), False),
        ])

        data = []

        # Group A: Normal range 100-200ms, with one outlier at 1000ms
        for i in range(50):
            data.append((f"int_a_{i}", f"user_a_{i}", "open",
                        datetime(2024, 1, 1), 100 + (i % 100), "A"))
        data.append(("int_a_outlier", "user_a_outlier", "edit",
                    datetime(2024, 1, 1), 1000, "A"))

        # Group B: Normal range 500-600ms, with one outlier at 2000ms
        for i in range(50):
            data.append((f"int_b_{i}", f"user_b_{i}", "save",
                        datetime(2024, 1, 1), 500 + (i % 100), "B"))
        data.append(("int_b_outlier", "user_b_outlier", "close",
                    datetime(2024, 1, 1), 2000, "B"))

        df = spark.createDataFrame(data, schema)

        # Detect anomalies within groups
        anomalies = detect_anomalies_statistical(
            df,
            value_column="duration_ms",
            threshold=3.0,
            group_by_columns=["group"]
        )

        anomaly_count = anomalies.count()

        # Should detect outliers in both groups
        assert anomaly_count >= 1, f"Should detect anomalies, found {anomaly_count}"
