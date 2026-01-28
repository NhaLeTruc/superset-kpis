"""
Unit tests for anomaly detection transforms.

Tests detect_anomalies_statistical() function from performance transforms.
"""

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from src.transforms.performance import detect_anomalies_statistical


class TestDetectAnomaliesStatistical:
    """Tests for detect_anomalies_statistical() function."""

    def test_detect_anomalies_statistical_basic(self, spark):
        """
        GIVEN:
            - 28 days with normal values around 1000ms (stddev~100ms)
            - 2 days with extreme outliers: 5000ms, 6000ms
        WHEN: detect_anomalies_statistical() with z_threshold=3.0
        THEN:
            - Returns 2 anomalies
            - Both have z_score > 3.0
            - anomaly_type = "high"
        """
        # Arrange
        schema = StructType(
            [
                StructField("date", DateType(), nullable=False),
                StructField("avg_duration_ms", DoubleType(), nullable=False),
            ]
        )

        # 28 normal days: values between 900-1100
        import random

        random.seed(42)
        normal_data = [
            (date(2023, 1, day), 1000.0 + random.randint(-100, 100)) for day in range(1, 29)
        ]

        # 2 anomaly days
        outlier_data = [(date(2023, 1, 29), 5000.0), (date(2023, 1, 30), 6000.0)]

        df = spark.createDataFrame(normal_data + outlier_data, schema=schema)

        # Act
        result_df = detect_anomalies_statistical(
            df, value_column="avg_duration_ms", z_threshold=3.0
        )

        # Assert - function returns only anomalies
        assert result_df.count() == 2, "Expected 2 anomalies"

        # Check columns exist
        assert "z_score" in result_df.columns
        assert "baseline_mean" in result_df.columns
        assert "baseline_stddev" in result_df.columns
        assert "anomaly_type" in result_df.columns

        # Check all anomalies have z_score > 3.0 and type = "high"
        anomalies = result_df.collect()
        anomaly_dates = sorted([row["date"] for row in anomalies])
        assert anomaly_dates == [date(2023, 1, 29), date(2023, 1, 30)]

        for row in anomalies:
            assert row["z_score"] > 3.0, f"Expected z_score > 3.0, got {row['z_score']}"
            assert row["anomaly_type"] == "high"

    def test_detect_anomalies_statistical_grouped(self, spark):
        """
        GIVEN:
            - app_version=1.0.0: 28 normal values + 2 outliers (5000, 6000)
            - app_version=2.0.0: 30 normal values (no anomalies)
        WHEN: detect_anomalies_statistical() grouped by app_version
        THEN:
            - Returns 2 anomalies (both from v1.0.0)
            - No anomalies for v2.0.0
        """
        # Arrange
        schema = StructType(
            [
                StructField("app_version", StringType(), nullable=False),
                StructField("date", DateType(), nullable=False),
                StructField("avg_duration_ms", DoubleType(), nullable=False),
            ]
        )

        import random

        random.seed(42)

        # v1.0.0 - 28 normal + 2 outliers
        v1_normal = [
            ("1.0.0", date(2023, 1, day), 1000.0 + random.randint(-100, 100))
            for day in range(1, 29)
        ]
        v1_outliers = [("1.0.0", date(2023, 1, 29), 5000.0), ("1.0.0", date(2023, 1, 30), 6000.0)]

        # v2.0.0 - 30 normal values, no outliers
        v2_normal = [
            ("2.0.0", date(2023, 1, day), 2000.0 + random.randint(-100, 100))
            for day in range(1, 31)
        ]

        df = spark.createDataFrame(v1_normal + v1_outliers + v2_normal, schema=schema)

        # Act
        result_df = detect_anomalies_statistical(
            df, value_column="avg_duration_ms", group_by_columns=["app_version"], z_threshold=3.0
        )

        # Assert - function returns only anomalies
        assert result_df.count() == 2, "Expected 2 anomalies"

        # Check all anomalies are from v1.0.0
        anomalies = result_df.collect()
        for anomaly in anomalies:
            assert anomaly["app_version"] == "1.0.0"
            assert anomaly["z_score"] > 3.0
            assert anomaly["anomaly_type"] == "high"

        # Check specific dates
        anomaly_dates = sorted([row["date"] for row in anomalies])
        assert anomaly_dates == [date(2023, 1, 29), date(2023, 1, 30)]

        # Check v2.0.0 has no anomalies
        v2_anomalies = result_df.filter(F.col("app_version") == "2.0.0").count()
        assert v2_anomalies == 0, "v2.0.0 should have no anomalies"

    def test_detect_anomalies_statistical_no_anomalies(self, spark):
        """
        GIVEN: DataFrame with normal distribution (no outliers)
        WHEN: detect_anomalies_statistical() is called
        THEN: Returns 0 anomalies
        """
        # Arrange
        schema = StructType(
            [
                StructField("date", DateType(), nullable=False),
                StructField("avg_duration_ms", DoubleType(), nullable=False),
            ]
        )

        # Create data with small variance (no anomalies)
        data = [
            (date(2023, 1, i), 1000.0 + (i % 3) * 50.0)
            for i in range(1, 31)  # 30 days
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = detect_anomalies_statistical(
            df, value_column="avg_duration_ms", z_threshold=3.0
        )

        # Assert - function returns only anomalies, so empty = no anomalies
        assert result_df.count() == 0, "Expected no anomalies"
