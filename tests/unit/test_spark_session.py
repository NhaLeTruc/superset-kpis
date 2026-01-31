"""
Unit tests for Spark session creation and configuration.

Tests the create_spark_session and get_spark_config_summary functions.
"""


class TestCreateSparkSession:
    """Test suite for create_spark_session function."""

    def test_creates_session_with_app_name(self, spark):
        """Test that spark session has correct app name."""
        # The fixture creates a session with app name "goodnote-tests"
        assert spark.sparkContext.appName == "goodnote-tests"

    def test_session_is_active(self, spark):
        """Test that the session is active and can run queries."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        assert df.count() == 2


class TestGetSparkConfigSummary:
    """Test suite for get_spark_config_summary function."""

    def test_returns_config_dict(self, spark):
        """Test that config summary returns a dictionary."""
        from src.config.spark_session import get_spark_config_summary

        summary = get_spark_config_summary(spark)

        assert isinstance(summary, dict)
        assert "app_name" in summary
        assert "master" in summary

    def test_includes_sql_config_values(self, spark):
        """Test that SQL configs are retrieved correctly via spark.conf API."""
        from src.config.spark_session import get_spark_config_summary

        # Set a SQL config
        spark.conf.set("spark.sql.shuffle.partitions", "42")

        summary = get_spark_config_summary(spark)

        # The fix ensures we use spark.conf.get() for SQL configs
        assert summary["shuffle_partitions"] == "42"

    def test_includes_aqe_status(self, spark):
        """Test that AQE enabled status is included."""
        from src.config.spark_session import get_spark_config_summary

        summary = get_spark_config_summary(spark)

        # AQE status should be present (value depends on spark version/config)
        assert "aqe_enabled" in summary
