"""
Unit tests for src/config/constants.py.

Verify that all public constants have the correct types and expected values.
"""

from __future__ import annotations

from typing import ClassVar

from src.config.constants import (
    ANOMALY_SEVERITY_CRITICAL,
    ANOMALY_SEVERITY_HIGH,
    ANOMALY_SEVERITY_MEDIUM,
    DEFAULT_PERCENTILES,
    HOT_KEY_THRESHOLD_PERCENTILE,
    SESSION_TIMEOUT_SECONDS,
    TABLE_BOUNCE_RATES,
    TABLE_COHORT_RETENTION,
    TABLE_DAILY_ACTIVE_USERS,
    TABLE_DEVICE_CORRELATION,
    TABLE_DEVICE_PERFORMANCE,
    TABLE_MONTHLY_ACTIVE_USERS,
    TABLE_PERFORMANCE_ANOMALIES,
    TABLE_PERFORMANCE_BY_VERSION,
    TABLE_POWER_USERS,
    TABLE_SESSION_FREQUENCY,
    TABLE_SESSION_METRICS,
    TABLE_USER_INTERACTIONS,
    TABLE_USER_METADATA,
    TABLE_USER_STICKINESS,
    Z_SCORE_ANOMALY_THRESHOLD,
)


class TestNumericThresholds:
    """Verify numeric thresholds have correct types and ordering."""

    def test_z_score_anomaly_threshold_is_float(self):
        assert isinstance(Z_SCORE_ANOMALY_THRESHOLD, float)

    def test_z_score_anomaly_threshold_equals_medium_severity(self):
        """Detection threshold must equal the lowest severity tier."""
        assert Z_SCORE_ANOMALY_THRESHOLD == ANOMALY_SEVERITY_MEDIUM

    def test_anomaly_severity_ordering(self):
        """MEDIUM < HIGH < CRITICAL — required by severity classification logic."""
        assert ANOMALY_SEVERITY_MEDIUM < ANOMALY_SEVERITY_HIGH < ANOMALY_SEVERITY_CRITICAL

    def test_hot_key_percentile_in_range(self):
        assert 0.0 < HOT_KEY_THRESHOLD_PERCENTILE < 1.0

    def test_session_timeout_positive(self):
        assert SESSION_TIMEOUT_SECONDS > 0

    def test_default_percentiles_ordered(self):
        assert sorted(DEFAULT_PERCENTILES) == DEFAULT_PERCENTILES

    def test_default_percentiles_in_range(self):
        for p in DEFAULT_PERCENTILES:
            assert 0.0 < p < 1.0


class TestTableNameConstants:
    """Verify table name constants are non-empty lowercase strings."""

    TABLE_CONSTANTS: ClassVar[list[str]] = [
        TABLE_DAILY_ACTIVE_USERS,
        TABLE_MONTHLY_ACTIVE_USERS,
        TABLE_USER_STICKINESS,
        TABLE_POWER_USERS,
        TABLE_COHORT_RETENTION,
        TABLE_PERFORMANCE_BY_VERSION,
        TABLE_DEVICE_PERFORMANCE,
        TABLE_DEVICE_CORRELATION,
        TABLE_PERFORMANCE_ANOMALIES,
        TABLE_SESSION_METRICS,
        TABLE_SESSION_FREQUENCY,
        TABLE_BOUNCE_RATES,
        TABLE_USER_METADATA,
        TABLE_USER_INTERACTIONS,
    ]

    def test_all_table_names_are_strings(self):
        for name in self.TABLE_CONSTANTS:
            assert isinstance(name, str), f"Expected str, got {type(name)} for {name!r}"

    def test_all_table_names_are_non_empty(self):
        for name in self.TABLE_CONSTANTS:
            assert name, "Table name constant must not be empty"

    def test_all_table_names_are_lowercase(self):
        for name in self.TABLE_CONSTANTS:
            assert name == name.lower(), f"Table name {name!r} must be lowercase"

    def test_table_names_are_unique(self):
        assert len(self.TABLE_CONSTANTS) == len(set(self.TABLE_CONSTANTS))

    def test_user_metadata_table_name(self):
        assert TABLE_USER_METADATA == "user_metadata"

    def test_user_interactions_table_name(self):
        assert TABLE_USER_INTERACTIONS == "user_interactions"
