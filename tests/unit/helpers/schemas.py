"""
Schema Factory Functions

Provides reusable schema definitions for test DataFrames.
Eliminates duplication of StructType definitions across tests.
"""
from typing import Optional, List
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, DateType, DoubleType, BooleanType
)


def interactions_schema(extra_fields: Optional[List[StructField]] = None) -> StructType:
    """
    Standard schema for user interactions.

    Base fields:
    - user_id (string)
    - timestamp (timestamp)
    - duration_ms (long)
    - action_type (string, nullable)
    - page_id (string, nullable)
    - app_version (string, nullable)

    Args:
        extra_fields: Optional additional fields to append

    Returns:
        StructType schema
    """
    base_fields = [
        StructField("user_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("duration_ms", LongType(), nullable=False),
        StructField("action_type", StringType(), nullable=True),
        StructField("page_id", StringType(), nullable=True),
        StructField("app_version", StringType(), nullable=True),
    ]

    if extra_fields:
        base_fields.extend(extra_fields)

    return StructType(base_fields)


def metadata_schema(extra_fields: Optional[List[StructField]] = None) -> StructType:
    """
    Standard schema for user metadata.

    Base fields:
    - user_id (string)
    - join_date (date)
    - country (string)
    - device_type (string)
    - subscription_type (string)

    Args:
        extra_fields: Optional additional fields to append

    Returns:
        StructType schema
    """
    base_fields = [
        StructField("user_id", StringType(), nullable=False),
        StructField("join_date", DateType(), nullable=False),
        StructField("country", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("subscription_type", StringType(), nullable=True),
    ]

    if extra_fields:
        base_fields.extend(extra_fields)

    return StructType(base_fields)


def session_schema(extra_fields: Optional[List[StructField]] = None) -> StructType:
    """
    Standard schema for sessionized data.

    Base fields:
    - user_id (string)
    - session_id (string)
    - timestamp (timestamp)
    - duration_ms (long, nullable)

    Args:
        extra_fields: Optional additional fields to append

    Returns:
        StructType schema
    """
    base_fields = [
        StructField("user_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("duration_ms", LongType(), nullable=True),
    ]

    if extra_fields:
        base_fields.extend(extra_fields)

    return StructType(base_fields)


def aggregation_schema(metric_type: str) -> StructType:
    """
    Schema for aggregated metrics.

    Args:
        metric_type: Type of aggregation - "daily", "monthly", "device"

    Returns:
        StructType schema appropriate for the metric type
    """
    if metric_type == "daily":
        return StructType([
            StructField("date", DateType(), nullable=False),
            StructField("count", LongType(), nullable=False),
            StructField("metric_value", DoubleType(), nullable=True),
        ])
    elif metric_type == "monthly":
        return StructType([
            StructField("month", DateType(), nullable=False),
            StructField("count", LongType(), nullable=False),
            StructField("metric_value", DoubleType(), nullable=True),
        ])
    elif metric_type == "device":
        return StructType([
            StructField("device_type", StringType(), nullable=False),
            StructField("count", LongType(), nullable=False),
            StructField("metric_value", DoubleType(), nullable=True),
        ])
    else:
        raise ValueError(f"Unknown metric type: {metric_type}")


def dau_schema() -> StructType:
    """Schema for Daily Active Users (DAU) results."""
    return StructType([
        StructField("date", DateType(), nullable=False),
        StructField("daily_active_users", LongType(), nullable=False),
        StructField("total_interactions", LongType(), nullable=False),
        StructField("total_duration_ms", LongType(), nullable=False),
        StructField("avg_duration_per_user", DoubleType(), nullable=False),
    ])


def mau_schema() -> StructType:
    """Schema for Monthly Active Users (MAU) results."""
    return StructType([
        StructField("month", DateType(), nullable=False),
        StructField("monthly_active_users", LongType(), nullable=False),
        StructField("total_interactions", LongType(), nullable=False),
    ])


def stickiness_schema() -> StructType:
    """Schema for stickiness ratio results."""
    return StructType([
        StructField("month", DateType(), nullable=False),
        StructField("avg_dau", DoubleType(), nullable=False),
        StructField("monthly_active_users", LongType(), nullable=False),
        StructField("stickiness_ratio", DoubleType(), nullable=False),
    ])


def power_users_schema() -> StructType:
    """Schema for power users identification results."""
    return StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("total_duration_ms", LongType(), nullable=False),
        StructField("total_interactions", LongType(), nullable=False),
        StructField("days_active", LongType(), nullable=False),
        StructField("unique_pages", LongType(), nullable=True),
        StructField("hours_spent", DoubleType(), nullable=False),
        StructField("avg_duration_per_interaction", DoubleType(), nullable=False),
        StructField("avg_duration_ms", DoubleType(), nullable=False),
        StructField("percentile_rank", DoubleType(), nullable=False),
        StructField("country", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("subscription_type", StringType(), nullable=True),
    ])


def cohort_retention_schema() -> StructType:
    """Schema for cohort retention analysis results."""
    return StructType([
        StructField("cohort_week", DateType(), nullable=False),
        StructField("week_number", IntegerType(), nullable=False),
        StructField("cohort_size", LongType(), nullable=False),
        StructField("active_users", LongType(), nullable=False),
        StructField("retention_rate", DoubleType(), nullable=False),
    ])


def hot_keys_schema() -> StructType:
    """Schema for hot key detection results."""
    return StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("interaction_count", LongType(), nullable=False),
    ])


def percentiles_schema() -> StructType:
    """Schema for percentile calculation results."""
    return StructType([
        StructField("app_version", StringType(), nullable=False),
        StructField("metric_date", DateType(), nullable=False),
        StructField("percentile_0.5", DoubleType(), nullable=False),
        StructField("percentile_0.95", DoubleType(), nullable=False),
        StructField("percentile_0.99", DoubleType(), nullable=False),
    ])


def anomaly_schema() -> StructType:
    """Schema for anomaly detection results."""
    return StructType([
        StructField("app_version", StringType(), nullable=False),
        StructField("metric_date", DateType(), nullable=False),
        StructField("metric_value", DoubleType(), nullable=False),
        StructField("mean", DoubleType(), nullable=False),
        StructField("stddev", DoubleType(), nullable=False),
        StructField("z_score", DoubleType(), nullable=False),
        StructField("is_anomaly", BooleanType(), nullable=False),
        StructField("anomaly_type", StringType(), nullable=True),
    ])
