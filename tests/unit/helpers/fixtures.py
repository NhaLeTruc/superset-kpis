"""
Test Data Generators

Provides reusable functions for generating test data.
Eliminates duplication of data generation logic across tests.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from .schemas import interactions_schema, metadata_schema


def generate_interactions(
    spark: SparkSession,
    n_users: int = 100,
    interactions_per_user: int = 10,
    start_date: date = date(2023, 1, 1),
    num_days: int = 30,
    action_types: list[str] | None = None,
    include_page_id: bool = True,
    include_app_version: bool = True,
) -> DataFrame:
    """
    Generate synthetic interaction data.

    Args:
        spark: SparkSession
        n_users: Number of unique users
        interactions_per_user: Average interactions per user
        start_date: Start date for interactions
        num_days: Number of days to span
        action_types: List of action types (default: ["view", "edit", "share"])
        include_page_id: Include page_id column
        include_app_version: Include app_version column

    Returns:
        DataFrame with interaction data
    """
    if action_types is None:
        action_types = ["view", "edit", "share", "delete"]

    data = []
    for user_idx in range(n_users):
        user_id = f"u{user_idx:03d}"

        for interaction_idx in range(interactions_per_user):
            # Random day within range
            day_offset = (user_idx * interactions_per_user + interaction_idx) % num_days
            interaction_date = start_date + timedelta(days=day_offset)

            # Random time within day
            hour = (interaction_idx * 3) % 24
            minute = (interaction_idx * 13) % 60
            timestamp = datetime.combine(interaction_date, datetime.min.time())
            timestamp = timestamp.replace(hour=hour, minute=minute)

            # Duration (vary by user)
            base_duration = 5000 + (user_idx * 100)
            duration_ms = base_duration + (interaction_idx * 500)

            # Action type (cycle through)
            action_type = (
                action_types[interaction_idx % len(action_types)] if action_types else None
            )

            # Page ID
            page_id = f"page_{interaction_idx % 20}" if include_page_id else None

            # App version
            app_version = f"1.{(interaction_idx // 5) % 3}.0" if include_app_version else None

            data.append((user_id, timestamp, duration_ms, action_type, page_id, app_version))

    schema = interactions_schema()
    return spark.createDataFrame(data, schema=schema)


def generate_metadata(
    spark: SparkSession,
    n_users: int = 100,
    start_join_date: date = date(2022, 1, 1),
    countries: list[str] | None = None,
    device_types: list[str] | None = None,
    subscription_types: list[str] | None = None,
) -> DataFrame:
    """
    Generate synthetic user metadata.

    Args:
        spark: SparkSession
        n_users: Number of users
        start_join_date: Earliest join date
        countries: List of countries (default: ["US", "UK", "CA", "AU"])
        device_types: List of device types (default: ["iOS", "Android", "Web"])
        subscription_types: List of subscription types (default: ["free", "premium", "enterprise"])

    Returns:
        DataFrame with user metadata
    """
    if countries is None:
        countries = ["US", "UK", "CA", "AU", "DE", "FR", "JP"]

    if device_types is None:
        device_types = ["iOS", "Android", "Web"]

    if subscription_types is None:
        subscription_types = ["free", "premium", "enterprise"]

    data = []
    for user_idx in range(n_users):
        user_id = f"u{user_idx:03d}"

        # Join date (spread over 6 months)
        days_offset = (user_idx * 7) % 180
        join_date = start_join_date + timedelta(days=days_offset)

        # Attributes (deterministic based on user_idx)
        country = countries[user_idx % len(countries)]
        device_type = device_types[user_idx % len(device_types)]
        subscription_type = subscription_types[user_idx % len(subscription_types)]

        data.append((user_id, join_date, country, device_type, subscription_type))

    schema = metadata_schema()
    return spark.createDataFrame(data, schema=schema)


def generate_skewed_data(
    spark: SparkSession,
    hot_keys: int = 10,
    normal_keys: int = 90,
    interactions_per_hot_key: int = 10000,
    interactions_per_normal_key: int = 100,
    start_date: date = date(2023, 1, 1),
) -> DataFrame:
    """
    Generate data with power-law distribution (data skew).

    Creates hot keys with many interactions and normal keys with few.

    Args:
        spark: SparkSession
        hot_keys: Number of hot key users
        normal_keys: Number of normal users
        interactions_per_hot_key: Interactions for each hot key
        interactions_per_normal_key: Interactions for each normal key
        start_date: Start date

    Returns:
        DataFrame with skewed interaction distribution
    """
    data = []

    # Generate hot key data
    for user_idx in range(hot_keys):
        user_id = f"hot_user_{user_idx:03d}"

        for interaction_idx in range(interactions_per_hot_key):
            day_offset = interaction_idx % 30
            timestamp = datetime.combine(
                start_date + timedelta(days=day_offset), datetime.min.time()
            )
            timestamp = timestamp.replace(hour=(interaction_idx % 24))

            duration_ms = 5000 + (interaction_idx % 10000)

            data.append(
                (user_id, timestamp, duration_ms, "view", f"page_{interaction_idx % 100}", "1.0.0")
            )

    # Generate normal key data
    for user_idx in range(normal_keys):
        user_id = f"normal_user_{user_idx:03d}"

        for interaction_idx in range(interactions_per_normal_key):
            day_offset = interaction_idx % 30
            timestamp = datetime.combine(
                start_date + timedelta(days=day_offset), datetime.min.time()
            )
            timestamp = timestamp.replace(hour=(interaction_idx % 24))

            duration_ms = 5000 + (interaction_idx % 1000)

            data.append(
                (user_id, timestamp, duration_ms, "view", f"page_{interaction_idx % 10}", "1.0.0")
            )

    schema = interactions_schema()
    return spark.createDataFrame(data, schema=schema)


def generate_cohort_data(
    spark: SparkSession,
    num_cohorts: int = 4,
    cohort_size: int = 100,
    num_weeks: int = 12,
    retention_rates: list[float] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """
    Generate cohort data for retention analysis.

    Args:
        spark: SparkSession
        num_cohorts: Number of weekly cohorts
        cohort_size: Users per cohort
        num_weeks: Weeks to track retention
        retention_rates: List of retention rates by week (default: declining curve)

    Returns:
        Tuple of (interactions_df, metadata_df)
    """
    if retention_rates is None:
        # Default: exponential decay retention curve
        retention_rates = [1.0, 0.7, 0.5, 0.4, 0.35, 0.3, 0.27, 0.25, 0.23, 0.21, 0.20, 0.19]

    start_date = date(2023, 1, 1)
    interactions = []
    metadata = []

    for cohort_idx in range(num_cohorts):
        cohort_start = start_date + timedelta(weeks=cohort_idx)

        for user_idx in range(cohort_size):
            user_id = f"cohort{cohort_idx}_user{user_idx:03d}"

            # Add to metadata
            metadata.append((user_id, cohort_start, "US", "iOS", "premium"))

            # Generate interactions based on retention curve
            for week_idx in range(num_weeks):
                # Check if user is active this week based on retention rate
                retention_rate = retention_rates[min(week_idx, len(retention_rates) - 1)]

                # Deterministic: user_idx determines if active
                is_active = (user_idx / cohort_size) < retention_rate

                if is_active:
                    # Generate 1-5 interactions this week
                    num_interactions = 1 + (user_idx % 5)
                    interaction_week = cohort_start + timedelta(weeks=week_idx)

                    for interaction_idx in range(num_interactions):
                        interaction_day = interaction_week + timedelta(days=interaction_idx % 7)
                        timestamp = datetime.combine(interaction_day, datetime.min.time())
                        timestamp = timestamp.replace(hour=(interaction_idx * 3) % 24)

                        interactions.append(
                            (
                                user_id,
                                timestamp,
                                5000 + (interaction_idx * 1000),
                                "view",
                                f"page_{interaction_idx}",
                                "1.0.0",
                            )
                        )

    interactions_schema_obj = interactions_schema()
    metadata_schema_obj = metadata_schema()

    interactions_df = spark.createDataFrame(interactions, schema=interactions_schema_obj)
    metadata_df = spark.createDataFrame(metadata, schema=metadata_schema_obj)

    return interactions_df, metadata_df


def generate_date_series(start_date: date, num_days: int, metric_value_fn=None) -> list[tuple]:
    """
    Generate time series data.

    Args:
        start_date: Start date
        num_days: Number of days
        metric_value_fn: Function to generate metric value (takes day_idx)

    Returns:
        List of tuples (date, value)
    """
    if metric_value_fn is None:

        def metric_value_fn(day_idx):
            return 100 + (day_idx * 5)

    data = []
    for day_idx in range(num_days):
        current_date = start_date + timedelta(days=day_idx)
        value = metric_value_fn(day_idx)
        data.append((current_date, value))

    return data
