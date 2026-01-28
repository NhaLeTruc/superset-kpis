#!/usr/bin/env python3
"""
Job 2: User Engagement Analytics

Calculates engagement metrics:
- Daily Active Users (DAU)
- Monthly Active Users (MAU)
- Stickiness Ratio (DAU/MAU)
- Power Users (Top 1%)
- Cohort Retention (Weekly cohorts, 6 months)

Writes results to PostgreSQL for dashboard consumption.

Usage (via helper script):
    ./scripts/run_spark_job.sh src/jobs/02_user_engagement.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db

Usage (direct spark-submit):
    docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/02_user_engagement.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db'
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import (
    HOT_KEY_THRESHOLD_PERCENTILE,
    TABLE_COHORT_RETENTION,
    TABLE_DAILY_ACTIVE_USERS,
    TABLE_MONTHLY_ACTIVE_USERS,
    TABLE_POWER_USERS,
    TABLE_USER_STICKINESS,
)
from src.jobs.base_job import BaseAnalyticsJob
from src.schemas.columns import (
    COL_COUNTRY,
    COL_DEVICE_TYPE,
    COL_REGISTRATION_DATE,
    COL_SUBSCRIPTION_TYPE,
    COL_USER_ID,
)
from src.transforms.engagement import (
    calculate_cohort_retention,
    calculate_dau,
    calculate_mau,
    calculate_stickiness,
    identify_power_users,
)


class UserEngagementJob(BaseAnalyticsJob):
    """User Engagement Analytics Job."""

    def __init__(self):
        super().__init__(job_name="User Engagement Analytics", job_type="analytics")

    def get_argument_parser(self) -> argparse.ArgumentParser:
        """Configure job-specific arguments."""
        parser = argparse.ArgumentParser(description="GoodNote User Engagement Analytics")
        parser.add_argument(
            "--enriched-path", required=True, help="Path to enriched interactions Parquet"
        )
        parser.add_argument(
            "--write-to-db", action="store_true", help="Write results to PostgreSQL"
        )
        parser.add_argument("--output-path", help="Optional: Write results to Parquet")
        return parser

    def compute_metrics(self) -> dict[str, DataFrame]:
        """
        Compute all engagement metrics.

        Returns:
            Dictionary with DataFrames for each metric
        """
        # Read enriched data
        enriched_df = self.read_parquet(self.args.enriched_path, "enriched interactions")

        metrics = {}

        # Extract metadata_df once and cache it (used by power_users and cohort_retention)
        metadata_df = (
            enriched_df.select(
                COL_USER_ID,
                COL_REGISTRATION_DATE,
                COL_COUNTRY,
                COL_DEVICE_TYPE,
                COL_SUBSCRIPTION_TYPE,
            )
            .distinct()
            .persist()
        )

        # 1. Daily Active Users (DAU)
        print("\n📊 Calculating Daily Active Users (DAU)...")
        dau_df = calculate_dau(enriched_df)
        print("   ✅ DAU calculation complete")
        metrics["dau"] = dau_df

        # 2. Monthly Active Users (MAU)
        print("\n📊 Calculating Monthly Active Users (MAU)...")
        mau_df = calculate_mau(enriched_df)
        print("   ✅ MAU calculation complete")
        metrics["mau"] = mau_df

        # 3. Stickiness Ratio (DAU/MAU)
        print("\n📊 Calculating Stickiness Ratio...")
        stickiness_df = calculate_stickiness(dau_df, mau_df)
        print("   ✅ Stickiness calculation complete")
        metrics["stickiness"] = stickiness_df

        # 4. Power Users (Top 1%)
        print("\n📊 Identifying Power Users...")
        power_users_df = identify_power_users(
            enriched_df, metadata_df, percentile=HOT_KEY_THRESHOLD_PERCENTILE
        )
        print("   ✅ Power users identification complete")
        metrics["power_users"] = power_users_df

        # 5. Cohort Retention (Weekly cohorts, 6 months)
        print("\n📊 Calculating Cohort Retention...")
        cohort_df = calculate_cohort_retention(
            enriched_df, metadata_df, cohort_period="week", retention_weeks=26
        )
        print("   ✅ Cohort retention calculation complete")
        metrics["cohort_retention"] = cohort_df

        # Unpersist cached DataFrames
        metadata_df.unpersist()
        enriched_df.unpersist()

        return metrics

    def print_summary(self, metrics: dict[str, DataFrame]) -> None:
        """Print summary of computed metrics."""
        print("\n" + "=" * 60)
        print("📊 Engagement Metrics Summary")
        print("=" * 60)

        # DAU Summary
        dau_df = metrics["dau"]
        dau_stats = dau_df.select(
            F.avg("daily_active_users"), F.max("daily_active_users"), F.min("daily_active_users")
        ).collect()[0]
        print("\nDaily Active Users:")
        print(f"  Average DAU: {dau_stats[0]:,.0f}")
        print(f"  Max DAU: {dau_stats[1]:,}")
        print(f"  Min DAU: {dau_stats[2]:,}")

        # MAU Summary
        mau_df = metrics["mau"]
        mau_stats = mau_df.agg({"monthly_active_users": "avg"}).collect()[0]
        print("\nMonthly Active Users:")
        print(f"  Average MAU: {mau_stats[0]:,.0f}")

        # Stickiness Summary
        stickiness_df = metrics["stickiness"]
        stickiness_stats = stickiness_df.select(
            F.avg("stickiness_ratio"), F.max("stickiness_ratio"), F.min("stickiness_ratio")
        ).collect()[0]
        print("\nStickiness Ratio:")
        print(f"  Average: {stickiness_stats[0]:.2%}")
        print(f"  Max: {stickiness_stats[1]:.2%}")
        print(f"  Min: {stickiness_stats[2]:.2%}")

        # Power Users Summary
        power_users_df = metrics["power_users"]
        power_user_count = power_users_df.count()
        total_hours_result = power_users_df.agg({"hours_spent": "sum"}).collect()[0][0]
        total_hours = total_hours_result if total_hours_result is not None else 0.0
        print("\nPower Users:")
        print(f"  Count: {power_user_count:,} (top 1%)")
        print(f"  Total Engagement: {total_hours:,.0f} hours")

        # Cohort Retention Summary
        cohort_df = metrics["cohort_retention"]
        avg_retention_result = (
            cohort_df.filter("week_number = 0").agg({"retention_rate": "avg"}).collect()[0][0]
        )
        avg_retention = avg_retention_result if avg_retention_result is not None else 0.0
        week_12_result = (
            cohort_df.filter("week_number = 12").agg({"retention_rate": "avg"}).collect()[0][0]
        )
        week_12_avg = week_12_result if week_12_result is not None else 0.0
        print("\nCohort Retention:")
        print(f"  Average Week 0: {avg_retention:.2%}")
        print(f"  Average Week 12: {week_12_avg:.2%}")

        print("=" * 60)

    def get_table_mapping(self) -> dict[str, str]:
        """Get database table mapping."""
        return {
            "dau": TABLE_DAILY_ACTIVE_USERS,
            "mau": TABLE_MONTHLY_ACTIVE_USERS,
            "stickiness": TABLE_USER_STICKINESS,
            "power_users": TABLE_POWER_USERS,
            "cohort_retention": TABLE_COHORT_RETENTION,
        }


def main():
    """Main job execution."""
    job = UserEngagementJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
