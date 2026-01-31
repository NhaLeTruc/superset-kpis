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
        --master "$(SPARK_MASTER_URL)" \
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

        try:
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

        finally:
            # Ensure unpersist is called even if an exception occurs
            metadata_df.unpersist()
            enriched_df.unpersist()

        return metrics

    def print_summary(self, metrics: dict[str, DataFrame]) -> None:
        """Print summary of computed metrics."""
        print("\n" + "=" * 60)
        print("📊 Engagement Metrics Summary")
        print("=" * 60)

        # DAU Summary - single aggregation
        dau_df = metrics["dau"]
        dau_stats = dau_df.agg(
            F.avg("daily_active_users").alias("avg_dau"),
            F.max("daily_active_users").alias("max_dau"),
            F.min("daily_active_users").alias("min_dau"),
        ).collect()[0]
        print("\nDaily Active Users:")
        avg_dau = dau_stats["avg_dau"] if dau_stats["avg_dau"] is not None else 0.0
        max_dau = dau_stats["max_dau"] if dau_stats["max_dau"] is not None else 0
        min_dau = dau_stats["min_dau"] if dau_stats["min_dau"] is not None else 0
        print(f"  Average DAU: {avg_dau:,.0f}")
        print(f"  Max DAU: {max_dau:,}")
        print(f"  Min DAU: {min_dau:,}")

        # MAU Summary
        mau_df = metrics["mau"]
        mau_stats = mau_df.agg(F.avg("monthly_active_users").alias("avg_mau")).collect()[0]
        print("\nMonthly Active Users:")
        avg_mau = mau_stats["avg_mau"] if mau_stats["avg_mau"] is not None else 0.0
        print(f"  Average MAU: {avg_mau:,.0f}")

        # Stickiness Summary - single aggregation
        stickiness_df = metrics["stickiness"]
        stickiness_stats = stickiness_df.agg(
            F.avg("stickiness_ratio").alias("avg_stick"),
            F.max("stickiness_ratio").alias("max_stick"),
            F.min("stickiness_ratio").alias("min_stick"),
        ).collect()[0]
        print("\nStickiness Ratio:")
        avg_stick = (
            stickiness_stats["avg_stick"] if stickiness_stats["avg_stick"] is not None else 0.0
        )
        max_stick = (
            stickiness_stats["max_stick"] if stickiness_stats["max_stick"] is not None else 0.0
        )
        min_stick = (
            stickiness_stats["min_stick"] if stickiness_stats["min_stick"] is not None else 0.0
        )
        print(f"  Average: {avg_stick:.2%}")
        print(f"  Max: {max_stick:.2%}")
        print(f"  Min: {min_stick:.2%}")

        # Power Users Summary - single aggregation combining count and sum
        power_users_df = metrics["power_users"]
        power_stats = power_users_df.agg(
            F.count("*").alias("count"),
            F.sum("hours_spent").alias("total_hours"),
        ).collect()[0]
        power_user_count = power_stats["count"]
        total_hours = power_stats["total_hours"] if power_stats["total_hours"] is not None else 0.0
        print("\nPower Users:")
        print(f"  Count: {power_user_count:,} (top 1%)")
        print(f"  Total Engagement: {total_hours:,.0f} hours")

        # Cohort Retention Summary - combined query
        cohort_df = metrics["cohort_retention"]
        cohort_stats = (
            cohort_df.filter("week_number IN (0, 12)")
            .groupBy("week_number")
            .agg(F.avg("retention_rate").alias("avg_retention"))
            .collect()
        )

        retention_by_week = {row["week_number"]: row["avg_retention"] for row in cohort_stats}
        avg_retention = retention_by_week.get(0, 0.0) or 0.0
        week_12_avg = retention_by_week.get(12, 0.0) or 0.0
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
