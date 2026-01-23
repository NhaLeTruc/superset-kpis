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
import argparse
import sys
from typing import Dict

from pyspark.sql import DataFrame

from src.jobs.base_job import BaseAnalyticsJob
from src.schemas.columns import (
    COL_USER_ID, COL_REGISTRATION_DATE, COL_COUNTRY, COL_DEVICE_TYPE, COL_SUBSCRIPTION_TYPE,
)
from src.config.constants import (
    HOT_KEY_THRESHOLD_PERCENTILE,
    TABLE_DAILY_ACTIVE_USERS, TABLE_MONTHLY_ACTIVE_USERS,
    TABLE_USER_STICKINESS, TABLE_POWER_USERS, TABLE_COHORT_RETENTION,
)
from src.transforms.engagement import (
    calculate_dau,
    calculate_mau,
    calculate_stickiness,
    identify_power_users,
    calculate_cohort_retention
)


class UserEngagementJob(BaseAnalyticsJob):
    """User Engagement Analytics Job."""

    def __init__(self):
        super().__init__(
            job_name="User Engagement Analytics",
            job_type="analytics"
        )

    def get_argument_parser(self) -> argparse.ArgumentParser:
        """Configure job-specific arguments."""
        parser = argparse.ArgumentParser(description="GoodNote User Engagement Analytics")
        parser.add_argument("--enriched-path", required=True,
                          help="Path to enriched interactions Parquet")
        parser.add_argument("--write-to-db", action="store_true",
                          help="Write results to PostgreSQL")
        parser.add_argument("--output-path",
                          help="Optional: Write results to Parquet")
        return parser

    def compute_metrics(self) -> Dict[str, DataFrame]:
        """
        Compute all engagement metrics.

        Returns:
            Dictionary with DataFrames for each metric
        """
        # Read enriched data
        enriched_df = self.read_enriched_data(self.args.enriched_path)

        metrics = {}

        # 1. Daily Active Users (DAU)
        print("\n📊 Calculating Daily Active Users (DAU)...")
        dau_df = calculate_dau(enriched_df)
        dau_count = dau_df.count()
        print(f"   ✅ Computed DAU for {dau_count} days")
        metrics["dau"] = dau_df

        # 2. Monthly Active Users (MAU)
        print("\n📊 Calculating Monthly Active Users (MAU)...")
        mau_df = calculate_mau(enriched_df)
        mau_count = mau_df.count()
        print(f"   ✅ Computed MAU for {mau_count} months")
        metrics["mau"] = mau_df

        # 3. Stickiness Ratio (DAU/MAU)
        print("\n📊 Calculating Stickiness Ratio...")
        stickiness_df = calculate_stickiness(dau_df, mau_df)
        stickiness_count = stickiness_df.count()
        avg_stickiness = stickiness_df.agg({"stickiness": "avg"}).collect()[0][0]
        print(f"   ✅ Computed stickiness for {stickiness_count} days")
        print(f"   📈 Average stickiness: {avg_stickiness:.2%}")
        metrics["stickiness"] = stickiness_df

        # 4. Power Users (Top 1%)
        print("\n📊 Identifying Power Users...")
        metadata_df = enriched_df.select(
            COL_USER_ID, COL_REGISTRATION_DATE, COL_COUNTRY, COL_DEVICE_TYPE, COL_SUBSCRIPTION_TYPE
        ).distinct()
        power_users_df = identify_power_users(
            enriched_df,
            metadata_df,
            percentile=HOT_KEY_THRESHOLD_PERCENTILE
        )
        power_user_count = power_users_df.count()
        print(f"   ✅ Identified {power_user_count} power users (top 1%)")
        metrics["power_users"] = power_users_df

        # 5. Cohort Retention (Weekly cohorts, 6 months)
        print("\n📊 Calculating Cohort Retention...")
        cohort_df = calculate_cohort_retention(
            enriched_df,
            metadata_df,
            cohort_period="week",
            retention_weeks=26
        )
        cohort_count = cohort_df.count()
        print(f"   ✅ Computed retention for {cohort_count} cohort-week combinations")
        metrics["cohort_retention"] = cohort_df

        return metrics

    def print_summary(self, metrics: Dict[str, DataFrame]) -> None:
        """Print summary of computed metrics."""
        print("\n" + "=" * 60)
        print("📊 Engagement Metrics Summary")
        print("=" * 60)

        # DAU Summary
        dau_df = metrics["dau"]
        dau_stats = dau_df.agg({"dau": "avg", "dau": "max", "dau": "min"}).collect()[0]
        print(f"\nDaily Active Users:")
        print(f"  Average DAU: {dau_stats[0]:,.0f}")
        print(f"  Max DAU: {dau_stats[1]:,}")
        print(f"  Min DAU: {dau_stats[2]:,}")

        # MAU Summary
        mau_df = metrics["mau"]
        mau_stats = mau_df.agg({"mau": "avg"}).collect()[0]
        print(f"\nMonthly Active Users:")
        print(f"  Average MAU: {mau_stats[0]:,.0f}")

        # Stickiness Summary
        stickiness_df = metrics["stickiness"]
        stickiness_stats = stickiness_df.agg({
            "stickiness": "avg",
            "stickiness": "max",
            "stickiness": "min"
        }).collect()[0]
        print(f"\nStickiness Ratio:")
        print(f"  Average: {stickiness_stats[0]:.2%}")
        print(f"  Max: {stickiness_stats[1]:.2%}")
        print(f"  Min: {stickiness_stats[2]:.2%}")

        # Power Users Summary
        power_users_df = metrics["power_users"]
        power_user_count = power_users_df.count()
        total_hours = power_users_df.agg({"total_duration_hours": "sum"}).collect()[0][0]
        print(f"\nPower Users:")
        print(f"  Count: {power_user_count:,} (top 1%)")
        print(f"  Total Engagement: {total_hours:,.0f} hours")

        # Cohort Retention Summary
        cohort_df = metrics["cohort_retention"]
        avg_retention = cohort_df.filter("week_number = 0").agg({"retention_rate": "avg"}).collect()[0][0]
        week_12_retention = cohort_df.filter("week_number = 12").agg({"retention_rate": "avg"}).collect()[0]
        week_12_avg = week_12_retention[0] if week_12_retention[0] else 0.0
        print(f"\nCohort Retention:")
        print(f"  Average Week 0: {avg_retention:.2%}")
        print(f"  Average Week 12: {week_12_avg:.2%}")

        print("=" * 60)

    def get_table_mapping(self) -> Dict[str, str]:
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
