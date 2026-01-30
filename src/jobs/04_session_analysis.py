#!/usr/bin/env python3
"""
Job 4: Session Analysis

Calculates session metrics using Spark's native session_window():
- Session metrics (duration, actions count, bounce flag) with 30-minute timeout
- Bounce rates (overall and by dimensions)

Uses session_window() for single-pass sessionization and aggregation.
Writes results to PostgreSQL for dashboard consumption.

Usage (via helper script):
    ./scripts/run_spark_job.sh src/jobs/04_session_analysis.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db

Usage (direct spark-submit):
    docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/04_session_analysis.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db'
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import (
    SESSION_TIMEOUT_SECONDS,
    TABLE_BOUNCE_RATES,
    TABLE_SESSION_FREQUENCY,
    TABLE_SESSION_METRICS,
)
from src.jobs.base_job import BaseAnalyticsJob
from src.schemas.columns import (
    COL_ACTION_COUNT,
    COL_COUNTRY,
    COL_DEVICE_TYPE,
    COL_DURATION_MS,
    COL_IS_BOUNCE,
    COL_METRIC_DATE,
    COL_SESSION_DURATION_MS,
    COL_TIMESTAMP,
    COL_USER_ID,
)
from src.transforms.session import (
    calculate_bounce_rate,
    calculate_session_frequency,
    calculate_session_metrics,
)


class SessionAnalysisJob(BaseAnalyticsJob):
    """Session Analysis Job."""

    def __init__(self):
        super().__init__(job_name="Session Analysis", job_type="analytics")
        # Cache counts to avoid redundant computations
        self._session_count: int = 0
        self._bounce_count: int = 0

    def get_argument_parser(self) -> argparse.ArgumentParser:
        """Configure job-specific arguments."""
        parser = argparse.ArgumentParser(description="GoodNote Session Analysis")
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
        Perform complete session analysis.

        Returns:
            Dictionary with DataFrames for session metrics and bounce rates
        """
        # Read enriched data
        enriched_df = self.read_parquet(self.args.enriched_path, "enriched interactions")

        # Validate required columns
        required_columns = [
            COL_USER_ID,
            COL_TIMESTAMP,
            COL_DURATION_MS,
            COL_DEVICE_TYPE,
            COL_COUNTRY,
        ]
        self.validate_dataframe(enriched_df, required_columns=required_columns, name="enriched_df")

        results = {}

        # 1. Calculate Session Metrics (uses session_window() internally)
        print("\n📊 Calculating session metrics...")
        print("   ⏱️  Session timeout: 30 minutes")

        # Pass timeout as integer - the function handles conversion internally
        session_metrics_df = calculate_session_metrics(
            enriched_df, session_timeout=SESSION_TIMEOUT_SECONDS
        )

        # Add metric date for time-series analysis
        session_metrics_df = session_metrics_df.withColumn(
            COL_METRIC_DATE, F.to_date("session_start_time")
        )

        # Persist session_metrics_df as it's used multiple times
        session_metrics_df.persist()

        self._session_count = session_metrics_df.count()
        print(f"   ✅ Created {self._session_count:,} sessions")

        self._bounce_count = session_metrics_df.filter(F.col(COL_IS_BOUNCE)).count()
        bounce_percentage = (
            (self._bounce_count / self._session_count) * 100 if self._session_count > 0 else 0
        )

        print(f"   📊 Bounce sessions: {self._bounce_count:,} ({bounce_percentage:.1f}%)")

        results["session_metrics"] = session_metrics_df

        # 2. Calculate Session Frequency
        print("\n📊 Calculating session frequency...")
        session_freq_df = calculate_session_frequency(
            session_metrics_df, group_by_columns=[COL_DEVICE_TYPE, COL_COUNTRY]
        )
        unique_users = session_freq_df.count()
        print(f"   ✅ Calculated frequency metrics for {unique_users:,} users")

        # Calculate summary stats for session frequency
        freq_stats = session_freq_df.agg(
            F.avg("total_sessions").alias("avg_sessions_per_user"),
            F.avg("avg_sessions_per_day").alias("avg_daily_freq"),
            (
                F.sum(F.when(F.col("total_sessions") >= 2, 1).otherwise(0)) / F.count("*") * 100
            ).alias("return_user_rate"),
        ).collect()[0]

        if freq_stats["avg_sessions_per_user"] is not None:
            print(f"   📊 Avg sessions per user: {freq_stats['avg_sessions_per_user']:.2f}")
        if freq_stats["return_user_rate"] is not None:
            print(f"   📊 Return user rate: {freq_stats['return_user_rate']:.1f}%")

        results["session_frequency"] = session_freq_df

        # 3. Calculate Bounce Rates
        print("\n📊 Calculating bounce rates...")

        # Overall bounce rate
        overall_bounce_df = calculate_bounce_rate(session_metrics_df)
        overall_bounce_df = (
            overall_bounce_df.withColumn(COL_METRIC_DATE, F.current_date())
            .withColumn("dimension_name", F.lit(None).cast("string"))
            .withColumn("dimension_value", F.lit(None).cast("string"))
        )

        overall_bounce_row = overall_bounce_df.collect()
        if overall_bounce_row and overall_bounce_row[0]["bounce_rate"] is not None:
            print(f"   ✅ Overall bounce rate: {overall_bounce_row[0]['bounce_rate']:.2f}%")
        else:
            print("   ✅ Overall bounce rate: N/A (no data)")

        # By Device Type
        device_bounce_df = calculate_bounce_rate(
            session_metrics_df, group_by_columns=[COL_DEVICE_TYPE, COL_METRIC_DATE]
        )
        device_bounce_df = device_bounce_df.withColumn(
            "dimension_name", F.lit(COL_DEVICE_TYPE)
        ).withColumnRenamed(COL_DEVICE_TYPE, "dimension_value")

        print("   ✅ Bounce rates by device type:")
        for row in device_bounce_df.select("dimension_value", "bounce_rate").distinct().collect():
            print(f"      {row['dimension_value']}: {row['bounce_rate']:.2f}%")

        # By Country
        country_bounce_df = calculate_bounce_rate(
            session_metrics_df, group_by_columns=[COL_COUNTRY, COL_METRIC_DATE]
        )
        country_bounce_df = country_bounce_df.withColumn(
            "dimension_name", F.lit(COL_COUNTRY)
        ).withColumnRenamed(COL_COUNTRY, "dimension_value")

        # Combine all bounce rates
        bounce_rates_df = overall_bounce_df.unionByName(device_bounce_df).unionByName(
            country_bounce_df
        )

        bounce_rate_count = bounce_rates_df.count()
        print(f"   ✅ Calculated {bounce_rate_count} bounce rate metrics")

        results["bounce_rates"] = bounce_rates_df

        # Unpersist DataFrames to free memory
        session_metrics_df.unpersist()
        enriched_df.unpersist()

        return results

    def print_summary(self, metrics: dict[str, DataFrame]) -> None:
        """Print summary of session analysis."""
        print("\n" + "=" * 60)
        print("📊 Session Analysis Summary")
        print("=" * 60)

        # Session Metrics Summary
        session_df = metrics["session_metrics"]
        # Use cached count instead of recomputing
        total_sessions = self._session_count

        session_stats = session_df.agg(
            F.avg(COL_SESSION_DURATION_MS).alias("avg_duration"),
            F.max(COL_SESSION_DURATION_MS).alias("max_duration"),
            F.avg(COL_ACTION_COUNT).alias("avg_actions"),
            F.sum(F.col(COL_IS_BOUNCE).cast("int")).alias("bounce_count"),
        ).collect()[0]

        # Extract with null safety
        avg_duration = session_stats["avg_duration"] or 0
        max_duration = session_stats["max_duration"] or 0
        avg_actions = session_stats["avg_actions"] or 0
        bounce_count = session_stats["bounce_count"] or 0

        print("\nSession Metrics:")
        print(f"  Total Sessions: {total_sessions:,}")
        print(f"  Average Duration: {avg_duration / 1000:.1f} seconds")
        print(f"  Max Duration: {max_duration / 1000:.1f} seconds")
        print(f"  Average Actions per Session: {avg_actions:.1f}")
        print(f"  Bounce Sessions: {int(bounce_count):,}")
        if total_sessions > 0:
            print(f"  Bounce Rate: {(bounce_count / total_sessions) * 100:.1f}%")
        else:
            print("  Bounce Rate: N/A (no sessions)")

        # Duration distribution
        print("\n  Session Duration Distribution:")
        duration_buckets = (
            session_df.select(
                F.when(F.col(COL_SESSION_DURATION_MS) < 60000, "< 1 min")
                .when(F.col(COL_SESSION_DURATION_MS) < 300000, "1-5 min")
                .when(F.col(COL_SESSION_DURATION_MS) < 900000, "5-15 min")
                .when(F.col(COL_SESSION_DURATION_MS) < 1800000, "15-30 min")
                .otherwise("> 30 min")
                .alias("duration_bucket")
            )
            .groupBy("duration_bucket")
            .count()
            .orderBy("count", ascending=False)
        )

        for row in duration_buckets.collect():
            pct = (row["count"] / total_sessions) * 100 if total_sessions > 0 else 0
            print(f"    {row['duration_bucket']}: {row['count']:,} ({pct:.1f}%)")

        # Bounce Rates Summary
        bounce_df = metrics["bounce_rates"]
        print("\nBounce Rates:")

        # Overall
        overall_bounce = bounce_df.filter(F.col("dimension_name").isNull()).collect()
        if overall_bounce:
            print(f"  Overall: {overall_bounce[0]['bounce_rate']:.2f}%")

        # By Device
        device_bounces = (
            bounce_df.filter(F.col("dimension_name") == COL_DEVICE_TYPE)
            .groupBy("dimension_value")
            .agg(F.avg("bounce_rate").alias("avg_bounce"))
            .orderBy(F.desc("avg_bounce"))
        )

        print("  By Device Type:")
        for row in device_bounces.collect():
            print(f"    {row['dimension_value']}: {row['avg_bounce']:.2f}%")

        # Top countries with high bounce rates
        country_bounces = (
            bounce_df.filter(F.col("dimension_name") == COL_COUNTRY)
            .groupBy("dimension_value")
            .agg(F.avg("bounce_rate").alias("avg_bounce"))
            .orderBy(F.desc("avg_bounce"))
            .limit(5)
        )

        print("  Top 5 Countries by Bounce Rate:")
        for row in country_bounces.collect():
            print(f"    {row['dimension_value']}: {row['avg_bounce']:.2f}%")

        # Session Frequency Summary
        if "session_frequency" in metrics:
            freq_df = metrics["session_frequency"]
            print("\nSession Frequency:")

            freq_stats = freq_df.agg(
                F.avg("total_sessions").alias("avg_sessions"),
                F.max("total_sessions").alias("max_sessions"),
                F.avg("avg_sessions_per_day").alias("avg_daily_freq"),
                (
                    F.sum(F.when(F.col("total_sessions") >= 2, 1).otherwise(0)) / F.count("*") * 100
                ).alias("return_rate"),
            ).collect()[0]

            avg_sessions = freq_stats["avg_sessions"] or 0
            max_sessions = freq_stats["max_sessions"] or 0
            avg_daily = freq_stats["avg_daily_freq"] or 0
            return_rate = freq_stats["return_rate"] or 0

            print(f"  Avg Sessions per User: {avg_sessions:.2f}")
            print(f"  Max Sessions per User: {int(max_sessions):,}")
            print(f"  Avg Daily Session Frequency: {avg_daily:.2f}")
            print(f"  Return User Rate (2+ sessions): {return_rate:.1f}%")

            # Session frequency by device
            device_freq = (
                freq_df.groupBy(COL_DEVICE_TYPE)
                .agg(F.avg("total_sessions").alias("avg_sessions"))
                .orderBy(F.desc("avg_sessions"))
            )

            print("  Avg Sessions by Device Type:")
            for row in device_freq.collect():
                print(f"    {row[COL_DEVICE_TYPE]}: {row['avg_sessions']:.2f}")

        print("=" * 60)

    def get_table_mapping(self) -> dict[str, str]:
        """Get database table mapping."""
        return {
            "session_metrics": TABLE_SESSION_METRICS,
            "session_frequency": TABLE_SESSION_FREQUENCY,
            "bounce_rates": TABLE_BOUNCE_RATES,
        }


def main():
    """Main job execution."""
    job = SessionAnalysisJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
