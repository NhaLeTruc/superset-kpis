#!/usr/bin/env python3
"""
Job 4: Session Analysis

Calculates session metrics using Spark's native session_window():
- Session metrics (duration, actions count, bounce flag) with 10-minute timeout
- Bounce rates (overall and by dimensions)

Uses session_window() for single-pass sessionization and aggregation.
Writes results to PostgreSQL for dashboard consumption.

Usage (via helper script):
    ./scripts/run_spark_job.sh src/jobs/04_session_analysis.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --dev-mode \
        --write-to-db

Usage (direct spark-submit):
    docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "$(SPARK_MASTER_URL)" \
        /opt/spark-apps/src/jobs/04_session_analysis.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --dev-mode \
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
    COL_ACTIONS_COUNT,
    COL_AVG_ACTION_DURATION_MS,
    COL_AVG_SESSIONS_PER_DAY,
    COL_BOUNCE_RATE,
    COL_COUNTRY,
    COL_DEVICE_TYPE,
    COL_DIMENSION_NAME,
    COL_DIMENSION_VALUE,
    COL_DURATION_MS,
    COL_IS_BOUNCE,
    COL_METRIC_DATE,
    COL_SESSION_DURATION_MS,
    COL_SESSION_END_TIME,
    COL_SESSION_ID,
    COL_SESSION_START_TIME,
    COL_TIMESTAMP,
    COL_TOTAL_SESSIONS,
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
        parser.add_argument(
            "--dev-mode", action="store_true", help="Enable development mode with summary stats"
        )
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
        print(f"   ⏱️  Session timeout: {SESSION_TIMEOUT_SECONDS // 60} minutes")

        # Pass timeout as integer - the function handles conversion internally
        session_metrics_df = calculate_session_metrics(
            enriched_df, session_timeout=SESSION_TIMEOUT_SECONDS
        )

        # Add metric date for time-series analysis
        session_metrics_df = session_metrics_df.withColumn(
            COL_METRIC_DATE, F.to_date(COL_SESSION_START_TIME)
        )

        # Persist session_metrics_df as it's used multiple times
        session_metrics_df = session_metrics_df.persist()

        self._session_count = session_metrics_df.count()
        print(f"   ✅ Created {self._session_count:,} sessions")

        self._bounce_count = session_metrics_df.filter(F.col(COL_IS_BOUNCE)).count()
        bounce_percentage = (
            (self._bounce_count / self._session_count) * 100 if self._session_count > 0 else 0
        )

        print(f"   📊 Bounce sessions: {self._bounce_count:,} ({bounce_percentage:.1f}%)")

        # Select only the columns defined in the DB schema.
        # Renames action_count → actions_count (DB schema column name) and casts
        # is_bounce from Boolean to INTEGER (schema: INTEGER NOT NULL, 0 or 1).
        # Drops compute artifacts (total_action_duration_ms, last_action_duration_ms,
        # session_duration_seconds) and raw enriched_df passthrough columns.
        results["session_metrics"] = session_metrics_df.select(
            COL_SESSION_ID,
            COL_USER_ID,
            COL_SESSION_START_TIME,
            COL_SESSION_END_TIME,
            COL_SESSION_DURATION_MS,
            F.col(COL_ACTION_COUNT).cast("int").alias(COL_ACTIONS_COUNT),
            COL_AVG_ACTION_DURATION_MS,
            F.col(COL_IS_BOUNCE).cast("int").alias(COL_IS_BOUNCE),
            COL_DEVICE_TYPE,
            COL_COUNTRY,
        )

        # 2. Calculate Session Frequency
        print("\n📊 Calculating session frequency...")
        session_freq_df = calculate_session_frequency(
            session_metrics_df, group_by_columns=[COL_DEVICE_TYPE, COL_COUNTRY]
        )
        unique_users = session_freq_df.count()
        print(f"   ✅ Calculated frequency metrics for {unique_users:,} users")

        # Calculate summary stats for session frequency
        freq_stats = session_freq_df.agg(
            F.avg(COL_TOTAL_SESSIONS).alias("avg_sessions_per_user"),
            F.avg(COL_AVG_SESSIONS_PER_DAY).alias("avg_daily_freq"),
            (
                F.sum(F.when(F.col(COL_TOTAL_SESSIONS) >= 2, 1).otherwise(0)) / F.count("*") * 100
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
            .withColumn(COL_DIMENSION_NAME, F.lit(None).cast("string"))
            .withColumn(COL_DIMENSION_VALUE, F.lit(None).cast("string"))
        )

        overall_bounce_row = overall_bounce_df.collect()
        if overall_bounce_row and overall_bounce_row[0][COL_BOUNCE_RATE] is not None:
            print(f"   ✅ Overall bounce rate: {overall_bounce_row[0][COL_BOUNCE_RATE]:.2f}%")
        else:
            print("   ✅ Overall bounce rate: N/A (no data)")

        # By Device Type
        device_bounce_df = calculate_bounce_rate(
            session_metrics_df, group_by_columns=[COL_DEVICE_TYPE, COL_METRIC_DATE]
        )
        device_bounce_df = device_bounce_df.withColumn(
            COL_DIMENSION_NAME, F.lit(COL_DEVICE_TYPE)
        ).withColumnRenamed(COL_DEVICE_TYPE, COL_DIMENSION_VALUE)

        print("   ✅ Bounce rates by device type:")
        # device_bounce_df has one row per (device_type, metric_date), so average across
        # dates to get a single per-device summary.  .distinct() on (device, rate) would
        # return multiple rows per device when rates differ across dates.
        for row in (
            device_bounce_df.groupBy(COL_DIMENSION_VALUE)
            .agg(F.avg(COL_BOUNCE_RATE).alias(COL_BOUNCE_RATE))
            .collect()
        ):
            print(f"      {row[COL_DIMENSION_VALUE]}: {row[COL_BOUNCE_RATE]:.2f}%")

        # By Country
        country_bounce_df = calculate_bounce_rate(
            session_metrics_df, group_by_columns=[COL_COUNTRY, COL_METRIC_DATE]
        )
        country_bounce_df = country_bounce_df.withColumn(
            COL_DIMENSION_NAME, F.lit(COL_COUNTRY)
        ).withColumnRenamed(COL_COUNTRY, COL_DIMENSION_VALUE)

        # Combine all bounce rates
        bounce_rates_df = overall_bounce_df.unionByName(device_bounce_df).unionByName(
            country_bounce_df
        )

        bounce_rate_count = bounce_rates_df.count()
        print(f"   ✅ Calculated {bounce_rate_count} bounce rate metrics")

        results["bounce_rates"] = bounce_rates_df

        # session_metrics_df and enriched_df are NOT unpersisted here.
        # session_freq_df and bounce_rates_df are lazy DataFrames that reference
        # session_metrics_df in their execution plans.  Unpersisting session_metrics_df
        # before those DataFrames are materialized (during write_to_database in run())
        # would force Spark to redo the full session_window() computation — one of
        # the most expensive Spark operations.  Similarly, unpersisting enriched_df
        # would force a Parquet re-read.  Both are released when spark.stop() is
        # called in base_job.run().

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
            F.avg(COL_ACTIONS_COUNT).alias("avg_actions"),
            F.sum(F.col(COL_IS_BOUNCE).cast("int")).alias("bounce_count"),
        ).collect()[0]

        # Extract with null safety
        avg_duration = (
            session_stats["avg_duration"] if session_stats["avg_duration"] is not None else 0
        )
        max_duration = (
            session_stats["max_duration"] if session_stats["max_duration"] is not None else 0
        )
        avg_actions = (
            session_stats["avg_actions"] if session_stats["avg_actions"] is not None else 0
        )
        bounce_count = (
            session_stats["bounce_count"] if session_stats["bounce_count"] is not None else 0
        )

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
        overall_bounce = bounce_df.filter(F.col(COL_DIMENSION_NAME).isNull()).collect()
        if overall_bounce:
            print(f"  Overall: {overall_bounce[0][COL_BOUNCE_RATE]:.2f}%")

        # By Device
        device_bounces = (
            bounce_df.filter(F.col(COL_DIMENSION_NAME) == COL_DEVICE_TYPE)
            .groupBy(COL_DIMENSION_VALUE)
            .agg(F.avg(COL_BOUNCE_RATE).alias("avg_bounce"))
            .orderBy(F.desc("avg_bounce"))
        )

        print("  By Device Type:")
        for row in device_bounces.collect():
            print(f"    {row[COL_DIMENSION_VALUE]}: {row['avg_bounce']:.2f}%")

        # Top countries with high bounce rates
        country_bounces = (
            bounce_df.filter(F.col(COL_DIMENSION_NAME) == COL_COUNTRY)
            .groupBy(COL_DIMENSION_VALUE)
            .agg(F.avg(COL_BOUNCE_RATE).alias("avg_bounce"))
            .orderBy(F.desc("avg_bounce"))
            .limit(5)
        )

        print("  Top 5 Countries by Bounce Rate:")
        for row in country_bounces.collect():
            print(f"    {row[COL_DIMENSION_VALUE]}: {row['avg_bounce']:.2f}%")

        # Session Frequency Summary
        if "session_frequency" in metrics:
            self._print_frequency_summary(metrics["session_frequency"])

        print("=" * 60)

    def _print_frequency_summary(self, freq_df: DataFrame) -> None:
        """Print session frequency summary."""
        print("\nSession Frequency:")

        freq_stats = freq_df.agg(
            F.avg(COL_TOTAL_SESSIONS).alias("avg_sessions"),
            F.max(COL_TOTAL_SESSIONS).alias("max_sessions"),
            F.avg(COL_AVG_SESSIONS_PER_DAY).alias("avg_daily_freq"),
            (
                F.sum(F.when(F.col(COL_TOTAL_SESSIONS) >= 2, 1).otherwise(0)) / F.count("*") * 100
            ).alias("return_rate"),
        ).collect()[0]

        avg_sessions = freq_stats["avg_sessions"] if freq_stats["avg_sessions"] is not None else 0
        max_sessions = freq_stats["max_sessions"] if freq_stats["max_sessions"] is not None else 0
        avg_daily = freq_stats["avg_daily_freq"] if freq_stats["avg_daily_freq"] is not None else 0
        return_rate = freq_stats["return_rate"] if freq_stats["return_rate"] is not None else 0

        print(f"  Avg Sessions per User: {avg_sessions:.2f}")
        print(f"  Max Sessions per User: {int(max_sessions):,}")
        print(f"  Avg Daily Session Frequency: {avg_daily:.2f}")
        print(f"  Return User Rate (2+ sessions): {return_rate:.1f}%")

        # Session frequency by device
        device_freq = (
            freq_df.groupBy(COL_DEVICE_TYPE)
            .agg(F.avg(COL_TOTAL_SESSIONS).alias("avg_sessions"))
            .orderBy(F.desc("avg_sessions"))
        )

        print("  Avg Sessions by Device Type:")
        for row in device_freq.collect():
            print(f"    {row[COL_DEVICE_TYPE]}: {row['avg_sessions']:.2f}")

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
