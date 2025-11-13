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

Usage:
    python src/jobs/02_user_engagement.py \
        --enriched-path data/processed/enriched_interactions.parquet \
        --write-to-db
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import DataFrame

from src.config.spark_config import create_spark_session, configure_job_specific_settings
from src.config.database_config import write_to_postgres
from src.transforms.engagement_transforms import (
    calculate_dau,
    calculate_mau,
    calculate_stickiness,
    identify_power_users,
    calculate_cohort_retention
)


def read_enriched_data(spark, path: str) -> DataFrame:
    """Read enriched interactions from Parquet."""
    print(f"📖 Reading enriched data from: {path}")
    df = spark.read.parquet(path)
    print(f"   ✅ Loaded {df.count():,} enriched interactions")
    return df


def compute_engagement_metrics(enriched_df: DataFrame) -> dict:
    """
    Compute all engagement metrics.

    Returns:
        Dictionary with DataFrames for each metric
    """
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
    power_users_df = identify_power_users(
        enriched_df,
        enriched_df.select("user_id", "join_date", "country", "device_type", "subscription_type").distinct(),
        percentile=0.99
    )
    power_user_count = power_users_df.count()
    print(f"   ✅ Identified {power_user_count} power users (top 1%)")
    metrics["power_users"] = power_users_df

    # 5. Cohort Retention (Weekly cohorts, 6 months)
    print("\n📊 Calculating Cohort Retention...")
    cohort_df = calculate_cohort_retention(
        enriched_df,
        enriched_df.select("user_id", "join_date", "country", "device_type", "subscription_type").distinct(),
        cohort_period="week",
        retention_weeks=26
    )
    cohort_count = cohort_df.count()
    print(f"   ✅ Computed retention for {cohort_count} cohort-week combinations")
    metrics["cohort_retention"] = cohort_df

    return metrics


def write_metrics_to_db(metrics: dict) -> None:
    """Write all metrics to PostgreSQL."""
    print("\n💾 Writing metrics to PostgreSQL...")

    # Table mapping
    table_mapping = {
        "dau": "daily_active_users",
        "mau": "monthly_active_users",
        "stickiness": "user_stickiness",
        "power_users": "power_users",
        "cohort_retention": "cohort_retention"
    }

    for metric_name, df in metrics.items():
        table_name = table_mapping[metric_name]
        print(f"\n   📝 Writing {metric_name} to {table_name}...")

        try:
            write_to_postgres(
                df=df,
                table_name=table_name,
                mode="overwrite",  # Replace existing data
                batch_size=10000,
                num_partitions=4
            )
            print(f"   ✅ Successfully wrote {df.count():,} rows")

        except Exception as e:
            print(f"   ❌ Failed to write {metric_name}: {str(e)}")
            raise


def print_metrics_summary(metrics: dict) -> None:
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
    stickiness_stats = stickiness_df.agg({"stickiness": "avg", "stickiness": "max", "stickiness": "min"}).collect()[0]
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


def main():
    """Main job execution."""
    parser = argparse.ArgumentParser(description="GoodNote User Engagement Analytics")
    parser.add_argument("--enriched-path", required=True, help="Path to enriched interactions Parquet")
    parser.add_argument("--write-to-db", action="store_true", help="Write results to PostgreSQL")
    parser.add_argument("--output-path", help="Optional: Write results to Parquet")

    args = parser.parse_args()

    print("=" * 60)
    print("🚀 GoodNote User Engagement Analytics")
    print("=" * 60)
    print(f"Start Time: {datetime.now()}")
    print(f"Enriched Data Path: {args.enriched_path}")
    print(f"Write to Database: {args.write_to_db}")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session(app_name="GoodNote - User Engagement")
    configure_job_specific_settings(spark, job_type="analytics")

    try:
        # Read enriched data
        enriched_df = read_enriched_data(spark, args.enriched_path)

        # Compute all engagement metrics
        metrics = compute_engagement_metrics(enriched_df)

        # Print summary
        print_metrics_summary(metrics)

        # Write to PostgreSQL if requested
        if args.write_to_db:
            write_metrics_to_db(metrics)

        # Optionally write to Parquet
        if args.output_path:
            print(f"\n💾 Writing metrics to Parquet: {args.output_path}")
            for metric_name, df in metrics.items():
                output_file = f"{args.output_path}/{metric_name}"
                df.write.mode("overwrite").parquet(output_file)
                print(f"   ✅ Wrote {metric_name}")

        print("\n" + "=" * 60)
        print("✅ Job completed successfully!")
        print(f"End Time: {datetime.now()}")
        print("=" * 60)

        sys.exit(0)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Job failed with error: {str(e)}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
