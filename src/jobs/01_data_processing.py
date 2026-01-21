#!/usr/bin/env python3
"""
Job 1: Data Processing

Reads raw interaction and metadata data, applies optimized joins with skew handling,
and writes enriched data to Parquet for downstream analytics.

Usage:
    python src/jobs/01_data_processing.py \
        --interactions-path data/raw/interactions.parquet \
        --metadata-path data/raw/metadata.parquet \
        --output-path data/processed/enriched_interactions.parquet
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.spark_config import create_spark_session, configure_job_specific_settings
from src.transforms.join.execution import optimized_join, identify_hot_keys
from src.utils.data_quality import validate_schema, detect_nulls
from src.utils.monitoring import create_monitoring_context, log_monitoring_summary


def read_interactions(spark, path: str) -> DataFrame:
    """Read interactions data from Parquet."""
    print(f"📖 Reading interactions from: {path}")
    df = spark.read.parquet(path)
    print(f"   ✅ Loaded {df.count():,} interactions")
    return df


def read_metadata(spark, path: str) -> DataFrame:
    """Read user metadata from Parquet."""
    print(f"📖 Reading metadata from: {path}")
    df = spark.read.parquet(path)
    print(f"   ✅ Loaded {df.count():,} user records")
    return df


def validate_input_data(interactions_df: DataFrame, metadata_df: DataFrame) -> None:
    """Validate input data quality."""
    print("\n🔍 Validating input data quality...")

    # Check for required columns in interactions
    required_interactions_cols = ["user_id", "timestamp", "action_type", "page_id", "duration_ms", "app_version"]
    missing_cols = [col for col in required_interactions_cols if col not in interactions_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in interactions: {missing_cols}")

    # Check for required columns in metadata
    required_metadata_cols = ["user_id", "join_date", "country", "device_type", "subscription_type"]
    missing_cols = [col for col in required_metadata_cols if col not in metadata_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in metadata: {missing_cols}")

    # Detect nulls in critical columns
    null_cols_interactions = detect_nulls(interactions_df, ["user_id", "timestamp"])
    if null_cols_interactions:
        print(f"   ⚠️  Warning: NULL values found in interactions: {null_cols_interactions}")

    null_cols_metadata = detect_nulls(metadata_df, ["user_id"])
    if null_cols_metadata:
        print(f"   ⚠️  Warning: NULL values found in metadata: {null_cols_metadata}")

    print("   ✅ Data validation complete")


def enrich_interactions(interactions_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    """
    Join interactions with user metadata using optimized join strategy.

    Applies:
    - Hot key detection
    - Salting for skewed joins
    - Broadcast optimization for small tables
    """
    print("\n🔗 Enriching interactions with user metadata...")

    # Check for data skew in user_id
    print("   🔍 Analyzing data skew...")
    hot_keys_df = identify_hot_keys(
        interactions_df,
        key_column="user_id",
        threshold_percentile=0.99
    )

    hot_key_count = hot_keys_df.count()
    if hot_key_count > 0:
        print(f"   ⚠️  Found {hot_key_count} hot keys (top 1% users)")
        print("   🧂 Applying salting strategy...")
    else:
        print("   ✅ No significant data skew detected")

    # Perform optimized join
    enriched_df = optimized_join(
        large_df=interactions_df,
        small_df=metadata_df,
        join_key="user_id",
        join_type="left",
        broadcast_threshold_mb=100
    )

    # Add processing metadata
    enriched_df = enriched_df.withColumn(
        "processing_timestamp",
        F.lit(datetime.now())
    )

    print(f"   ✅ Enriched {enriched_df.count():,} interactions")

    return enriched_df


def write_enriched_data(enriched_df: DataFrame, output_path: str) -> None:
    """Write enriched data to Parquet with partitioning."""
    print(f"\n💾 Writing enriched data to: {output_path}")

    # Add date partition column
    enriched_df = enriched_df.withColumn(
        "date",
        F.to_date("timestamp")
    )

    # Write with date partitioning and Snappy compression
    enriched_df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(output_path)

    print(f"   ✅ Data written successfully")


def print_data_summary(enriched_df: DataFrame) -> None:
    """Print summary statistics of enriched data."""
    print("\n📊 Enriched Data Summary:")
    print("=" * 60)

    # Date range
    date_stats = enriched_df.agg(
        F.min("timestamp").alias("min_date"),
        F.max("timestamp").alias("max_date")
    ).collect()[0]

    print(f"Date Range: {date_stats['min_date']} to {date_stats['max_date']}")

    # Record counts
    total_records = enriched_df.count()
    total_users = enriched_df.select("user_id").distinct().count()

    print(f"Total Interactions: {total_records:,}")
    print(f"Unique Users: {total_users:,}")

    # Action type distribution
    print("\nAction Type Distribution:")
    action_counts = enriched_df.groupBy("action_type").count().orderBy(F.desc("count"))
    for row in action_counts.collect()[:10]:
        print(f"  {row['action_type']}: {row['count']:,}")

    # Device type distribution
    print("\nDevice Type Distribution:")
    device_counts = enriched_df.groupBy("device_type").count().orderBy(F.desc("count"))
    for row in device_counts.collect():
        print(f"  {row['device_type']}: {row['count']:,}")

    print("=" * 60)


def main():
    """Main job execution."""
    parser = argparse.ArgumentParser(description="GoodNote Data Processing Job")
    parser.add_argument("--interactions-path", required=True, help="Path to interactions Parquet")
    parser.add_argument("--metadata-path", required=True, help="Path to metadata Parquet")
    parser.add_argument("--output-path", required=True, help="Path to write enriched data")
    parser.add_argument("--skip-validation", action="store_true", help="Skip data validation")

    args = parser.parse_args()

    print("=" * 60)
    print("🚀 GoodNote Data Processing Job")
    print("=" * 60)
    print(f"Start Time: {datetime.now()}")
    print(f"Interactions Path: {args.interactions_path}")
    print(f"Metadata Path: {args.metadata_path}")
    print(f"Output Path: {args.output_path}")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session(app_name="GoodNote - Data Processing")
    configure_job_specific_settings(spark, job_type="etl")

    # Create monitoring context
    monitoring_ctx = create_monitoring_context(spark.sparkContext, "data_processing")

    try:
        # Read input data
        interactions_df = read_interactions(spark, args.interactions_path)
        metadata_df = read_metadata(spark, args.metadata_path)

        # Track initial record count
        interaction_count = interactions_df.count()
        monitoring_ctx["record_counter"].add(interaction_count)

        # Validate data quality
        if not args.skip_validation:
            validate_input_data(interactions_df, metadata_df)

        # Enrich interactions with metadata
        enriched_df = enrich_interactions(interactions_df, metadata_df)

        # Print summary
        print_data_summary(enriched_df)

        # Write output
        write_enriched_data(enriched_df, args.output_path)

        # Log monitoring summary
        print("\n")
        log_monitoring_summary(monitoring_ctx, "Data Processing Job")

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
