#!/usr/bin/env python3
"""
Job 1: Data Processing

Reads raw interaction and metadata data, applies optimized joins with skew handling,
and writes enriched data to Parquet for downstream analytics.

Usage (via helper script):
    ./scripts/run_spark_job.sh src/jobs/01_data_processing.py \
        --interactions-path /app/data/raw/user_interactions.csv \
        --metadata-path /app/data/raw/user_metadata.csv \
        --output-path /app/data/processed/enriched_interactions.parquet

Usage (direct spark-submit):
    docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "$(SPARK_MASTER_URL)" \
        /opt/spark-apps/src/jobs/01_data_processing.py \
        --interactions-path /app/data/raw/user_interactions.csv \
        --metadata-path /app/data/raw/user_metadata.csv \
        --output-path /app/data/processed/enriched_interactions.parquet'
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import HOT_KEY_THRESHOLD_PERCENTILE
from src.jobs.base_job import BaseAnalyticsJob
from src.schemas import INTERACTIONS_SCHEMA, METADATA_SCHEMA
from src.schemas.columns import COL_ACTION_TYPE, COL_DEVICE_TYPE, COL_TIMESTAMP, COL_USER_ID
from src.transforms.join.execution import identify_hot_keys, optimized_join


class DataProcessingJob(BaseAnalyticsJob):
    """Data Processing ETL Job."""

    def __init__(self):
        super().__init__(
            job_name="Data Processing",
            job_type="etl",
            data_size_gb=1,  # Estimated data size for dynamic partitioning
        )

    def get_argument_parser(self) -> argparse.ArgumentParser:
        """Configure job-specific arguments."""
        parser = argparse.ArgumentParser(description="GoodNote Data Processing Job")
        parser.add_argument(
            "--interactions-path", required=True, help="Path to interactions Parquet"
        )
        parser.add_argument("--metadata-path", required=True, help="Path to metadata Parquet")
        parser.add_argument("--output-path", required=True, help="Path to write enriched data")
        parser.add_argument("--skip-validation", action="store_true", help="Skip data validation")
        return parser

    def enrich_interactions(self, interactions_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
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
            key_column=COL_USER_ID,
            threshold_percentile=HOT_KEY_THRESHOLD_PERCENTILE,
        ).persist()

        hot_key_count = hot_keys_df.count()
        if hot_key_count > 0:
            print(f"   ⚠️  Found {hot_key_count} hot keys (top 1% users)")
            print("   🧂 Applying salting strategy...")
        else:
            print("   ✅ No significant data skew detected")

        # Perform optimized join with hot keys for salting
        enriched_df = optimized_join(
            large_df=interactions_df,
            small_df=metadata_df,
            join_key=COL_USER_ID,
            join_type="left",
            hot_keys_df=hot_keys_df,
        )

        # Add processing metadata (use Spark's current_timestamp for consistency across cluster)
        enriched_df = enriched_df.withColumn("processing_timestamp", F.current_timestamp())

        hot_keys_df.unpersist()
        return enriched_df

    def compute_metrics(self) -> dict[str, DataFrame]:
        """
        Read and enrich interaction data.

        Returns:
            Dictionary with enriched DataFrame
        """
        # Read input data from generated CSV files
        interactions_df = self.read_csv(
            self.args.interactions_path,
            name="interactions",
            schema=INTERACTIONS_SCHEMA,
            num_partitions=1,
        )
        metadata_df = self.read_csv(
            self.args.metadata_path, name="user metadata", schema=METADATA_SCHEMA, num_partitions=1
        )

        # Track initial record count
        interaction_count = interactions_df.count()
        if self.monitoring_ctx:
            self.monitoring_ctx["record_counter"].add(interaction_count)

        # Validate data quality
        if not self.args.skip_validation:
            print("\n🔍 Validating input data quality...")

            warnings = []
            warnings.extend(
                self.validate_dataframe(
                    interactions_df, name="interactions", schema=INTERACTIONS_SCHEMA
                )
            )
            warnings.extend(
                self.validate_dataframe(metadata_df, name="metadata", schema=METADATA_SCHEMA)
            )

            for warning in warnings:
                print(f"   ⚠️  {warning}")

            print("   ✅ Data validation complete")

        # Enrich interactions with metadata
        enriched_df = self.enrich_interactions(interactions_df, metadata_df)

        # Unpersist input dataframes to free memory
        interactions_df.unpersist()
        metadata_df.unpersist()
        print("   🧹 Released input DataFrames from memory")

        # Add date partition column for downstream partitioned writes
        enriched_df = enriched_df.withColumn("date", F.to_date(COL_TIMESTAMP))

        return {"enriched_interactions": enriched_df}

    def print_summary(self, metrics: dict[str, DataFrame]) -> None:
        """Print summary statistics of enriched data."""
        enriched_df = metrics["enriched_interactions"].persist()

        print("\n📊 Enriched Data Summary:")
        print("=" * 60)

        # Combine all aggregations into a single pass to avoid multiple Spark jobs
        summary_stats = enriched_df.agg(
            F.min(COL_TIMESTAMP).alias("min_date"),
            F.max(COL_TIMESTAMP).alias("max_date"),
            F.count("*").alias("total_records"),
            F.countDistinct(COL_USER_ID).alias("unique_users"),
        ).collect()[0]

        print(f"Date Range: {summary_stats['min_date']} to {summary_stats['max_date']}")
        print(f"Total Interactions: {summary_stats['total_records']:,}")
        print(f"Unique Users: {summary_stats['unique_users']:,}")

        # Action type distribution
        print("\nAction Type Distribution:")
        action_counts = enriched_df.groupBy(COL_ACTION_TYPE).count().orderBy(F.desc("count"))
        for row in action_counts.collect()[:10]:
            print(f"  {row[COL_ACTION_TYPE]}: {row['count']:,}")

        # Device type distribution
        print("\nDevice Type Distribution:")
        device_counts = enriched_df.groupBy(COL_DEVICE_TYPE).count().orderBy(F.desc("count"))
        for row in device_counts.collect():
            print(f"  {row[COL_DEVICE_TYPE]}: {row['count']:,}")

        enriched_df.unpersist()
        print("=" * 60)

    def get_table_mapping(self) -> dict[str, str] | None:
        """This job writes to Parquet, not database."""
        return None


def main():
    """Main job execution."""
    job = DataProcessingJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
