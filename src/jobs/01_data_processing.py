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
        --master "local[*]" \
        /opt/spark-apps/src/jobs/01_data_processing.py \
        --interactions-path /app/data/raw/user_interactions.csv \
        --metadata-path /app/data/raw/user_metadata.csv \
        --output-path /app/data/processed/enriched_interactions.parquet'
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import HOT_KEY_THRESHOLD_PERCENTILE
from src.jobs.base_job import BaseAnalyticsJob
from src.schemas import INTERACTIONS_SCHEMA, METADATA_SCHEMA
from src.schemas.columns import COL_ACTION_TYPE, COL_DEVICE_TYPE, COL_TIMESTAMP, COL_USER_ID
from src.transforms.join.execution import identify_hot_keys, optimized_join
from src.utils.monitoring import log_monitoring_summary


class DataProcessingJob(BaseAnalyticsJob):
    """Data Processing ETL Job."""

    def __init__(self):
        super().__init__(
            job_name="Data Processing",
            job_type="etl",
            data_size_gb=0.15,  # Estimated data size for dynamic partitioning
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

        # Add processing metadata
        enriched_df = enriched_df.withColumn("processing_timestamp", F.lit(datetime.now()))

        hot_keys_df.unpersist()
        return enriched_df

    def compute_metrics(self) -> dict[str, DataFrame]:
        """
        Read and enrich interaction data.

        Returns:
            Dictionary with enriched DataFrame
        """
        # Read input data
        interactions_df = self.read_csv(self.args.interactions_path, name="interactions")
        metadata_df = self.read_csv(self.args.metadata_path, name="user metadata")

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

        # Date range
        date_stats = enriched_df.agg(
            F.min(COL_TIMESTAMP).alias("min_date"), F.max(COL_TIMESTAMP).alias("max_date")
        ).collect()[0]

        print(f"Date Range: {date_stats['min_date']} to {date_stats['max_date']}")

        # Record counts
        total_records = enriched_df.count()
        total_users = enriched_df.select(COL_USER_ID).distinct().count()

        print(f"Total Interactions: {total_records:,}")
        print(f"Unique Users: {total_users:,}")

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

    def run(self) -> int:
        """
        Execute data processing pipeline.

        Overrides base class to handle ETL-specific output writing
        (date-partitioned Parquet instead of metrics tables).
        """
        try:
            # Parse arguments
            parser = self.get_argument_parser()
            self.args = parser.parse_args()

            # Print header
            self.print_job_header()

            # Setup Spark
            self.setup_spark()

            # Setup monitoring
            self.setup_monitoring("data_processing")

            # Execute core computation
            metrics = self.compute_metrics()

            # Print summary
            self.print_summary(metrics)

            # Write enriched output with date partitioning
            self.write_to_parquet(metrics, self.args.output_path, partition_by=["date"])

            # Log monitoring summary
            if self.monitoring_ctx:
                print("\n")
                log_monitoring_summary(self.monitoring_ctx, "Data Processing Job")

            # Print footer
            self.print_job_footer(success=True)

            return 0

        except Exception as e:
            print("\n" + "=" * 60)
            print(f"❌ Job failed with error: {e!s}")
            print("=" * 60)
            traceback.print_exc()

            self.print_job_footer(success=False)

            return 1

        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Main job execution."""
    job = DataProcessingJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
