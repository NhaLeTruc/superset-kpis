#!/usr/bin/env python3
"""
Job 3: Performance Metrics

Calculates performance metrics:
- Percentiles (P50, P95, P99) by app version
- Device performance correlation
- Statistical anomaly detection

Writes results to PostgreSQL for monitoring dashboards.

Usage:
    python src/jobs/03_performance_metrics.py \
        --enriched-path data/processed/enriched_interactions.parquet \
        --write-to-db
"""
import argparse
import sys
from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.jobs.base_job import BaseAnalyticsJob
from src.transforms.performance import (
    calculate_percentiles,
    calculate_device_correlation,
    detect_anomalies_statistical
)


class PerformanceMetricsJob(BaseAnalyticsJob):
    """Performance Metrics Analytics Job."""

    def __init__(self):
        super().__init__(
            job_name="Performance Metrics",
            job_type="analytics"
        )

    def get_argument_parser(self) -> argparse.ArgumentParser:
        """Configure job-specific arguments."""
        parser = argparse.ArgumentParser(description="GoodNote Performance Metrics")
        parser.add_argument("--enriched-path", required=True,
                          help="Path to enriched interactions Parquet")
        parser.add_argument("--write-to-db", action="store_true",
                          help="Write results to PostgreSQL")
        parser.add_argument("--output-path",
                          help="Optional: Write results to Parquet")
        return parser

    def compute_metrics(self) -> Dict[str, DataFrame]:
        """
        Compute all performance metrics.

        Returns:
            Dictionary with DataFrames for each metric
        """
        # Read enriched data
        enriched_df = self.read_enriched_data(self.args.enriched_path)

        # Add date column for time-series analysis
        enriched_df = enriched_df.withColumn("metric_date", F.to_date("timestamp"))

        metrics = {}

        # 1. Percentiles by App Version
        print("\n📊 Calculating Performance Percentiles by App Version...")
        version_perf_df = calculate_percentiles(
            df=enriched_df,
            value_column="duration_ms",
            group_by_columns=["app_version", "metric_date"],
            percentiles=[0.5, 0.95, 0.99]
        )

        # Rename percentile columns to match database schema
        version_perf_df = version_perf_df \
            .withColumnRenamed("p50", "p50_duration_ms") \
            .withColumnRenamed("p95", "p95_duration_ms") \
            .withColumnRenamed("p99", "p99_duration_ms")

        version_count = version_perf_df.count()
        print(f"   ✅ Computed percentiles for {version_count} app version-date combinations")
        metrics["performance_by_version"] = version_perf_df

        # 2. Device Performance
        print("\n📊 Calculating Device Performance...")
        device_percentiles = calculate_percentiles(
            df=enriched_df,
            value_column="duration_ms",
            group_by_columns=["device_type", "metric_date"],
            percentiles=[0.5, 0.95, 0.99]
        )

        device_percentiles = device_percentiles \
            .withColumnRenamed("p50", "p50_duration_ms") \
            .withColumnRenamed("p95", "p95_duration_ms") \
            .withColumnRenamed("p99", "p99_duration_ms")

        # Add user count
        device_perf_with_users = enriched_df.groupBy("device_type", "metric_date").agg(
            F.countDistinct("user_id").alias("user_count")
        )

        device_perf_final = device_percentiles.join(
            device_perf_with_users,
            ["device_type", "metric_date"],
            "left"
        )

        device_count = device_perf_final.count()
        print(f"   ✅ Computed device performance for {device_count} device-date combinations")
        metrics["device_performance"] = device_perf_final

        # 3. Statistical Anomaly Detection
        print("\n📊 Detecting Performance Anomalies...")
        anomalies_df = detect_anomalies_statistical(
            df=enriched_df,
            value_column="duration_ms",
            z_threshold=3.0,
            group_by_columns=["app_version", "metric_date"]
        )

        if anomalies_df.count() > 0:
            # Rename duration_ms to metric_value for database schema
            anomalies_df = anomalies_df.withColumnRenamed("duration_ms", "metric_value")

            # Enrich anomalies with severity classification
            anomalies_df = anomalies_df.withColumn(
                "severity",
                F.when(F.abs(F.col("z_score")) >= 4.0, "critical")
                 .when(F.abs(F.col("z_score")) >= 3.5, "high")
                 .when(F.abs(F.col("z_score")) >= 3.0, "medium")
                 .otherwise("low")
            )

            # Add detection timestamp and description
            anomalies_df = anomalies_df \
                .withColumn("detected_at", F.lit(datetime.now())) \
                .withColumn("metric_name", F.lit("duration_ms")) \
                .withColumn(
                    "description",
                    F.concat(
                        F.lit("Performance anomaly detected for "),
                        F.col("app_version"),
                        F.lit(" on "),
                        F.col("metric_date").cast("string"),
                        F.lit(": "),
                        F.round("metric_value", 2).cast("string"),
                        F.lit("ms ("),
                        F.round(F.abs("z_score"), 2).cast("string"),
                        F.lit(" std deviations, "),
                        F.col("anomaly_type"),
                        F.lit(")")
                    )
                )

            anomaly_count = anomalies_df.count()
            print(f"   ⚠️  Detected {anomaly_count} performance anomalies")

            # Show critical anomalies
            critical_anomalies = anomalies_df.filter("severity = 'critical'").count()
            if critical_anomalies > 0:
                print(f"   🚨 {critical_anomalies} CRITICAL anomalies require immediate attention!")

        else:
            print("   ✅ No anomalies detected")

        metrics["performance_anomalies"] = anomalies_df

        return metrics

    def print_summary(self, metrics: Dict[str, DataFrame]) -> None:
        """Print summary of computed metrics."""
        print("\n" + "=" * 60)
        print("📊 Performance Metrics Summary")
        print("=" * 60)

        # Version Performance Summary
        version_df = metrics["performance_by_version"]
        print(f"\nPerformance by Version:")
        print(f"  Total Combinations: {version_df.count():,}")

        version_summary = version_df.agg({
            "p50_duration_ms": "avg",
            "p95_duration_ms": "avg",
            "p99_duration_ms": "avg"
        }).collect()[0]

        print(f"  Average P50: {version_summary[0]:,.0f}ms")
        print(f"  Average P95: {version_summary[1]:,.0f}ms")
        print(f"  Average P99: {version_summary[2]:,.0f}ms")

        # Device Performance Summary
        device_df = metrics["device_performance"]
        print(f"\nDevice Performance:")
        print(f"  Total Combinations: {device_df.count():,}")

        device_summary = device_df.groupBy("device_type").agg(
            F.avg("avg_duration_ms").alias("avg_duration")
        ).orderBy(F.desc("avg_duration"))

        print("  Average Duration by Device:")
        for row in device_summary.collect():
            print(f"    {row['device_type']}: {row['avg_duration']:,.0f}ms")

        # Anomalies Summary
        anomalies_df = metrics["performance_anomalies"]
        anomaly_count = anomalies_df.count()
        print(f"\nPerformance Anomalies:")
        print(f"  Total Detected: {anomaly_count}")

        if anomaly_count > 0:
            severity_dist = anomalies_df.groupBy("severity").count().orderBy(F.desc("count"))
            print("  By Severity:")
            for row in severity_dist.collect():
                print(f"    {row['severity']}: {row['count']}")

        print("=" * 60)

    def get_table_mapping(self) -> Dict[str, str]:
        """Get database table mapping with special mode for anomalies."""
        return {
            "performance_by_version": "performance_by_version",
            "device_performance": "device_performance",
            "performance_anomalies": "performance_anomalies"
        }


def main():
    """Main job execution."""
    job = PerformanceMetricsJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
