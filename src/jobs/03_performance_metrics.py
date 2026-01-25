#!/usr/bin/env python3
"""
Job 3: Performance Metrics

Calculates performance metrics:
- Percentiles (P50, P95, P99) by app version
- Device performance correlation
- Statistical anomaly detection

Writes results to PostgreSQL for monitoring dashboards.

Usage (via helper script):
    ./scripts/run_spark_job.sh src/jobs/03_performance_metrics.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db

Usage (direct spark-submit):
    docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/03_performance_metrics.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db'
"""
import argparse
import sys
from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.jobs.base_job import BaseAnalyticsJob
from src.schemas.columns import (
    COL_TIMESTAMP, COL_DURATION_MS, COL_APP_VERSION,
    COL_DEVICE_TYPE, COL_USER_ID, COL_METRIC_DATE,
)
from src.config.constants import (
    DEFAULT_PERCENTILES, Z_SCORE_ANOMALY_THRESHOLD,
    TABLE_PERFORMANCE_BY_VERSION, TABLE_DEVICE_PERFORMANCE,
    TABLE_DEVICE_CORRELATION, TABLE_PERFORMANCE_ANOMALIES,
)
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
        enriched_df = self.read_parquet(self.args.enriched_path, "enriched interactions")

        # Add date column for time-series analysis
        enriched_df = enriched_df.withColumn(COL_METRIC_DATE, F.to_date(COL_TIMESTAMP))

        metrics = {}

        # 1. Percentiles by App Version
        print("\n📊 Calculating Performance Percentiles by App Version...")
        version_perf_df = calculate_percentiles(
            df=enriched_df,
            value_column=COL_DURATION_MS,
            group_by_columns=[COL_APP_VERSION, COL_METRIC_DATE],
            percentiles=DEFAULT_PERCENTILES
        )

        # Rename percentile columns to match database schema
        version_perf_df = version_perf_df \
            .withColumnRenamed("p50", "p50_duration_ms") \
            .withColumnRenamed("p95", "p95_duration_ms") \
            .withColumnRenamed("p99", "p99_duration_ms") \
            .persist()

        version_count = version_perf_df.count()
        print(f"   ✅ Computed percentiles for {version_count} app version-date combinations")
        metrics["performance_by_version"] = version_perf_df

        # 2. Device Performance
        print("\n📊 Calculating Device Performance...")
        device_percentiles = calculate_percentiles(
            df=enriched_df,
            value_column=COL_DURATION_MS,
            group_by_columns=[COL_DEVICE_TYPE, COL_METRIC_DATE],
            percentiles=DEFAULT_PERCENTILES
        )

        device_percentiles = device_percentiles \
            .withColumnRenamed("p50", "p50_duration_ms") \
            .withColumnRenamed("p95", "p95_duration_ms") \
            .withColumnRenamed("p99", "p99_duration_ms")

        # Add user count
        device_perf_with_users = enriched_df.groupBy(COL_DEVICE_TYPE, COL_METRIC_DATE).agg(
            F.countDistinct(COL_USER_ID).alias("user_count")
        )

        device_perf_final = device_percentiles.join(
            device_perf_with_users,
            [COL_DEVICE_TYPE, COL_METRIC_DATE],
            "left"
        ).persist()

        device_count = device_perf_final.count()
        print(f"   ✅ Computed device performance for {device_count} device-date combinations")
        metrics["device_performance"] = device_perf_final

        # 2b. Device-Performance Correlation (ANOVA)
        print("\n📊 Calculating Device-Performance Correlation (ANOVA)...")
        device_correlation_df = calculate_device_correlation(
            interactions_df=enriched_df,
            metadata_df=enriched_df.select(COL_USER_ID, COL_DEVICE_TYPE).distinct()
        ).persist()
        correlation_count = device_correlation_df.count()
        print(f"   ✅ Computed correlation for {correlation_count} device types")

        # Show ANOVA results
        anova_stats = device_correlation_df.select("f_statistic", "eta_squared").first()
        if anova_stats["f_statistic"] is not None:
            print(f"   📈 F-statistic: {anova_stats['f_statistic']:.2f}")
            print(f"   📈 Eta-squared (effect size): {anova_stats['eta_squared']:.4f}")
        metrics["device_correlation"] = device_correlation_df

        # 3. Statistical Anomaly Detection
        print("\n📊 Detecting Performance Anomalies...")
        anomalies_df = detect_anomalies_statistical(
            df=enriched_df,
            value_column=COL_DURATION_MS,
            z_threshold=Z_SCORE_ANOMALY_THRESHOLD,
            group_by_columns=[COL_APP_VERSION, COL_METRIC_DATE]
        )

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

        # Cache and count once
        anomalies_df = anomalies_df.persist()
        anomaly_count = anomalies_df.count()

        if anomaly_count > 0:
            print(f"   ⚠️  Detected {anomaly_count} performance anomalies")

            # Show critical anomalies
            critical_count = anomalies_df.filter("severity = 'critical'").count()
            if critical_count > 0:
                print(f"   🚨 {critical_count} CRITICAL anomalies require immediate attention!")
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

        version_summary = version_df.agg(
            F.avg("p50_duration_ms").alias("avg_p50"),
            F.avg("p95_duration_ms").alias("avg_p95"),
            F.avg("p99_duration_ms").alias("avg_p99")
        ).collect()[0]

        print(f"  Average P50: {version_summary['avg_p50']:,.0f}ms")
        print(f"  Average P95: {version_summary['avg_p95']:,.0f}ms")
        print(f"  Average P99: {version_summary['avg_p99']:,.0f}ms")

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

        # Device Correlation Summary (ANOVA)
        correlation_df = metrics["device_correlation"]
        print(f"\nDevice-Performance Correlation (ANOVA):")
        anova_row = correlation_df.select("f_statistic", "eta_squared").first()
        if anova_row and anova_row["f_statistic"] is not None:
            f_stat = anova_row["f_statistic"]
            eta_sq = anova_row["eta_squared"]
            print(f"  F-statistic: {f_stat:.2f}")
            print(f"  Eta-squared: {eta_sq:.4f} ({eta_sq * 100:.1f}% variance explained)")
            # Interpret effect size
            if eta_sq >= 0.14:
                effect = "large"
            elif eta_sq >= 0.06:
                effect = "medium"
            else:
                effect = "small"
            print(f"  Effect size: {effect}")
        else:
            print("  Insufficient data for correlation analysis")

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
            "performance_by_version": TABLE_PERFORMANCE_BY_VERSION,
            "device_performance": TABLE_DEVICE_PERFORMANCE,
            "device_correlation": TABLE_DEVICE_CORRELATION,
            "performance_anomalies": TABLE_PERFORMANCE_ANOMALIES,
        }


def main():
    """Main job execution."""
    job = PerformanceMetricsJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
