"""
Base Analytics Job Class

Provides common infrastructure for all analytics jobs:
- Spark session management
- Monitoring context setup
- Argument parsing
- Error handling and logging
- Template method pattern for job execution

Jobs should inherit from BaseAnalyticsJob and implement:
- compute_metrics() - core computation logic
- print_summary() - custom reporting
- get_argument_parser() - job-specific arguments
"""
import argparse
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from src.config.spark_config import create_spark_session, configure_job_specific_settings
from src.config.database_config import write_to_postgres
from src.utils.monitoring import create_monitoring_context, log_monitoring_summary


class BaseAnalyticsJob(ABC):
    """
    Base class for all analytics jobs.

    Provides common infrastructure and template method pattern.
    Subclasses implement job-specific logic.
    """

    def __init__(self, job_name: str, job_type: str = "analytics"):
        """
        Initialize base job.

        Args:
            job_name: Human-readable name (e.g., "User Engagement Analytics")
            job_type: Type for Spark tuning - "etl", "analytics", or "ml"
        """
        self.job_name = job_name
        self.job_type = job_type
        self.spark: Optional[SparkSession] = None
        self.monitoring_ctx: Optional[Dict] = None
        self.args = None

    def read_enriched_data(self, path: str) -> DataFrame:
        """
        Read enriched interactions from Parquet.

        Common implementation used by most analytics jobs.
        Override if job needs different input.

        Args:
            path: Path to Parquet file/directory

        Returns:
            DataFrame with enriched interactions (cached)
        """
        print(f"📖 Reading enriched data from: {path}")
        from pyspark import StorageLevel
        df = self.spark.read.parquet(path).persist(StorageLevel.MEMORY_AND_DISK)
        record_count = df.count()
        print(f"   ✅ Loaded {record_count:,} enriched interactions")

        # Track in monitoring
        if self.monitoring_ctx:
            self.monitoring_ctx["record_counter"].add(record_count)

        return df

    def read_parquet(self, path: str, name: str = "data") -> DataFrame:
        """
        Generic method to read any Parquet file.

        Args:
            path: Path to Parquet file/directory
            name: Display name for logging

        Returns:
            DataFrame
        """
        print(f"📖 Reading {name} from: {path}")
        df = self.spark.read.parquet(path)
        record_count = df.count()
        print(f"   ✅ Loaded {record_count:,} records")
        return df

    def write_to_database(self, metrics: Dict[str, DataFrame],
                         table_mapping: Dict[str, str],
                         mode: str = "overwrite",
                         batch_size: int = 10000,
                         num_partitions: Optional[int] = None) -> None:
        """
        Write multiple DataFrames to PostgreSQL.

        Args:
            metrics: Dictionary mapping metric names to DataFrames
            table_mapping: Dictionary mapping metric names to table names
            mode: Write mode - "append", "overwrite", "error", "ignore"
            batch_size: Rows per batch for JDBC
            num_partitions: Number of parallel write partitions
        """
        print("\n💾 Writing metrics to PostgreSQL...")

        for metric_name, df in metrics.items():
            if metric_name not in table_mapping:
                print(f"   ⚠️  Skipping {metric_name} - no table mapping")
                continue

            table_name = table_mapping[metric_name]
            print(f"\n   📝 Writing {metric_name} to {table_name}...")

            try:
                write_to_postgres(
                    df=df,
                    table_name=table_name,
                    mode=mode,
                    batch_size=batch_size,
                    num_partitions=num_partitions
                )

            except Exception as e:
                print(f"   ❌ Failed to write {metric_name}: {str(e)}")
                raise

    def write_to_parquet(self, metrics: Dict[str, DataFrame],
                        output_path: str) -> None:
        """
        Write multiple DataFrames to Parquet files.

        Args:
            metrics: Dictionary mapping metric names to DataFrames
            output_path: Base directory path for outputs
        """
        print(f"\n💾 Writing metrics to Parquet: {output_path}")

        for metric_name, df in metrics.items():
            output_file = f"{output_path}/{metric_name}"
            df.write.mode("overwrite").parquet(output_file)
            print(f"   ✅ Wrote {metric_name}")

    def setup_spark(self) -> None:
        """Create and configure Spark session."""
        self.spark = create_spark_session(app_name=f"GoodNote - {self.job_name}")
        configure_job_specific_settings(self.spark, job_type=self.job_type)

    def setup_monitoring(self, context_name: str) -> None:
        """
        Create monitoring context.

        Args:
            context_name: Name for monitoring context (e.g., "user_engagement")
        """
        self.monitoring_ctx = create_monitoring_context(
            self.spark.sparkContext,
            context_name
        )

    def print_job_header(self) -> None:
        """Print job startup header."""
        print("=" * 60)
        print(f"🚀 GoodNote {self.job_name}")
        print("=" * 60)
        print(f"Start Time: {datetime.now()}")

        # Print key arguments
        if self.args:
            for key, value in vars(self.args).items():
                if value and key not in ["func"]:  # Skip empty and internal args
                    print(f"{key.replace('_', ' ').title()}: {value}")

        print("=" * 60)

    def print_job_footer(self, success: bool = True) -> None:
        """
        Print job completion footer.

        Args:
            success: Whether job completed successfully
        """
        print("\n" + "=" * 60)
        if success:
            print("✅ Job completed successfully!")
        else:
            print("❌ Job failed!")
        print(f"End Time: {datetime.now()}")
        print("=" * 60)

    @abstractmethod
    def get_argument_parser(self) -> argparse.ArgumentParser:
        """
        Create argument parser with job-specific arguments.

        Returns:
            Configured ArgumentParser

        Example:
            parser = argparse.ArgumentParser(description="My Job")
            parser.add_argument("--input-path", required=True)
            return parser
        """
        pass

    @abstractmethod
    def compute_metrics(self) -> Dict[str, DataFrame]:
        """
        Core job computation logic.

        Returns:
            Dictionary mapping metric names to DataFrames

        Example:
            return {
                "dau": dau_df,
                "mau": mau_df
            }
        """
        pass

    @abstractmethod
    def print_summary(self, metrics: Dict[str, DataFrame]) -> None:
        """
        Print job-specific summary.

        Args:
            metrics: Computed metrics dictionary

        Example:
            print("DAU Average:", metrics["dau"].agg({"dau": "avg"}).collect()[0][0])
        """
        pass
    
    @abstractmethod
    def get_table_mapping(self) -> Optional[Dict[str, str]]:
        """
        Get mapping from metric names to database table names.

        Override this if job writes to database.

        Returns:
            Dictionary mapping metric names to table names, or None
        """
        return None

    def run(self) -> int:
        """
        Execute complete job pipeline.

        Template method that orchestrates:
        1. Argument parsing
        2. Spark setup
        3. Monitoring setup
        4. Core computation
        5. Output writing
        6. Summary reporting
        7. Cleanup

        Returns:
            Exit code (0 for success, 1 for failure)
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
            context_name = self.job_name.lower().replace(" ", "_").replace("-", "_")
            self.setup_monitoring(context_name)

            # Execute core computation
            metrics = self.compute_metrics()

            # Print summary
            self.print_summary(metrics)

            # Write to database if requested
            if hasattr(self.args, "write_to_db") and self.args.write_to_db:
                table_mapping = self.get_table_mapping()
                if table_mapping:
                    self.write_to_database(metrics, table_mapping)

            # Write to Parquet if requested
            if hasattr(self.args, "output_path") and self.args.output_path:
                self.write_to_parquet(metrics, self.args.output_path)

            # Log monitoring summary
            if self.monitoring_ctx:
                print("\n")
                log_monitoring_summary(self.monitoring_ctx, self.job_name)

            # Print footer
            self.print_job_footer(success=True)

            return 0

        except Exception as e:
            print("\n" + "=" * 60)
            print(f"❌ Job failed with error: {str(e)}")
            print("=" * 60)
            import traceback
            traceback.print_exc()

            self.print_job_footer(success=False)

            return 1

        finally:
            if self.spark:
                self.spark.stop()
