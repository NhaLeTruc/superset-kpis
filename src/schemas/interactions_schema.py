"""
Schema definition for interactions data.

This module defines the schema for user interaction events.
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


INTERACTIONS_SCHEMA = StructType([
    StructField("interaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action_type", StringType(), False),
    StructField("page_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("duration_ms", IntegerType(), False),
])