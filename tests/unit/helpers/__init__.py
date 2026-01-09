"""
Test Helper Utilities

Provides reusable test infrastructure:
- Schema factories
- Data generators
- Custom assertions

Eliminates duplication across test files.
"""
from .schemas import (
    interactions_schema,
    metadata_schema,
    session_schema,
    aggregation_schema,
    dau_schema,
    mau_schema,
    stickiness_schema,
    power_users_schema,
    cohort_retention_schema
)
from .fixtures import (
    generate_interactions,
    generate_metadata,
    generate_skewed_data,
    generate_cohort_data,
    generate_date_series
)
from .assertions import (
    assert_dataframe_schema,
    assert_percentile_accuracy,
    assert_retention_curve,
    assert_column_exists,
    assert_no_nulls
)

__all__ = [
    # Schemas
    "interactions_schema",
    "metadata_schema",
    "session_schema",
    "aggregation_schema",
    "dau_schema",
    "mau_schema",
    "stickiness_schema",
    "power_users_schema",
    "cohort_retention_schema",
    # Fixtures
    "generate_interactions",
    "generate_metadata",
    "generate_skewed_data",
    "generate_cohort_data",
    "generate_date_series",
    # Assertions
    "assert_dataframe_schema",
    "assert_percentile_accuracy",
    "assert_retention_curve",
    "assert_column_exists",
    "assert_no_nulls",
]
