"""
Test Helper Utilities

Provides reusable test infrastructure:
- Schema factories
- Data generators
- Custom assertions

Eliminates duplication across test files.
"""

from .assertions import (
    assert_column_exists,
    assert_dataframe_schema,
    assert_no_nulls,
    assert_percentile_accuracy,
    assert_retention_curve,
)
from .fixtures import (
    generate_cohort_data,
    generate_date_series,
    generate_interactions,
    generate_metadata,
    generate_skewed_data,
)
from .schemas import (
    aggregation_schema,
    cohort_retention_schema,
    dau_schema,
    interactions_schema,
    mau_schema,
    metadata_schema,
    power_users_schema,
    session_schema,
    stickiness_schema,
)


__all__ = [
    "aggregation_schema",
    "assert_column_exists",
    # Assertions
    "assert_dataframe_schema",
    "assert_no_nulls",
    "assert_percentile_accuracy",
    "assert_retention_curve",
    "cohort_retention_schema",
    "dau_schema",
    "generate_cohort_data",
    "generate_date_series",
    # Fixtures
    "generate_interactions",
    "generate_metadata",
    "generate_skewed_data",
    # Schemas
    "interactions_schema",
    "mau_schema",
    "metadata_schema",
    "power_users_schema",
    "session_schema",
    "stickiness_schema",
]
