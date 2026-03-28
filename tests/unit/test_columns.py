"""
Unit tests for column name constants module.

Verifies that all constants are non-empty strings and that no two
constants share the same value (which would silently alias columns).
"""

import inspect

import src.schemas.columns as columns_module


def _all_col_constants() -> dict[str, str]:
    """Return all COL_* names and their string values from the module."""
    return {
        name: value
        for name, value in inspect.getmembers(columns_module)
        if name.startswith("COL_") and isinstance(value, str)
    }


class TestColumnConstants:
    """Sanity checks for src/schemas/columns.py."""

    def test_all_constants_are_non_empty_strings(self):
        """
        GIVEN: All COL_* constants defined in the module
        WHEN: Each value is inspected
        THEN: Every value is a non-empty string
        """
        constants = _all_col_constants()
        assert constants, "No COL_* constants found — module may be empty"
        for name, value in constants.items():
            assert isinstance(value, str), f"{name} is not a string"
            assert value.strip(), f"{name} is an empty or whitespace-only string"

    def test_no_duplicate_values(self):
        """
        GIVEN: All COL_* constants defined in the module
        WHEN: Values are compared for uniqueness
        THEN: No two constants share the same string value

        Duplicate values would cause silent column aliasing when both are
        used in the same DataFrame operation (e.g., a select that includes
        both COL_FOO and COL_BAR where both equal "foo" would silently
        produce two columns named "foo").
        """
        constants = _all_col_constants()
        seen: dict[str, str] = {}
        for name, value in constants.items():
            if value in seen:
                raise AssertionError(
                    f"Duplicate column value '{value}': "
                    f"both {seen[value]} and {name} map to the same string"
                )
            seen[value] = name

    def test_values_use_snake_case(self):
        """
        GIVEN: All COL_* constants defined in the module
        WHEN: Each value is inspected
        THEN: Values contain only lowercase letters, digits, and underscores

        Spark column names are case-insensitive but the project convention
        is lowercase snake_case to match PostgreSQL column naming.
        """
        constants = _all_col_constants()
        for name, value in constants.items():
            assert value == value.lower(), f"{name}='{value}' is not lowercase"
            assert all(c.isalnum() or c == "_" for c in value), (
                f"{name}='{value}' contains characters other than [a-z0-9_]"
            )
