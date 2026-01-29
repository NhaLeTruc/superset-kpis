#!/bin/bash
# Setup TDD enforcement for this repository

echo "🔧 Setting up TDD enforcement..."

# Make pre-commit hook executable
chmod +x .githooks/pre-commit

# Install git hooks
git config core.hooksPath .githooks

echo "✅ Git hooks installed"
echo "   Pre-commit hook will now enforce TDD (tests before code)"
echo ""

# Create pytest configuration if it doesn't exist
if [ ! -f "pytest.ini" ]; then
    cat > pytest.ini << 'EOF'
[tool:pytest]
# Pytest configuration for TDD enforcement

testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Require minimum 80% coverage
addopts =
    -v
    --strict-markers
    --tb=short
    --cov=src
    --cov-report=term-missing
    --cov-report=html
    --cov-fail-under=80

# Markers
markers =
    unit: Unit tests (fast, isolated)
    integration: Integration tests (slower, requires services)
    slow: Tests that take >5 seconds

# Coverage settings
[coverage:run]
source = src
omit =
    */tests/*
    */__init__.py
    */conftest.py

[coverage:report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise AssertionError
    raise NotImplementedError
    if __name__ == .__main__.:
    if TYPE_CHECKING:
EOF
    echo "✅ pytest.ini created with 80% coverage requirement"
else
    echo "ℹ️  pytest.ini already exists"
fi

# Create conftest.py if it doesn't exist
if [ ! -f "tests/conftest.py" ]; then
    mkdir -p tests
    cat > tests/conftest.py << 'EOF'
"""
Pytest configuration and fixtures for TDD tests.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing.

    Scope: session (created once, shared across all tests)
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("goodnote-tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture
def sample_interactions(spark):
    """Basic interactions test data."""
    data = [
        ("u001", "2023-01-01 10:00:00", "page_view", "p001", 5000, "1.0.0"),
        ("u001", "2023-01-01 10:05:00", "edit", "p001", 120000, "1.0.0"),
        ("u002", "2023-01-01 10:10:00", "page_view", "p002", 3000, "1.0.0"),
    ]
    return spark.createDataFrame(data, ["user_id", "timestamp", "action_type", "page_id", "duration_ms", "app_version"])


@pytest.fixture
def sample_metadata(spark):
    """Basic metadata test data."""
    data = [
        ("u001", "2023-01-01", "US", "iPad", "premium"),
        ("u002", "2023-01-01", "UK", "iPhone", "free"),
    ]
    return spark.createDataFrame(data, ["user_id", "join_date", "country", "device_type", "subscription_type"])
EOF
    echo "✅ tests/conftest.py created with basic fixtures"
else
    echo "ℹ️  tests/conftest.py already exists"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ TDD Enforcement Setup Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "What was installed:"
echo "  1. ✅ Pre-commit git hook (enforces tests exist)"
echo "  2. ✅ pytest.ini (requires 80% coverage)"
echo "  3. ✅ tests/conftest.py (test fixtures)"
echo ""
echo "How it works:"
echo "  • git commit → checks tests exist for all source files"
echo "  • pytest → fails if coverage < 80%"
echo "  • Prevents accidental TDD violations"
echo ""
echo "Next steps:"
echo "  1. Read: .claude/SESSION_RULES.md (Claude must follow)"
echo "  2. Read: docs/TESTING_GUIDE.md (testing guide)"
echo "  3. Start coding with TDD!"
echo ""
echo "Quick test:"
echo "  pytest tests/unit -v  # Run unit tests"
echo "  pytest --cov=src      # Check coverage"
echo ""
