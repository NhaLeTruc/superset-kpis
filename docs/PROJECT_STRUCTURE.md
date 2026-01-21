# Project Structure

This document provides a comprehensive overview of the project directory structure and file organization for the GoodNote Analytics Platform.

---

## Complete Directory Tree

```
superset-kpis/
├── README.md                          # Project overview and quick start guide
├── IMPLEMENTATION_PLAN.md             # Detailed implementation plan
├── ARCHITECTURE.md                    # System architecture documentation
├── PROJECT_STRUCTURE.md               # This file
├── requirements.txt                   # Python dependencies
├── pytest.ini                         # Pytest configuration
├── .gitignore                         # Git ignore rules
├── .env.example                       # Environment variables template
│
├── data/                              # Data directory (gitignored)
│   ├── raw/                           # Raw CSV datasets
│   │   ├── interactions/
│   │   │   ├── date=2023-01-01/
│   │   │   │   └── interactions.csv
│   │   │   ├── date=2023-01-02/
│   │   │   └── ...
│   │   └── metadata/
│   │       ├── country=US/
│   │       │   └── metadata.csv
│   │       ├── country=UK/
│   │       └── ...
│   ├── processed/                     # Processed Parquet files
│   │   └── interactions_enriched/
│   │       ├── date=2023-01-01/
│   │       │   ├── country=US/
│   │       │   └── country=UK/
│   │       └── ...
│   └── results/                       # CSV exports for validation
│       ├── dau_results.csv
│       ├── power_users.csv
│       └── ...
│
├── src/                               # Source code directory
│   ├── __init__.py
│   ├── config/                        # Configuration files
│   │   ├── __init__.py
│   │   ├── spark_config.py            # Spark session configuration
│   │   ├── database_config.py         # PostgreSQL connection config
│   │   └── logging_config.py          # Logging setup
│   │
│   ├── jobs/                          # Spark ETL jobs
│   │   ├── __init__.py
│   │   ├── 01_data_processing.py      # Data ingestion and join optimization
│   │   ├── 02_user_engagement.py      # DAU/MAU/cohorts analysis
│   │   ├── 03_performance_metrics.py  # Performance and correlation analysis
│   │   ├── 04_session_analysis.py     # Sessionization and metrics
│   │   └── 05_anomaly_detection.py    # Anomaly detection job
│   │
│   ├── utils/                         # Utility modules
│   │   ├── __init__.py
│   │   ├── spark_utils.py             # Spark helper functions
│   │   ├── postgres_writer.py         # PostgreSQL JDBC writer
│   │   ├── data_quality.py            # Data validation functions
│   │   ├── optimization.py            # Salting, bucketing helpers
│   │   ├── metrics.py                 # Custom accumulator definitions
│   │   └── logger.py                  # Logging utilities
│   │
│   ├── transforms/                    # Data transformation modules
│   │   ├── __init__.py
│   │   ├── engagement_transforms.py   # User engagement calculations
│   │   ├── performance_transforms.py  # Performance metric calculations
│   │   ├── session_transforms.py      # Session logic
│   │   └── anomaly_transforms.py      # Anomaly detection logic
│   │
│   └── schemas/                       # Data schemas
│       ├── __init__.py
│       ├── interactions_schema.py     # Interactions dataset schema
│       └── metadata_schema.py         # Metadata dataset schema
│
├── tests/                             # Test directory
│   ├── __init__.py
│   ├── conftest.py                    # Pytest fixtures and setup
│   ├── unit/                          # Unit tests
│   │   ├── __init__.py
│   │   ├── test_data_processing.py
│   │   ├── test_user_engagement.py
│   │   ├── test_performance_metrics.py
│   │   ├── test_session_analysis.py
│   │   ├── test_data_quality.py
│   │   └── test_optimization.py
│   ├── integration/                   # Integration tests
│   │   ├── __init__.py
│   │   ├── test_end_to_end.py
│   │   └── test_postgres_connection.py
│   └── fixtures/                      # Test data fixtures
│       ├── sample_interactions.csv
│       └── sample_metadata.csv
│
├── notebooks/                         # Jupyter notebooks
│   ├── 01_data_exploration.ipynb      # Initial data exploration
│   ├── 02_spark_ui_analysis.ipynb     # Spark UI screenshots and analysis
│   ├── 03_performance_tuning.ipynb    # Performance optimization notes
│   ├── 04_query_testing.ipynb         # SQL query development for Superset
│   └── 05_results_visualization.ipynb # Matplotlib/Seaborn visualizations
│
├── docker/                            # Docker configuration
│   ├── compose.yml                    # Main compose file
│   ├── .env.example                   # Environment variables for Docker
│   ├── spark/
│   │   ├── Dockerfile                 # Custom Spark image
│   │   ├── requirements.txt           # Python packages for Spark
│   │   ├── entrypoint.sh              # Container startup script
│   │   └── conf/
│   │       ├── spark-defaults.conf    # Spark configuration
│   │       └── log4j.properties       # Logging configuration
│   ├── superset/
│   │   ├── Dockerfile                 # Custom Superset image
│   │   ├── superset_config.py         # Superset configuration
│   │   └── pythonpath/                # Custom Superset extensions
│   └── postgres/
│       └── init.sql                   # Database initialization script
│
├── database/                          # Database scripts
│   ├── schema.sql                     # PostgreSQL schema definition
│   ├── indexes.sql                    # Index creation scripts
│   ├── partitions.sql                 # Partition management scripts
│   ├── views.sql                      # Materialized views for Superset
│   └── migrations/                    # Schema migration scripts
│       ├── 001_initial_schema.sql
│       ├── 002_add_indexes.sql
│       └── ...
│
├── superset/                          # Superset configuration
│   ├── dashboards/                    # Dashboard exports (JSON)
│   │   ├── 01_executive_overview.json
│   │   ├── 02_user_engagement.json
│   │   ├── 03_performance_monitoring.json
│   │   └── 04_session_analytics.json
│   ├── datasets/                      # Dataset configurations (YAML)
│   │   ├── daily_active_users.yaml
│   │   ├── power_users.yaml
│   │   └── ...
│   ├── charts/                        # Chart definitions (JSON)
│   │   ├── dau_timeseries.json
│   │   ├── cohort_heatmap.json
│   │   └── ...
│   └── sql/                           # Virtual dataset SQL queries
│       ├── user_retention_pivot.sql
│       ├── device_comparison.sql
│       └── ...
│
├── scripts/                           # Utility scripts
│   ├── generate_data.py               # Data generation script
│   ├── setup_environment.sh           # Environment setup script
│   ├── setup_postgres.sh              # PostgreSQL initialization
│   ├── setup_superset.sh              # Superset initialization
│   ├── run_all_jobs.sh                # Run all Spark jobs sequentially
│   ├── run_job.sh                     # Generic job runner
│   ├── validate_data.py               # Data quality validation
│   ├── export_dashboards.sh           # Export Superset dashboards
│   └── cleanup.sh                     # Clean up generated data
│
├── docs/                              # Documentation
│   ├── REPORT.md                      # Comprehensive technical report
│   ├── SETUP_GUIDE.md                 # Troubleshooting and quick reference
│   ├── SUPERSET_GUIDE.md              # Superset dashboard user guide
│   ├── OPTIMIZATION_REPORT.md         # Spark optimization analysis
│   ├── API_REFERENCE.md               # Code API documentation
│   ├── TROUBLESHOOTING.md             # Common issues and solutions
│   ├── screenshots/                   # Screenshots for documentation
│   │   ├── spark_ui/
│   │   │   ├── 01_job_overview.png
│   │   │   ├── 02_stage_details.png
│   │   │   ├── 03_task_metrics.png
│   │   │   ├── 04_executors.png
│   │   │   └── 05_sql_tab.png
│   │   └── superset_dashboards/
│   │       ├── 01_executive_overview.png
│   │       ├── 02_user_engagement.png
│   │       ├── 03_performance_monitoring.png
│   │       └── 04_session_analytics.png
│   └── diagrams/                      # Architecture diagrams
│       ├── system_architecture.png
│       ├── data_flow.png
│       └── deployment_diagram.png
│
└── logs/                              # Log files (gitignored)
    ├── spark/                         # Spark logs
    │   ├── application_123.log
    │   └── ...
    ├── jobs/                          # Job execution logs
    │   ├── data_processing_2023-11-13.log
    │   └── ...
    └── superset/                      # Superset logs
        └── superset.log
```

---

## Key Files Description

### Root Level Files

#### `README.md`
- Project overview and introduction
- Quick start guide (5-minute setup)
- Prerequisites and system requirements
- Common commands and usage examples
- Links to detailed documentation
- Contribution guidelines
- License information

#### `IMPLEMENTATION_PLAN.md`
- Comprehensive implementation strategy
- Task-by-task breakdown
- Optimization techniques explained
- Timeline and milestones
- Success criteria

#### `ARCHITECTURE.md`
- System architecture overview
- Component descriptions
- Data flow diagrams
- Technology stack details
- Deployment strategies
- Performance benchmarks

#### `requirements.txt`
```txt
# Core Dependencies
pyspark==3.5.0
py4j==0.10.9.7

# Database
psycopg2-binary==2.9.9
sqlalchemy==2.0.23

# Data Processing
pandas==2.1.4
numpy==1.26.2
pyarrow==14.0.1

# Testing
pytest==7.4.3
pytest-spark==0.6.0
chispa==0.9.4

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1

# Visualization (for notebooks)
matplotlib==3.8.2
seaborn==0.13.0
plotly==5.18.0

# Monitoring
py-spy==0.3.14
memory-profiler==0.61.0
```

#### `pytest.ini`
```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts =
    -v
    --tb=short
    --strict-markers
    --disable-warnings
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
```

---

### Source Code (`src/`)

#### `src/config/spark_config.py`
```python
"""
Spark session configuration and initialization
"""

from pyspark.sql import SparkSession
from typing import Dict, Optional

def create_spark_session(
    app_name: str = "goodnote-analytics",
    config: Optional[Dict[str, str]] = None
) -> SparkSession:
    """
    Create and configure Spark session with optimized settings

    Args:
        app_name: Application name
        config: Additional Spark configurations

    Returns:
        Configured SparkSession
    """
    # Implementation...
```

#### `src/jobs/01_data_processing.py`
```python
"""
Job 1: Data Processing and Optimized Join

This job performs:
1. Data ingestion from CSV
2. Schema validation
3. Data quality checks
4. Optimized join with skew handling
5. Output to Parquet format

Runtime: ~30 minutes for 1TB dataset
"""

import argparse
from pyspark.sql import SparkSession
from src.utils.spark_utils import broadcast_join, salted_join
from src.utils.data_quality import validate_schema, check_nulls
from src.schemas.interactions_schema import INTERACTIONS_SCHEMA
from src.schemas.metadata_schema import METADATA_SCHEMA

def main(args):
    # Implementation...

if __name__ == "__main__":
    # Entry point...
```

#### `src/utils/optimization.py`
```python
"""
Optimization utilities for Spark jobs

Includes:
- Salting for skewed keys
- Bucketing strategies
- Partition size calculations
- Memory tuning helpers
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import rand, col, concat, lit
from typing import List

def salt_dataframe(
    df: DataFrame,
    key_column: str,
    salt_factor: int = 10
) -> DataFrame:
    """
    Apply salting to a DataFrame to distribute skewed keys

    Args:
        df: Input DataFrame
        key_column: Column name containing skewed keys
        salt_factor: Number of salt buckets

    Returns:
        DataFrame with salted key column
    """
    # Implementation...
```

---

### Tests (`tests/`)

#### `tests/conftest.py`
```python
"""
Pytest configuration and fixtures for Spark testing
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()

@pytest.fixture
def sample_interactions_df(spark_session):
    """Create sample interactions DataFrame for testing"""
    # Implementation...
```

#### `tests/unit/test_data_processing.py`
```python
"""
Unit tests for data processing job
"""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from src.jobs.data_processing import process_data

def test_broadcast_join(spark_session, sample_interactions_df, sample_metadata_df):
    """Test that broadcast join produces correct results"""
    result = process_data(sample_interactions_df, sample_metadata_df, join_type="broadcast")

    # Assertions...
    assert result.count() == expected_count

def test_salted_join(spark_session):
    """Test that salted join handles skewed data correctly"""
    # Implementation...
```

---

### Docker Configuration (`docker/`)

#### `docker/compose.yml`
```yaml
version: '3.8'

services:
  spark-master:
    build: ./spark
    container_name: goodnote-spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
    ports:
      - "8080:8080"  # Spark UI
      - "7077:7077"  # Spark Master
      - "4040:4040"  # Application UI
    volumes:
      - ../src:/opt/spark-apps
      - ../data:/opt/spark-data
      - spark_logs:/opt/spark/logs
    networks:
      - goodnote-network

  spark-worker-1:
    build: ./spark
    container_name: goodnote-spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=16G
      - SPARK_WORKER_CORES=8
    depends_on:
      - spark-master
    volumes:
      - ../src:/opt/spark-apps
      - ../data:/opt/spark-data
    networks:
      - goodnote-network

  postgres:
    image: postgres:15-alpine
    container_name: goodnote-postgres
    environment:
      POSTGRES_DB: analytics
      POSTGRES_USER: analytics_user
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-secure_password}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ../database:/docker-entrypoint-initdb.d
    networks:
      - goodnote-network

  superset:
    image: apache/superset:3.0.0
    container_name: goodnote-superset
    environment:
      - SUPERSET_SECRET_KEY=${SUPERSET_SECRET_KEY:-change_me_in_production}
    ports:
      - "8088:8088"
    volumes:
      - ./superset/superset_config.py:/app/pythonpath/superset_config.py
      - superset_data:/app/superset
    networks:
      - goodnote-network
    depends_on:
      - postgres
      - redis

  redis:
    image: redis:7-alpine
    container_name: goodnote-redis
    ports:
      - "6379:6379"
    networks:
      - goodnote-network

  jupyter:
    image: jupyter/pyspark-notebook:spark-3.5.0
    container_name: goodnote-jupyter
    ports:
      - "8888:8888"
    volumes:
      - ../notebooks:/home/jovyan/work
      - ../src:/home/jovyan/src
      - ../data:/home/jovyan/data
    networks:
      - goodnote-network

volumes:
  postgres_data:
  superset_data:
  spark_logs:

networks:
  goodnote-network:
    driver: bridge
```

---

### Database Scripts (`database/`)

#### `database/schema.sql`
```sql
-- GoodNote Analytics Database Schema
-- PostgreSQL 15+

-- Create schema
CREATE SCHEMA IF NOT EXISTS goodnote_analytics;

-- Daily Active Users (Time-series, range partitioned)
CREATE TABLE goodnote_analytics.daily_active_users (
    date DATE NOT NULL,
    dau BIGINT NOT NULL,
    new_users BIGINT,
    returning_users BIGINT,
    total_interactions BIGINT,
    avg_duration_per_user DOUBLE PRECISION,
    PRIMARY KEY (date)
) PARTITION BY RANGE (date);

-- Create partitions for 2023
CREATE TABLE daily_active_users_2023_q1 PARTITION OF goodnote_analytics.daily_active_users
    FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');

CREATE TABLE daily_active_users_2023_q2 PARTITION OF goodnote_analytics.daily_active_users
    FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');

-- Additional tables...
-- (See full schema in database/schema.sql)
```

#### `database/indexes.sql`
```sql
-- Performance indexes for analytics tables

-- Index for date range queries
CREATE INDEX idx_dau_date ON goodnote_analytics.daily_active_users (date DESC);

-- Composite indexes for filtered queries
CREATE INDEX idx_power_users_country_device
    ON goodnote_analytics.power_users (country, device_type);

-- Covering index for common queries
CREATE INDEX idx_session_analytics_covering
    ON goodnote_analytics.session_analytics (date, device_type, country)
    INCLUDE (avg_session_duration_ms, total_sessions);
```

---

### Scripts (`scripts/`)

#### `scripts/run_all_jobs.sh`
```bash
#!/bin/bash
# Run all Spark jobs in sequence

set -e  # Exit on error

echo "========================================="
echo "GoodNote Analytics - Running All Jobs"
echo "========================================="

# Load environment variables
source .env

# Job 1: Data Processing
echo "[1/4] Running Data Processing Job..."
spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.sql.adaptive.enabled=true \
    --driver-memory 8g \
    --executor-memory 16g \
    src/jobs/01_data_processing.py \
    --input-interactions data/raw/interactions \
    --input-metadata data/raw/metadata \
    --output data/processed/interactions_enriched

echo "✓ Job 1 completed"

# Job 2: User Engagement
echo "[2/4] Running User Engagement Job..."
spark-submit \
    --master spark://spark-master:7077 \
    src/jobs/02_user_engagement.py \
    --input data/processed/interactions_enriched

echo "✓ Job 2 completed"

# Job 3: Performance Metrics
echo "[3/4] Running Performance Metrics Job..."
spark-submit \
    --master spark://spark-master:7077 \
    src/jobs/03_performance_metrics.py \
    --input data/processed/interactions_enriched

echo "✓ Job 3 completed"

# Job 4: Session Analysis
echo "[4/4] Running Session Analysis Job..."
spark-submit \
    --master spark://spark-master:7077 \
    src/jobs/04_session_analysis.py \
    --input data/processed/interactions_enriched

echo "✓ Job 4 completed"

echo "========================================="
echo "All jobs completed successfully!"
echo "========================================="
```

---

## File Naming Conventions

### Python Files
- **Jobs:** `01_job_name.py` (numbered for execution order)
- **Utilities:** `snake_case.py`
- **Tests:** `test_feature_name.py`
- **Classes:** `ClassName` (PascalCase)
- **Functions:** `function_name` (snake_case)
- **Constants:** `CONSTANT_NAME` (UPPER_SNAKE_CASE)

### Data Files
- **CSV:** `dataset_name.csv`
- **Parquet:** Partitioned directories (e.g., `date=2023-01-01/`)
- **Results:** `metric_name_results.csv`

### Documentation
- **Markdown:** `DOCUMENT_NAME.md` (UPPER_SNAKE_CASE for root docs)
- **Screenshots:** `01_descriptive_name.png` (numbered)

### Configuration
- **YAML:** `config_name.yaml`
- **SQL:** `script_purpose.sql`
- **Shell:** `script_name.sh`

---

## Directory Guidelines

### What to Commit
✅ Source code (`src/`)
✅ Tests (`tests/`)
✅ Documentation (`docs/`)
✅ Configuration templates (`*.example`)
✅ Database schemas (`database/*.sql`)
✅ Superset exports (`superset/dashboards/*.json`)
✅ Scripts (`scripts/*.sh`, `scripts/*.py`)

### What NOT to Commit (.gitignore)
❌ Data files (`data/`)
❌ Log files (`logs/`)
❌ Environment files (`.env`)
❌ Jupyter checkpoints (`.ipynb_checkpoints/`)
❌ Python cache (`__pycache__/`, `*.pyc`)
❌ Virtual environments (`venv/`, `.venv/`)
❌ IDE settings (`.vscode/`, `.idea/`)
❌ Docker volumes data

---

## Code Organization Principles

### Separation of Concerns
- **Jobs** (`src/jobs/`) - Orchestration and entry points
- **Transforms** (`src/transforms/`) - Business logic
- **Utils** (`src/utils/`) - Reusable utilities
- **Config** (`src/config/`) - Configuration management

### Dependency Direction
```
jobs/ → transforms/ → utils/
  ↓         ↓           ↓
config/ ← schemas/ ← config/
```

### Testability
- Keep business logic in `transforms/` (easy to unit test)
- Keep Spark setup in `config/` (shared across jobs and tests)
- Keep I/O in `jobs/` (integration tests)

---

## Getting Started

### Clone and Setup
```bash
# Clone repository
git clone <repo-url>
cd superset-kpis

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Setup environment variables
cp .env.example .env
# Edit .env with your settings

# Start Docker services
cd docker
docker compose up -d

# Generate sample data
python scripts/generate_data.py

# Run jobs
./scripts/run_all_jobs.sh
```

---

## Next Steps

1. Review [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for detailed task breakdown
2. Review [ARCHITECTURE.md](./ARCHITECTURE.md) for system design
3. Run `make quickstart` or see [SETUP_GUIDE.md](./SETUP_GUIDE.md) for troubleshooting
4. Refer to [API_REFERENCE.md](./docs/API_REFERENCE.md) for code documentation

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
