# CLAUDE.md — GoodNote Analytics Platform

## What This Project Is

Production-grade Apache Spark ETL pipeline that processes user interaction data
(GoodNote note-taking app) and serves analytics through Apache Superset dashboards.

**Stack:** PySpark 3.5 · PostgreSQL 15 · Superset 3.0 · Redis 7 · Docker Compose

---

## TDD Is Mandatory

This project enforces strict Test-Driven Development. Full rules are in
[.claude/SESSION_RULES.md](.claude/SESSION_RULES.md). The short version:

- **Write the failing test first. Always.**
- Show RED state before writing any implementation.
- Show GREEN state after implementing.
- Every function must have a corresponding test — no exceptions.
- Function signatures must match `docs/TDD_SPEC.md` if it exists for that module.

**If I write implementation code without a failing test first, stop me.**

---

## Running Commands

**All commands run inside Docker containers, not on the host.**

```bash
make test                        # run all 167 tests (unit + integration)
make test-unit                   # unit tests only
make test-integration            # integration tests only
make test-coverage               # coverage report → htmlcov/index.html
make test-file F=tests/unit/...  # single file
make test-function T=tests/...::test_name  # single test

make lint                        # Ruff linter (no fixes)
make fix                         # auto-fix all Ruff issues + format
make check                       # check format + lint without changes
make typecheck                   # mypy (src/ only, jobs/ excluded)
make security                    # Bandit scan
make quality                     # lint + typecheck + security combined
```

**Data and jobs:**

```bash
make generate-data               # medium dataset (runs on HOST, not Docker)
make generate-data-small         # 1K users / 10K interactions
make generate-data-large         # 100K users / 1M interactions

make run-jobs                    # all 4 ETL jobs sequentially
make run-job-1                   # Job 1: data processing → Parquet
make run-job-2                   # Job 2: engagement → PostgreSQL
make run-job-3                   # Job 3: performance → PostgreSQL
make run-job-4                   # Job 4: session analysis → PostgreSQL
```

**Infrastructure:**

```bash
make quickstart                  # up + generate-data + run-jobs (full reset)
make up / make down              # start / stop all containers
make status                      # docker compose ps
make shell                       # bash in goodnote-spark-dev
make shell-master                # bash in goodnote-spark-master
make db-connect                  # psql into analytics DB
make db-tables                   # list all tables
make db-table-counts             # row counts per table
make db-query Q="SELECT ..."     # run ad-hoc SQL
```

**The data generator runs on the host** (plain `python3`), not via `docker exec`.
Everything else (`test`, `lint`, `run-job-*`) runs inside containers.

---

## Docker Services and Container Names

| Service | Container | Ports |
| --- | --- | --- |
| Spark Master | `goodnote-spark-master` | 7077, 8080, 4040 |
| Spark Worker 1 | `goodnote-spark-worker-1` | — |
| Spark Worker 2 | `goodnote-spark-worker-2` | — |
| Spark History | `goodnote-spark-history` | 18080 |
| Spark Dev (tests) | `goodnote-spark-dev` | — |
| PostgreSQL | `goodnote-postgres` | 5432 |
| Redis | `goodnote-redis` | 6379 |
| Superset | `goodnote-superset` | 8088 |
| Jupyter | `goodnote-jupyter` | 8888 |

All containers share network `goodnote-network`. Services reach each other by
service name (e.g. `postgres`, `spark-master`, `redis`).

**PYTHONPATH inside containers:**

- `goodnote-spark-master`: `/opt/spark-apps`  → `from src.xxx` resolves there
- `goodnote-spark-dev`: `/app`                → `from src.xxx` resolves there

Jupyter (`goodnote-jupyter`) is defined in `docker-compose.yml`. Start it separately with
`make jupyter-up`; access at `http://localhost:8888` (token: `goodnote`).

---

## Project Layout

```text
src/
├── jobs/           # 4 Spark ETL jobs (01–04), plus base_job.py
├── transforms/     # modular computation logic
│   ├── engagement/ # DAU, MAU, cohort retention, power users, stickiness
│   ├── performance/# percentile calculations, Z-score anomaly detection
│   ├── session/    # sessionization, bounce rates, action distribution
│   └── join/       # broadcast join + salting strategies
├── config/
│   ├── spark_session.py    # create_spark_session() factory
│   ├── spark_tuning.py     # configure_job_specific_settings()
│   ├── constants.py        # all numeric thresholds + table name strings
│   └── postgres/           # connection.py, reader.py, writer.py
├── schemas/
│   ├── columns.py          # column name constants (use these, not raw strings)
│   ├── interactions_schema.py
│   └── metadata_schema.py
└── utils/
    ├── data_quality.py     # null detection, validation
    └── monitoring/         # Spark accumulators, metrics reporting

tests/
├── unit/           # isolated transform tests (use chispa for DataFrame equality)
└── integration/    # full job tests with PostgreSQL writes

database/
├── 01_schema.sql   # 13 tables — edit here to add new output tables
└── 02_indexes.sql  # 40+ indexes — edit here to add new indexes

scripts/
├── generate_sample_data.py  # synthetic data generator (host only)
└── run_spark_job.sh         # spark-submit wrapper

docs/               # 12 reference guides
notebooks/          # Jupyter ML notebooks (to be created, see ML_PLAN.md)
```

---

## The 4 ETL Jobs

| Job | File | Input | Output |
| --- | --- | --- | --- |
| 1 Data Processing | `src/jobs/01_data_processing.py` | CSV (raw) | Parquet (`data/processed/`) |
| 2 User Engagement | `src/jobs/02_user_engagement.py` | Parquet | `daily_active_users`, `monthly_active_users`, `user_stickiness`, `power_users`, `cohort_retention` |
| 3 Performance | `src/jobs/03_performance_metrics.py` | Parquet | `performance_by_version`, `device_performance`, `device_correlation`, `performance_anomalies` |
| 4 Session Analysis | `src/jobs/04_session_analysis.py` | Parquet | `session_metrics`, `session_frequency`, `bounce_rates`, `action_distribution` |

Jobs 2–4 all read from the same Parquet output of Job 1.

**Adding a new job:** Inherit from `BaseAnalyticsJob` in `src/jobs/base_job.py`.
Set `job_type="ml"` for ML jobs — the tuning layer already handles this.

---

## Code Conventions

### Style

- **Line length:** 100 characters (Ruff enforced)
- **Quotes:** double (`"`)
- **Formatter:** Ruff (not Black) — `make fix` runs it
- **Imports:** isort via Ruff, 2 blank lines after imports, first-party = `src`

### Column and Table Names

Never use raw strings for column or table names. Use the constants:

```python
from src.schemas.columns import COL_USER_ID, COL_TIMESTAMP, COL_DURATION_MS
from src.config.constants import TABLE_DAILY_ACTIVE_USERS, Z_SCORE_ANOMALY_THRESHOLD
```

All column names are in `src/schemas/columns.py`.
All table names and numeric thresholds are in `src/config/constants.py`.

### Spark Sessions

Always use the factory, never call `SparkSession.builder` directly in production code:

```python
from src.config.spark_config import create_spark_session
spark = create_spark_session(app_name="My Job", job_type="analytics")
```

### Writing to PostgreSQL

Use the existing writer — never write JDBC calls directly:

```python
from src.config.database_config import write_to_postgres
write_to_postgres(spark, df, table_name="my_table")
```

### Type Checking

- mypy runs on `src/` but **excludes** `src/jobs/` and `scripts/` (structural typing issues)
- `src/schemas/` has strict checking enabled
- PySpark stubs are incomplete — `ignore_missing_imports = true` is set globally

---

## Testing Rules

- Unit tests use **chispa** (`assert_df_equality`) for PySpark DataFrame comparisons
- Integration tests write to PostgreSQL and verify row counts
- Test files mirror source layout: `tests/unit/engagement/` ↔ `src/transforms/engagement/`
- Run `make test`, never `pytest` on host (no local PySpark/Java)
- Coverage target: 85%+ on `src/`

---

## Database Schema

13 tables across 4 categories:

- **Engagement:** `daily_active_users`, `monthly_active_users`, `user_stickiness`, `power_users`, `cohort_retention`
- **Performance:** `performance_by_version`, `device_performance`, `device_correlation`, `performance_anomalies`
- **Session:** `session_metrics`, `session_frequency`, `bounce_rates`, `action_distribution`
- **Raw/Monitoring:** `user_interactions`, `user_metadata`, `etl_job_runs`

Connection (inside containers): `host=postgres port=5432 dbname=analytics user=analytics_user`

---

## Key Design Decisions

- **Anomaly detection** in `src/transforms/performance/anomalies.py` uses a 2-pass iterative
  Z-score (`Z_SCORE_ANOMALY_THRESHOLD = 3.0`). The Z-score is stored as-is; severity is derived
  from separate thresholds (CRITICAL ≥ 4.0, HIGH ≥ 3.5, MEDIUM ≥ 3.0).
- **Hot-key salting** in Job 1 detects users at the 99th percentile of interaction frequency
  (`HOT_KEY_THRESHOLD_PERCENTILE = 0.99`) and distributes them across 10 partitions to
  prevent Spark shuffle skew.
- **No scikit-learn** in `requirements.txt`. ML uses Spark MLlib only (bundled with PySpark).
- **Session window** is 10 minutes (`SESSION_TIMEOUT_SECONDS = 600`) matching the Spark
  `session_window()` call in Job 4.
- **Superset credentials** are set in `.env`. To reset them, delete `superset/superset.db`
  and restart the container — `fab create-admin` does not update existing users.

---

## ML Notebooks

Four Spark MLlib feasibility notebooks live in `notebooks/`:

| Notebook | Model | Target |
| --- | --- | --- |
| `01_churn_prediction.ipynb` | `GBTClassifier` | Predict 30-day churn (AUC-ROC) |
| `02_behavioral_segmentation.ipynb` | `KMeans` | Discover user clusters (silhouette) |
| `03_ml_anomaly_detection.ipynb` | `KMeans` + distance | Compare vs Z-score anomalies |
| `04_retention_forecasting.ipynb` | `GBTRegressor` + `LinearRegression` | Forecast week-12 retention (RMSE) |

Start Jupyter with `make jupyter-up`, then open `http://localhost:8888` (token: `goodnote`).

## Active Plans

- [docs/EXPANSION_PLAN.md](docs/EXPANSION_PLAN.md) — Broader roadmap items.
