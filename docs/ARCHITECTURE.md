# System Architecture

## Overview

This document describes the complete system architecture for the GoodNote Analytics Platform, a production-grade data processing and visualization system built on Apache Spark, PostgreSQL, and Apache Superset.

---

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES LAYER                         │
│  ┌────────────────────────┐      ┌────────────────────────┐     │
│  │  User Interactions     │      │   User Metadata        │     │
│  │  ~1TB CSV Files        │      │   ~100GB CSV Files     │     │
│  │  Partitioned by Date   │      │   Partitioned by       │     │
│  │  Schema: user_id,      │      │   Country              │     │
│  │  timestamp, action,    │      │   Schema: user_id,     │     │
│  │  page_id, duration,    │      │   registration_date, country,  │     │
│  │  app_version           │      │   device, subscription │     │
│  └────────────┬───────────┘      └────────────┬───────────┘     │
└───────────────┼──────────────────────────────┼──────────────────┘
                │                              │
                └──────────────┬───────────────┘
                               ↓
┌──────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER (Spark)                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Spark Cluster                          │    │
│  │  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐ │    │
│  │  │ Master Node   │  │ Worker Node  │  │ Worker Node  │ │    │
│  │  │ (8080, 7077)  │  │ (8 cores,    │  │ (8 cores,    │ │    │
│  │  └───────────────┘  │  16GB RAM)   │  │  16GB RAM)   │ │    │
│  │                     └──────────────┘  └──────────────┘ │    │
│  │                                                          │    │
│  │  PySpark 3.5 ETL Jobs:                                  │    │
│  │  ┌────────────────────────────────────────────────┐    │    │
│  │  │ Job 1: Data Processing & Optimized Join        │    │    │
│  │  │  - Broadcast join for metadata                 │    │    │
│  │  │  - Salting for skewed keys                     │    │    │
│  │  │  - Adaptive Query Execution (AQE)              │    │    │
│  │  └────────────────────────────────────────────────┘    │    │
│  │  ┌────────────────────────────────────────────────┐    │    │
│  │  │ Job 2: User Engagement Analysis                │    │    │
│  │  │  - DAU/MAU calculations                        │    │    │
│  │  │  - Power users identification                  │    │    │
│  │  │  - Cohort retention analysis                   │    │    │
│  │  └────────────────────────────────────────────────┘    │    │
│  │  ┌────────────────────────────────────────────────┐    │    │
│  │  │ Job 3: Performance Metrics                     │    │    │
│  │  │  - Percentile calculations (P50, P95, P99)     │    │    │
│  │  │  - Device-performance correlation              │    │    │
│  │  │  - Anomaly detection                           │    │    │
│  │  └────────────────────────────────────────────────┘    │    │
│  │  ┌────────────────────────────────────────────────┐    │    │
│  │  │ Job 4: Session Analysis                        │    │    │
│  │  │  - Sessionization (30-min window)              │    │    │
│  │  │  - Session metrics (duration, actions)         │    │    │
│  │  │  - Bounce rate calculation                     │    │    │
│  │  └────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
│  Monitoring:                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐  │
│  │ Spark UI        │  │ History Server  │  │ Custom         │  │
│  │ (4040, 8080)    │  │ (18080)         │  │ Accumulators   │  │
│  └─────────────────┘  └─────────────────┘  └────────────────┘  │
└───────────────────────────────┬──────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                                 │
│  ┌────────────────────────────────────────────────────────┐      │
│  │               Intermediate Storage (Parquet)            │      │
│  │  ┌──────────────────────────────────────────────┐      │      │
│  │  │ data/processed/interactions_enriched/        │      │      │
│  │  │   Partitioned by: date, country              │      │      │
│  │  │   Format: Parquet (Snappy compression)       │      │      │
│  │  └──────────────────────────────────────────────┘      │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                   │
│  ┌────────────────────────────────────────────────────────┐      │
│  │         Analytics Database (PostgreSQL 15)              │      │
│  │  ┌──────────────────────────────────────────────┐      │      │
│  │  │  Schema: goodnote_analytics                  │      │      │
│  │  │                                               │      │      │
│  │  │  Tables:                                      │      │      │
│  │  │  • daily_active_users (time-series)          │      │      │
│  │  │  • monthly_active_users                      │      │      │
│  │  │  • power_users (top 1% by engagement)        │      │      │
│  │  │  • cohort_retention (weekly cohorts)         │      │      │
│  │  │  • performance_by_version (p95 metrics)      │      │      │
│  │  │  • device_performance (correlation data)     │      │      │
│  │  │  • usage_anomalies (alerts)                  │      │      │
│  │  │  • session_analytics (sessionized data)      │      │      │
│  │  │  • action_distribution (by type)             │      │      │
│  │  │  • user_engagement_summary (user profiles)   │      │      │
│  │  │                                               │      │      │
│  │  │  Indexes: B-tree on date, user_id, country   │      │      │
│  │  │  Partitioning: Range partitioning by date    │      │      │
│  │  └──────────────────────────────────────────────┘      │      │
│  └────────────────────────────────────────────────────────┘      │
└───────────────────────────────┬──────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│              VISUALIZATION LAYER (Apache Superset)                │
│  ┌────────────────────────────────────────────────────────┐      │
│  │                   Superset Web Server                   │      │
│  │                      (Port 8088)                        │      │
│  │                                                          │      │
│  │  Features:                                              │      │
│  │  • SQL Lab (Ad-hoc querying)                           │      │
│  │  • Interactive dashboards                               │      │
│  │  • Native filters & cross-filtering                    │      │
│  │  • Scheduled reports                                    │      │
│  │  • Row-level security                                   │      │
│  │  • Caching layer (Redis)                               │      │
│  │                                                          │      │
│  │  Dashboards:                                            │      │
│  │  ┌──────────────────────────────────────────────┐      │      │
│  │  │ 1. Executive Overview                        │      │      │
│  │  │    - DAU/MAU trends                          │      │      │
│  │  │    - Key metrics cards                       │      │      │
│  │  │    - Geographic distribution                 │      │      │
│  │  └──────────────────────────────────────────────┘      │      │
│  │  ┌──────────────────────────────────────────────┐      │      │
│  │  │ 2. User Engagement Deep Dive                 │      │      │
│  │  │    - Cohort retention heatmap                │      │      │
│  │  │    - Power users table                       │      │      │
│  │  │    - Engagement score distribution           │      │      │
│  │  └──────────────────────────────────────────────┘      │      │
│  │  ┌──────────────────────────────────────────────┐      │      │
│  │  │ 3. Performance Monitoring                    │      │      │
│  │  │    - P95 load times by version               │      │      │
│  │  │    - Device performance comparison           │      │      │
│  │  │    - Anomaly alerts table                    │      │      │
│  │  └──────────────────────────────────────────────┘      │      │
│  │  ┌──────────────────────────────────────────────┐      │      │
│  │  │ 4. Session Analytics                         │      │      │
│  │  │    - Session duration treemap                │      │      │
│  │  │    - Action type distribution                │      │      │
│  │  │    - Bounce rate analysis                    │      │      │
│  │  └──────────────────────────────────────────────┘      │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                   │
│  ┌────────────────────────────────────────────────────────┐      │
│  │            Redis Cache (Port 6379)                      │      │
│  │  • Query result caching (5-minute TTL)                 │      │
│  │  • Dashboard metadata caching                          │      │
│  └────────────────────────────────────────────────────────┘      │
└───────────────────────────────┬──────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│                          END USERS                                │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │ Data         │  │ Product      │  │ Engineering        │    │
│  │ Analysts     │  │ Managers     │  │ Teams              │    │
│  │ (SQL Lab)    │  │ (Dashboards) │  │ (Performance)      │    │
│  └──────────────┘  └──────────────┘  └────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Data Sources Layer

#### User Interactions Dataset
- **Size:** ~1 TB
- **Format:** CSV (will be converted to Parquet)
- **Partitioning:** By date (YYYY-MM-DD)
- **Schema:**
  ```
  user_id: STRING
  timestamp: TIMESTAMP
  action_type: STRING (page_view, edit, create, delete, share)
  page_id: STRING
  duration_ms: LONG (0 to 28,800,000 = 8 hours)
  app_version: STRING (semantic versioning: X.Y.Z)
  ```
- **Volume:** ~10 billion rows (estimated)
- **Growth Rate:** ~100M rows per day

#### User Metadata Dataset
- **Size:** ~100 GB
- **Format:** CSV (will be converted to Parquet)
- **Partitioning:** By country
- **Schema:**
  ```
  user_id: STRING (unique identifier)
  registration_date: DATE
  country: STRING (ISO 2-letter code)
  device_type: STRING (iPhone, iPad, Android, etc.)
  subscription_type: STRING (free, basic, premium, enterprise)
  ```
- **Volume:** ~10 million users (estimated)
- **Update Frequency:** Daily incremental updates

---

### 2. Processing Layer (Apache Spark)

#### Cluster Configuration

**Development Environment:**
```yaml
Spark Version: 3.5.0
Deployment Mode: Standalone
Master Node: 1 (4 cores, 8GB RAM)
Worker Nodes: 2 (8 cores, 16GB RAM each)
Total Resources: 20 cores, 40GB RAM
```

**Production Recommendations:**
```yaml
Deployment Mode: YARN or Kubernetes
Worker Nodes: 10-20 (16 cores, 64GB RAM each)
Total Resources: 160-320 cores, 640-1280GB RAM
Dynamic Allocation: Enabled
```

#### Spark Configuration

```python
# Optimized Spark Configuration
spark_config = {
    # Application
    "spark.app.name": "goodnote-analytics",

    # Execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",

    # Shuffle
    "spark.sql.shuffle.partitions": "2000",  # Tuned for 1TB dataset
    "spark.sql.shuffle.compression.codec": "lz4",
    "spark.shuffle.service.enabled": "true",

    # Memory
    "spark.executor.memory": "16g",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "8g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "4g",

    # Parallelism
    "spark.default.parallelism": "400",
    "spark.sql.files.maxPartitionBytes": "128MB",

    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max": "512m",

    # I/O
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.parquet.mergeSchema": "false",

    # Broadcast
    "spark.sql.autoBroadcastJoinThreshold": "10MB",

    # History
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "/opt/spark/logs",
    "spark.history.fs.logDirectory": "/opt/spark/logs",
}
```

#### ETL Job Pipeline

**Job 1: Data Processing (01_data_processing.py)**
- **Input:** Raw CSV files
- **Processing:**
  - Read with schema enforcement
  - Data quality validation
  - Optimized join (broadcast or salted)
  - Column pruning and filtering
- **Output:** Parquet files partitioned by date and country
- **Runtime:** ~30 minutes (1TB dataset)

**Job 2: User Engagement (02_user_engagement.py)**
- **Input:** Processed Parquet
- **Processing:**
  - DAU/MAU calculations
  - Power user identification (top 1%)
  - Cohort retention analysis
- **Output:** PostgreSQL tables
- **Runtime:** ~20 minutes

**Job 3: Performance Metrics (03_performance_metrics.py)**
- **Input:** Processed Parquet
- **Processing:**
  - Percentile calculations (P95, P99)
  - Device-performance correlation
  - Anomaly detection
- **Output:** PostgreSQL tables
- **Runtime:** ~15 minutes

**Job 4: Session Analysis (04_session_analysis.py)**
- **Input:** Processed Parquet
- **Processing:**
  - Sessionization (30-minute window)
  - Session metrics aggregation
  - Bounce rate calculation
- **Output:** PostgreSQL tables
- **Runtime:** ~25 minutes

**Total Pipeline Runtime:** ~90 minutes for full dataset

---

### 3. Storage Layer

#### Intermediate Storage (Parquet)

**Directory Structure:**
```
data/
├── raw/
│   ├── interactions/
│   │   ├── date=2023-01-01/
│   │   │   └── interactions.csv
│   │   ├── date=2023-01-02/
│   │   └── ...
│   └── metadata/
│       ├── country=US/
│       │   └── metadata.csv
│       ├── country=UK/
│       └── ...
├── processed/
│   └── interactions_enriched/
│       ├── date=2023-01-01/
│       │   ├── country=US/
│       │   │   └── part-00000.parquet
│       │   ├── country=UK/
│       │   └── ...
│       └── ...
└── results/
    └── [CSV exports for validation]
```

**Parquet Configuration:**
- **Compression:** Snappy (good balance of speed and compression)
- **Row Group Size:** 128 MB
- **Page Size:** 1 MB
- **Encoding:** Dictionary + RLE for strings, Delta for integers

#### Analytics Database (PostgreSQL)

**Database Configuration:**
```sql
-- PostgreSQL 15 Configuration
-- postgresql.conf optimizations

-- Memory
shared_buffers = 8GB
effective_cache_size = 24GB
work_mem = 256MB
maintenance_work_mem = 2GB

-- Parallelism
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
max_worker_processes = 8

-- Query Planning
random_page_cost = 1.1  # For SSD storage
effective_io_concurrency = 200

-- WAL
wal_buffers = 16MB
checkpoint_completion_target = 0.9
```

**Schema Design:**

```sql
-- Time-series tables (append-only, range partitioned)
CREATE TABLE goodnote_analytics.daily_active_users (
    date DATE NOT NULL,
    dau BIGINT,
    new_users BIGINT,
    returning_users BIGINT,
    total_interactions BIGINT,
    avg_duration_per_user DOUBLE PRECISION
) PARTITION BY RANGE (date);

-- Create monthly partitions
CREATE TABLE daily_active_users_2023_01 PARTITION OF daily_active_users
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

-- Indexes for fast queries
CREATE INDEX idx_dau_date ON goodnote_analytics.daily_active_users (date);

-- Fact tables (updated daily)
CREATE TABLE goodnote_analytics.power_users (
    user_id VARCHAR(50) PRIMARY KEY,
    total_duration_ms BIGINT,
    total_interactions BIGINT,
    percentile_rank DOUBLE PRECISION,
    country VARCHAR(10),
    device_type VARCHAR(50),
    subscription_type VARCHAR(20)
);

-- Composite indexes for filtering
CREATE INDEX idx_power_users_country_device
    ON goodnote_analytics.power_users (country, device_type);
```

**Data Retention Policy:**
- **Raw CSV:** 30 days (archived to S3 after)
- **Processed Parquet:** 90 days (queryable)
- **PostgreSQL:** 2 years (partitioned by month)
- **Superset Cache:** 5 minutes (Redis TTL)

---

### 4. Visualization Layer (Apache Superset)

#### Superset Architecture

**Components:**
- **Web Server:** Gunicorn with 4 workers
- **Metadata Database:** PostgreSQL (separate from analytics DB)
- **Cache:** Redis with 1GB memory limit
- **Async Query Execution:** Celery workers (optional)

**Configuration:**
```python
# superset_config.py

# Database Connections
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:password@postgres:5432/superset'

# Analytics Database (read-only)
ANALYTICS_DB_URI = 'postgresql://analytics_user:password@postgres:5432/analytics'

# Redis Cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,  # 5 minutes
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis:6379/0'
}

# Query Performance
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
ROW_LIMIT = 50000

# Security
WTF_CSRF_ENABLED = True
SECRET_KEY = 'your-secret-key-here'

# Feature Flags
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
```

#### Dashboard Design

**Dashboard 1: Executive Overview**
- **Purpose:** High-level KPIs for leadership
- **Charts:** 7 charts (KPI cards, time-series, geo map)
- **Refresh:** Every 1 hour
- **Filters:** Date range, country

**Dashboard 2: User Engagement**
- **Purpose:** Deep-dive into user behavior
- **Charts:** 8 charts (cohort heatmap, power users table, histograms)
- **Refresh:** Every 6 hours
- **Filters:** Date range, country, device type, subscription

**Dashboard 3: Performance Monitoring**
- **Purpose:** App performance tracking
- **Charts:** 6 charts (time-series, bar charts, anomaly table)
- **Refresh:** Every 30 minutes
- **Filters:** Date range, app version, device type

**Dashboard 4: Session Analytics**
- **Purpose:** Session behavior analysis
- **Charts:** 9 charts (treemap, stacked area, bubble chart)
- **Refresh:** Every 6 hours
- **Filters:** Date range, country, device type

---

## Data Flow

### End-to-End Pipeline

```
1. Data Generation (Scripts)
   ↓ [CSV Files: 1TB interactions, 100GB metadata]

2. Data Ingestion (Spark Job 1)
   ↓ [Read CSV → Validate → Transform → Write Parquet]

3. Data Processing (Spark Jobs 2-4)
   ↓ [Read Parquet → Aggregate → Calculate Metrics]

4. Data Storage (PostgreSQL)
   ↓ [Write via JDBC → 10 analytics tables]

5. Data Visualization (Superset)
   ↓ [Query PostgreSQL → Render Charts → Cache Results]

6. End User Consumption
   ↓ [Interactive Dashboards, SQL Lab, Scheduled Reports]
```

### Data Refresh Strategy

**Batch Processing (Daily):**
```
00:00 - Data generation for previous day
01:00 - Spark Job 1: Data processing (30 min)
01:30 - Spark Job 2: User engagement (20 min)
01:50 - Spark Job 3: Performance metrics (15 min)
02:05 - Spark Job 4: Session analysis (25 min)
02:30 - Data quality checks
03:00 - Superset cache warm-up
```

**Real-Time Processing (Future Enhancement):**
- Spark Structured Streaming
- Kafka message queue
- 5-minute micro-batches
- Incremental updates to PostgreSQL

---

## Deployment Strategy

### Local Development (Docker Compose)

```yaml
# compose.yml
version: '3.8'

services:
  spark-master:
    image: bitnami/spark:3.5
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
      - "7077:7077"

  spark-worker:
    image: bitnami/spark:3.5
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master

  postgres:
    image: postgres:15-alpine
    environment:
      - POSTGRES_DB=analytics
    ports:
      - "5432:5432"

  superset:
    image: apache/superset:3.0.0
    ports:
      - "8088:8088"
    depends_on:
      - postgres
      - redis

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  jupyter:
    image: jupyter/pyspark-notebook:spark-3.5.0
    ports:
      - "8888:8888"
```

### Production Deployment (Kubernetes)

**Architecture:**
- **Spark:** Deployed on Kubernetes using Spark Operator
- **PostgreSQL:** Managed service (AWS RDS, Google Cloud SQL)
- **Superset:** Kubernetes deployment with Helm chart
- **Redis:** Managed service (ElastiCache, MemoryStore)
- **Orchestration:** Apache Airflow for job scheduling

**Scalability:**
- Horizontal scaling: Add more Spark workers during peak hours
- Vertical scaling: Increase executor memory for larger datasets
- Auto-scaling: Based on job queue length and SLA requirements

---

## Monitoring and Observability

### Spark Monitoring

**Built-in Tools:**
- **Spark UI (4040):** Real-time job monitoring
- **Spark Master UI (8080):** Cluster status
- **History Server (18080):** Historical job analysis

**Key Metrics:**
- Job completion time
- Stage duration and skew
- Task duration distribution
- Shuffle read/write volume
- GC time percentage
- Executor memory usage
- Spilled memory/disk

### Database Monitoring

**PostgreSQL Metrics:**
- Query execution time (pg_stat_statements)
- Table bloat and index usage
- Connection pool usage
- Cache hit ratio (should be >95%)
- Transaction throughput

**Tools:**
- pgAdmin for database management
- pg_stat_activity for active queries
- EXPLAIN ANALYZE for query optimization

### Application Monitoring

**Superset Metrics:**
- Dashboard load time
- Query execution time
- Cache hit rate
- User session count
- Error rate

**Tools:**
- Prometheus for metrics collection
- Grafana for visualization
- Alertmanager for alerting

---

## Security Considerations

### Data Security
- **Encryption at Rest:** PostgreSQL transparent data encryption (TDE)
- **Encryption in Transit:** TLS/SSL for all connections
- **Data Masking:** PII fields masked in non-production environments

### Access Control
- **Spark:** LDAP/AD integration for authentication
- **PostgreSQL:** Role-based access control (RBAC)
- **Superset:** Row-level security for multi-tenancy

### Network Security
- **Firewalls:** Restrict access to ports (only necessary ports exposed)
- **VPN:** Required for production access
- **API Keys:** Rotate every 90 days

---

## Disaster Recovery

### Backup Strategy
- **PostgreSQL:** Daily full backups + continuous WAL archiving
- **Parquet Files:** Replicated to S3 with versioning
- **Superset Metadata:** Weekly backups of dashboard configs

### Recovery Time Objectives (RTO)
- **Database Restore:** < 4 hours
- **Spark Cluster Rebuild:** < 1 hour
- **Superset Restore:** < 30 minutes

### Recovery Point Objectives (RPO)
- **Analytics Data:** 24 hours (daily batch)
- **Metadata:** 1 week (acceptable for dashboards)

---

## Cost Optimization

### Compute Costs
- **Spark:** Use spot instances for non-critical jobs (60-90% savings)
- **Idle Time:** Auto-shutdown Spark cluster during off-hours

### Storage Costs
- **Parquet:** 3-5x compression compared to CSV
- **PostgreSQL:** Partition pruning reduces query costs
- **Archival:** Move old data to cheaper storage (S3 Glacier)

### Network Costs
- **Data Locality:** Process data where it's stored (avoid egress)
- **Compression:** Enable compression for all data transfers

---

## Performance Benchmarks

### Expected Performance (1TB Dataset)

| Job | Runtime (Dev) | Runtime (Prod) | Optimization |
|-----|---------------|----------------|--------------|
| Data Processing | 45 min | 30 min | Broadcast join + AQE |
| User Engagement | 30 min | 20 min | Salting for skew |
| Performance Metrics | 20 min | 15 min | Approx percentiles |
| Session Analysis | 35 min | 25 min | Window optimization |
| **Total** | **130 min** | **90 min** | **31% improvement** |

### Query Performance (Superset)

| Query Type | Response Time | Optimization |
|------------|---------------|--------------|
| Simple Aggregation | < 2 sec | Indexed columns |
| Time-Series | < 5 sec | Partition pruning |
| Complex Join | < 10 sec | Materialized views |
| Ad-hoc Exploration | < 30 sec | Query cache |

---

## Appendices

### A. Technology Versions
- Apache Spark: 3.5.0
- Python: 3.9+
- PostgreSQL: 15.x
- Apache Superset: 3.0.0
- Redis: 7.x
- Docker: 24.x
- Docker Compose: 2.x

### B. Hardware Requirements

**Development:**
- CPU: 8+ cores
- RAM: 32+ GB
- Storage: 500+ GB SSD
- Network: 100 Mbps+

**Production:**
- CPU: 160+ cores (distributed)
- RAM: 640+ GB (distributed)
- Storage: 10+ TB SSD
- Network: 10 Gbps+

### C. External Dependencies
- JDBC Driver: PostgreSQL JDBC 42.6.0
- Python Libraries: See requirements.txt
- Spark Packages: None (using built-in libraries)

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Author:** Claude (Senior Data Engineer)
