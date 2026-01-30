# Database Index Creation Decisions

This document provides a step-by-step explanation of the indexing strategy in `database/02_indexes.sql` for the GoodNote Analytics Platform.

---

## Table of Contents

1. [Overview](#overview)
2. [Engagement Metrics Indexes](#1-engagement-metrics-indexes)
3. [Performance Metrics Indexes](#2-performance-metrics-indexes)
4. [Session Analysis Indexes](#3-session-analysis-indexes)
5. [Raw/Staging Tables Indexes](#4-rawstaging-tables-indexes)
6. [Job Monitoring Indexes](#5-job-monitoring-indexes)
7. [Dashboard-Specific Optimizations](#6-dashboard-specific-optimizations)
8. [Maintenance Indexes](#7-maintenance-indexes)
9. [Trade-offs and Considerations](#trade-offs-and-considerations)

---

## Overview

### Why Indexes Matter

Indexes accelerate query performance by allowing PostgreSQL to locate rows without scanning entire tables. The trade-off is:
- **Benefit**: Faster SELECT queries, especially with WHERE, ORDER BY, and JOIN clauses
- **Cost**: Increased storage, slower INSERT/UPDATE/DELETE operations

### Naming Convention

All indexes follow the pattern: `idx_<table>_<column(s)>` for clarity and maintainability.

### Design Principles Applied

1. **Index columns used in WHERE clauses** - Filter predicates benefit most from indexes
2. **Index columns used in ORDER BY** - Avoids expensive sort operations
3. **Use DESC for time-series data** - Most queries fetch recent data first
4. **Composite indexes for multi-column filters** - One index serves multiple conditions
5. **Partial indexes for subset queries** - Reduce index size for specific use cases

---

## 1. Engagement Metrics Indexes

### Daily Active Users (`daily_active_users`)

```sql
CREATE INDEX IF NOT EXISTS idx_dau_created_at ON daily_active_users(created_at);
```

**Decision Rationale:**
- Primary key on `date` already provides an index for date-based lookups
- `created_at` index supports ETL auditing queries (e.g., "find records created in the last hour")
- Enables efficient data reconciliation between pipeline runs

### Monthly Active Users (`monthly_active_users`)

```sql
CREATE INDEX IF NOT EXISTS idx_mau_created_at ON monthly_active_users(created_at);
```

**Decision Rationale:**
- Same reasoning as DAU - supports ETL pipeline monitoring
- Tracks when records were inserted for debugging data freshness issues

### User Stickiness (`user_stickiness`)

```sql
CREATE INDEX IF NOT EXISTS idx_stickiness_year_month ON user_stickiness(year_month);
CREATE INDEX IF NOT EXISTS idx_stickiness_ratio ON user_stickiness(stickiness DESC);
```

**Decision Rationale:**
- `year_month` index: Enables monthly aggregation queries without primary key dependency
- `stickiness DESC` index: Optimizes "top N stickiest days" queries for dashboards
- DESC ordering pre-sorts data for ranking queries, avoiding runtime sorts

### Power Users (`power_users`)

```sql
CREATE INDEX IF NOT EXISTS idx_power_users_country ON power_users(country);
CREATE INDEX IF NOT EXISTS idx_power_users_device ON power_users(device_type);
CREATE INDEX IF NOT EXISTS idx_power_users_subscription ON power_users(subscription_type);
CREATE INDEX IF NOT EXISTS idx_power_users_duration_hours ON power_users(total_duration_hours DESC);
CREATE INDEX IF NOT EXISTS idx_power_users_interactions ON power_users(total_interactions DESC);
```

**Decision Rationale:**
- `country`, `device_type`, `subscription_type`: Support segmentation filters in dashboards (e.g., "power users in Germany on iPad")
- `total_duration_hours DESC`, `total_interactions DESC`: Enable efficient ranking queries ("top 100 users by engagement")
- Multiple single-column indexes allow PostgreSQL to choose the best index per query or combine via bitmap index scans

### Cohort Retention (`cohort_retention`)

```sql
CREATE INDEX IF NOT EXISTS idx_cohort_week ON cohort_retention(cohort_week);
CREATE INDEX IF NOT EXISTS idx_cohort_retention_rate ON cohort_retention(retention_rate DESC);
```

**Decision Rationale:**
- `cohort_week` index: Supports filtering by specific cohort start dates
- `retention_rate DESC`: Optimizes queries finding best/worst performing cohorts

---

## 2. Performance Metrics Indexes

### Performance by Version (`performance_by_version`)

```sql
CREATE INDEX IF NOT EXISTS idx_perf_version_date ON performance_by_version(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_perf_version_p99 ON performance_by_version(p99_duration_ms DESC);
CREATE INDEX IF NOT EXISTS idx_perf_version_created ON performance_by_version(created_at);
```

**Decision Rationale:**
- `metric_date DESC`: Time-series queries typically fetch recent data first
- `p99_duration_ms DESC`: Identifies slowest versions quickly for SLA monitoring
- `created_at`: ETL auditing (when was this data generated?)

### Device Performance (`device_performance`)

```sql
CREATE INDEX IF NOT EXISTS idx_device_perf_date ON device_performance(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_device_perf_p99 ON device_performance(p99_duration_ms DESC);
```

**Decision Rationale:**
- Same pattern as version performance - optimizes time-series and ranking queries
- Enables "which device types are underperforming this week?" queries

### Performance Anomalies (`performance_anomalies`)

```sql
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON performance_anomalies(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_metric_date ON performance_anomalies(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON performance_anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_app_version ON performance_anomalies(app_version);
CREATE INDEX IF NOT EXISTS idx_anomalies_device_type ON performance_anomalies(device_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_z_score ON performance_anomalies(z_score DESC);
```

**Decision Rationale:**
- `detected_at DESC`, `metric_date DESC`: Recent anomalies are most relevant for alerting
- `severity`: Enables filtering by severity level (critical, high, medium, low)
- `app_version`, `device_type`: Supports root cause analysis by segmentation
- `z_score DESC`: Quickly finds statistically most significant anomalies

---

## 3. Session Analysis Indexes

### Session Metrics (`session_metrics`)

```sql
CREATE INDEX IF NOT EXISTS idx_session_user_id ON session_metrics(user_id);
CREATE INDEX IF NOT EXISTS idx_session_start_time ON session_metrics(session_start_time DESC);
CREATE INDEX IF NOT EXISTS idx_session_device_type ON session_metrics(device_type);
CREATE INDEX IF NOT EXISTS idx_session_country ON session_metrics(country);
CREATE INDEX IF NOT EXISTS idx_session_is_bounce ON session_metrics(is_bounce);
CREATE INDEX IF NOT EXISTS idx_session_duration ON session_metrics(session_duration_ms DESC);
CREATE INDEX IF NOT EXISTS idx_session_actions_count ON session_metrics(actions_count DESC);
```

**Decision Rationale:**
- `user_id`: Essential for "show all sessions for user X" queries
- `session_start_time DESC`: Time-series analysis of session trends
- `device_type`, `country`: Common segmentation dimensions
- `is_bounce`: Boolean index for bounce rate calculations
- `duration DESC`, `actions_count DESC`: Ranking queries for engagement analysis

### Composite Indexes for Session Metrics

```sql
CREATE INDEX IF NOT EXISTS idx_session_device_bounce ON session_metrics(device_type, is_bounce);
CREATE INDEX IF NOT EXISTS idx_session_country_bounce ON session_metrics(country, is_bounce);
```

**Decision Rationale:**
- **Composite indexes optimize multi-column filters**: A query like `WHERE device_type = 'iPad' AND is_bounce = true` benefits from a single index scan
- **Column order matters**: Most selective column (device_type/country) comes first
- Avoids multiple index scans and bitmap heap operations

### Bounce Rates (`bounce_rates`)

```sql
CREATE INDEX IF NOT EXISTS idx_bounce_metric_date ON bounce_rates(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_bounce_dimension ON bounce_rates(dimension_name, dimension_value);
CREATE INDEX IF NOT EXISTS idx_bounce_rate_value ON bounce_rates(bounce_rate DESC);
```

**Decision Rationale:**
- `metric_date DESC`: Time-series dashboard queries
- `(dimension_name, dimension_value)`: Composite index for dimension-based filtering (e.g., "bounce rate for device_type=iPad")
- `bounce_rate DESC`: Identifies worst-performing segments

---

## 4. Raw/Staging Tables Indexes

### User Interactions (`user_interactions`)

```sql
CREATE INDEX IF NOT EXISTS idx_interactions_user_id ON user_interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_timestamp ON user_interactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_interactions_action_type ON user_interactions(action_type);
CREATE INDEX IF NOT EXISTS idx_interactions_session_id ON user_interactions(session_id);
CREATE INDEX IF NOT EXISTS idx_interactions_app_version ON user_interactions(app_version);
```

**Decision Rationale:**
- This is a high-volume raw events table - indexes are critical for ETL performance
- `user_id`, `session_id`: JOINs to user/session tables
- `timestamp DESC`: Time-range filtering for incremental processing
- `action_type`: Aggregation by event type
- `app_version`: Version-specific analysis

### Composite Index for User Timeline

```sql
CREATE INDEX IF NOT EXISTS idx_interactions_user_time ON user_interactions(user_id, timestamp DESC);
```

**Decision Rationale:**
- **Optimizes a common query pattern**: "Get events for user X ordered by time"
- Single index satisfies both the WHERE (user_id) and ORDER BY (timestamp DESC)
- Avoids a separate sort operation after filtering

### User Metadata (`user_metadata`)

```sql
CREATE INDEX IF NOT EXISTS idx_metadata_country ON user_metadata(country);
CREATE INDEX IF NOT EXISTS idx_metadata_device ON user_metadata(device_type);
CREATE INDEX IF NOT EXISTS idx_metadata_subscription ON user_metadata(subscription_type);
CREATE INDEX IF NOT EXISTS idx_metadata_registration_date ON user_metadata(registration_date);
```

**Decision Rationale:**
- Dimension table used for JOINs in analytics queries
- Each index supports a common filter dimension
- `registration_date`: Enables cohort analysis by signup date

---

## 5. Job Monitoring Indexes

### ETL Job Runs (`etl_job_runs`)

```sql
CREATE INDEX IF NOT EXISTS idx_job_runs_name ON etl_job_runs(job_name);
CREATE INDEX IF NOT EXISTS idx_job_runs_type ON etl_job_runs(job_type);
CREATE INDEX IF NOT EXISTS idx_job_runs_start_time ON etl_job_runs(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON etl_job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_created ON etl_job_runs(created_at DESC);
```

**Decision Rationale:**
- `job_name`, `job_type`: Filter by specific jobs or job categories
- `start_time DESC`, `created_at DESC`: Find recent job runs for monitoring
- `status`: Filter by success/failure for alerting dashboards

---

## 6. Dashboard-Specific Optimizations

These indexes are tailored to specific Superset dashboard query patterns.

### Dashboard 1: Executive Overview

```sql
CREATE INDEX IF NOT EXISTS idx_dau_date_desc ON daily_active_users(date DESC);
CREATE INDEX IF NOT EXISTS idx_mau_yearmonth_desc ON monthly_active_users(year_month DESC);
```

**Decision Rationale:**
- Executive dashboards show recent trends first
- DESC ordering eliminates the need for runtime sorting
- The primary key index doesn't guarantee DESC ordering

### Dashboard 2: User Engagement Deep Dive

```sql
CREATE INDEX IF NOT EXISTS idx_cohort_week_weeknum ON cohort_retention(cohort_week, week_number);
```

**Decision Rationale:**
- Cohort heatmaps query by (cohort_week, week_number) pairs
- Composite index serves the exact query pattern
- Note: The primary key may already cover this, but explicit index ensures optimization

### Dashboard 3: Performance Monitoring

```sql
CREATE INDEX IF NOT EXISTS idx_perf_version_date_composite ON performance_by_version(app_version, metric_date DESC);
```

**Decision Rationale:**
- Queries filter by app_version then sort by date
- Composite index satisfies both conditions in one index scan
- Particularly useful for "compare version X over time" charts

### Dashboard 4: Session Analytics

```sql
CREATE INDEX IF NOT EXISTS idx_bounce_device_date ON bounce_rates(metric_date DESC, dimension_value)
    WHERE dimension_name = 'device_type';
```

**Decision Rationale:**
- **Partial index**: Only indexes rows where `dimension_name = 'device_type'`
- Smaller index size compared to a full index
- Optimizes the specific query pattern for device bounce rate analysis
- PostgreSQL will only use this index when the WHERE clause matches

---

## 7. Maintenance Indexes

### Archival Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_dau_created_archival ON daily_active_users(created_at);
CREATE INDEX IF NOT EXISTS idx_sessions_created_archival ON session_metrics(created_at);
```

**Decision Rationale:**
- Support data retention policies (e.g., "delete records older than 90 days")
- `created_at` index enables efficient deletion of old data
- Note: Partial indexes with CURRENT_DATE are not possible (CURRENT_DATE is not immutable)
- Regular indexes on created_at still optimize `WHERE created_at < some_date` queries

---

## Trade-offs and Considerations

### Storage Overhead

Each index consumes disk space proportional to the indexed columns. Monitor index sizes with:

```sql
SELECT indexname, pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;
```

### Write Performance Impact

- Every INSERT, UPDATE, DELETE must update all indexes on the table
- High-volume tables (like `user_interactions`) should balance index count against write throughput
- Consider batch loading with indexes disabled, then rebuilding

### Index Maintenance

- Run `VACUUM ANALYZE` after bulk data loads (commented out in script)
- Monitor unused indexes with the provided monitoring queries
- Remove indexes with zero or very low `idx_scan` counts

### Composite Index Column Order

Column order in composite indexes matters:
1. **Equality columns first**: Columns used with `=` should come before range columns
2. **Most selective first**: High-cardinality columns before low-cardinality
3. **ORDER BY columns last**: If query sorts by a column, include it at the end

### When to Consider Additional Indexes

- Query explains show sequential scans on large tables
- Dashboard queries have high latency
- New filter dimensions are added to dashboards

### When to Remove Indexes

- `idx_scan = 0` in `pg_stat_user_indexes` after significant runtime
- Duplicate indexes covering same columns
- Write-heavy tables where read optimization is less critical

---

## Summary

| Index Category | Purpose | Key Patterns |
|----------------|---------|--------------|
| Engagement | Dashboard filters, rankings | Segmentation, DESC ordering |
| Performance | Anomaly detection, version comparison | Time-series, P99 ranking |
| Session | User journey analysis | Composite filters, bounce rate |
| Raw/Staging | ETL processing | JOINs, time-range filtering |
| Job Monitoring | Pipeline observability | Status filtering, recency |
| Dashboard-specific | Query optimization | Exact query pattern matching |
| Maintenance | Data lifecycle | Archival date ranges |

This indexing strategy balances query performance against storage and write overhead, with specific attention to the analytics dashboard query patterns used in the GoodNote Analytics Platform.
