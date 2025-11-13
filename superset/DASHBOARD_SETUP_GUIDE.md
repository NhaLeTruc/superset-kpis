# Apache Superset Dashboard Setup Guide

This guide provides step-by-step instructions for setting up and importing the GoodNote Analytics dashboards into Apache Superset.

## 📋 Table of Contents
- [Prerequisites](#prerequisites)
- [Database Connection Setup](#database-connection-setup)
- [Dashboard Import](#dashboard-import)
- [Manual Dashboard Creation](#manual-dashboard-creation)
- [Dashboard Overview](#dashboard-overview)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software
- Apache Superset 3.0+ (running and accessible)
- PostgreSQL 15+ with analytics data
- Access to Superset admin account

### Required Data
Ensure the following tables are populated in PostgreSQL:
- `daily_active_users`
- `monthly_active_users`
- `user_stickiness`
- `power_users`
- `cohort_retention`
- `performance_by_version`
- `device_performance`
- `performance_anomalies`
- `session_metrics`
- `bounce_rates`

---

## Database Connection Setup

### 1. Access Superset Admin Panel
Navigate to: **Data** → **Databases** → **+ Database**

### 2. Configure PostgreSQL Connection

**Connection Details:**
```
Database Name: GoodNote Analytics
SQLAlchemy URI: postgresql+psycopg2://USER:PASSWORD@HOST:PORT/goodnote_analytics
```

**Example:**
```
postgresql+psycopg2://postgres:password@localhost:5432/goodnote_analytics
```

### 3. Test Connection
Click **"Test Connection"** to verify connectivity.

### 4. Advanced Settings (Optional)
```json
{
  "metadata_params": {},
  "engine_params": {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_recycle": 3600
  },
  "metadata_cache_timeout": {},
  "schemas_allowed_for_file_upload": []
}
```

### 5. Save Database Connection

---

## Dataset Creation

### 1. Create Datasets for Each Table

Navigate to: **Data** → **Datasets** → **+ Dataset**

**Required Datasets:**
1. `daily_active_users`
2. `monthly_active_users`
3. `user_stickiness`
4. `power_users`
5. `cohort_retention`
6. `performance_by_version`
7. `device_performance`
8. `performance_anomalies`
9. `session_metrics`
10. `bounce_rates`

**For each dataset:**
- **Database:** GoodNote Analytics
- **Schema:** public
- **Table:** [select table name]
- Click **Add** and **Save**

### 2. Configure Temporal Columns

For time-series charts, mark temporal columns:
- `daily_active_users.date` → Date (D)
- `user_stickiness.date` → Date (D)
- `session_metrics.session_start_time` → Timestamp (T)
- `bounce_rates.metric_date` → Date (D)
- `performance_by_version.metric_date` → Date (D)

---

## Dashboard Import

### Method 1: Using Dashboard JSON Files

**Note:** Superset's import/export format may vary by version. The JSON files provided are dashboard specifications.

1. Navigate to: **Dashboards** → **Import Dashboard**
2. Upload the JSON file
3. Map datasets to existing ones
4. Click **Import**

### Method 2: Using Superset CLI

```bash
superset import-dashboards -p /path/to/dashboard_file.json
```

---

## Manual Dashboard Creation

If automatic import doesn't work, create dashboards manually using the specifications below.

### Dashboard 1: Executive Overview

**Create Dashboard:**
1. Navigate to: **Dashboards** → **+ Dashboard**
2. Name: "Executive Overview - GoodNote Analytics"
3. Slug: `executive-overview`

**Add Charts:**

#### Chart 1: DAU/MAU Time Series
- **Visualization:** Line Chart
- **Dataset:** `daily_active_users`
- **Metrics:** `dau`
- **Time Column:** `date`
- **Time Range:** Last 90 days
- **SQL:**
```sql
SELECT date, dau, total_interactions
FROM daily_active_users
WHERE date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY date
```

#### Chart 2: Stickiness Ratio Gauge
- **Visualization:** Gauge Chart
- **Dataset:** `user_stickiness`
- **Metric:** `AVG(stickiness)`
- **Time Range:** Last 7 days
- **SQL:**
```sql
SELECT AVG(stickiness) as avg_stickiness
FROM user_stickiness
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
```

#### Chart 3: Geographic Heatmap
- **Visualization:** Country Map
- **Dataset:** `power_users`
- **Entity:** `country`
- **Metric:** `COUNT(*)`
- **SQL:**
```sql
SELECT country, COUNT(*) as user_count
FROM power_users
GROUP BY country
ORDER BY user_count DESC
```

#### Chart 4: Top 10 Countries
- **Visualization:** Bar Chart
- **Dataset:** `power_users`
- **Group by:** `country`
- **Metric:** `COUNT(*)`
- **Row Limit:** 10

#### Chart 5: MAU Trend
- **Visualization:** Line Chart
- **Dataset:** `monthly_active_users`
- **Metrics:** `mau`
- **X-axis:** `year_month`
- **Time Range:** Last 12 months

#### Chart 6: Engagement Summary
- **Visualization:** Table
- **Dataset:** `daily_active_users`
- **SQL:**
```sql
SELECT
  AVG(dau) as avg_daily_active_users,
  AVG(total_interactions) as avg_daily_interactions,
  AVG(total_duration_ms)/1000/60 as avg_minutes_per_day
FROM daily_active_users
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
```

#### Chart 7: Stickiness Trend
- **Visualization:** Line Chart
- **Dataset:** `user_stickiness`
- **Metrics:** `stickiness`
- **Time Column:** `date`
- **Y-axis Format:** `.2%`

**Configure Filters:**
- Date Range filter on `date` column

**Set Refresh:**
- Auto-refresh: Every 1 hour
- Crontab: `0 * * * *`

---

### Dashboard 2: User Engagement Deep Dive

**Charts:**
1. **Cohort Retention Heatmap** (Heatmap)
2. **Power Users Table** (Table)
3. **Engagement Distribution** (Histogram)
4. **Retention by Cohort** (Line)
5. **Power Users by Country** (Pie)
6. **Power Users by Device** (Pie)
7. **Week 0 vs Week 12 Retention** (Big Number)
8. **Engagement by Subscription** (Bar)

**Filters:**
- Country (Select)
- Device Type (Select)
- Subscription Type (Select)

**Refresh:** Every 6 hours

---

### Dashboard 3: Performance Monitoring

**Charts:**
1. **P95 Load Time by Version** (Line)
2. **Device Performance Comparison** (Bar)
3. **Performance Anomalies** (Table)
4. **P99 vs P50 Gap** (Line)
5. **Critical Anomalies Count** (Big Number)
6. **Performance Heatmap** (Heatmap)

**Filters:**
- App Version (Select)
- Date Range (Time)
- Severity (Select)

**Refresh:** Every 30 minutes

---

### Dashboard 4: Session Analytics

**Charts:**
1. **Session Duration Treemap** (Treemap)
2. **Action Distribution** (Pie)
3. **Bounce Rate by Device** (Bar)
4. **Overall Bounce Rate** (Gauge)
5. **Bounce Rate Trend** (Line)
6. **Avg Session Duration by Device** (Bar)
7. **Bounce Rate by Country** (Country Map)
8. **Session Metrics Summary** (Table)
9. **Actions per Session Distribution** (Histogram)

**Filters:**
- Date Range (Time)
- Device Type (Select)
- Country (Select)

**Refresh:** Every 6 hours

---

## Dashboard Configuration

### Setting Auto-Refresh

1. Open Dashboard
2. Click **⋮** (three dots) → **Edit Dashboard**
3. In **Properties**, set:
   - **Auto Refresh Interval:** [seconds]
   - **Refresh Frequency:** [crontab expression]

**Crontab Examples:**
- Every hour: `0 * * * *`
- Every 30 minutes: `*/30 * * * *`
- Every 6 hours: `0 */6 * * *`

### Setting Default Filters

1. Edit Dashboard
2. Add **Filter Box** component
3. Configure filters for each column
4. Set default values
5. Save

### Dashboard Permissions

Navigate to: **Dashboards** → [Dashboard Name] → **Manage Access**

**Roles:**
- **Admin:** Full access
- **Analyst:** View + Edit
- **Viewer:** View only

---

## Verification Checklist

After setup, verify:

- [ ] All 4 dashboards are created
- [ ] All charts display data (no errors)
- [ ] Filters work correctly
- [ ] Cross-filtering between charts works
- [ ] Auto-refresh is configured
- [ ] Queries respond in <5 seconds
- [ ] Permissions are correctly set
- [ ] Color schemes are consistent

---

## Troubleshooting

### Issue: No Data Showing

**Solution:**
1. Verify tables have data: `SELECT COUNT(*) FROM table_name;`
2. Check date filters (may be outside data range)
3. Verify dataset refresh: **Data** → **Datasets** → [Dataset] → **Sync Columns from Source**

### Issue: Slow Query Performance

**Solution:**
1. Check PostgreSQL indexes are created (`database/indexes.sql`)
2. Run `VACUUM ANALYZE` on tables
3. Increase cache timeout on charts
4. Consider materialized views for complex queries

### Issue: Chart Not Rendering

**Solution:**
1. Check SQL syntax in chart editor
2. Verify column names match dataset
3. Clear browser cache
4. Check Superset logs: `tail -f /var/log/superset/superset.log`

### Issue: Import Fails

**Solution:**
1. Ensure Superset version compatibility
2. Create datasets manually first
3. Import dashboards one at a time
4. Check for duplicate dashboard slugs

---

## Performance Optimization

### Enable Query Result Caching

**superset_config.py:**
```python
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': 'localhost',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_DEFAULT_TIMEOUT': 3600,
    'CACHE_KEY_PREFIX': 'superset_'
}
```

### Enable Async Query Execution

```python
RESULTS_BACKEND = 'redis://localhost:6379/0'
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600
```

### Optimize Postgres Connections

```python
SQLALCHEMY_POOL_SIZE = 10
SQLALCHEMY_MAX_OVERFLOW = 20
SQLALCHEMY_POOL_RECYCLE = 3600
```

---

## Monitoring Dashboard Usage

### View Dashboard Analytics

Navigate to: **Charts** → **Chart** → **View Usage**

Metrics:
- View count
- Last viewed
- Average query time
- Cache hit rate

---

## Next Steps

1. ✅ Complete database setup
2. ✅ Import all 4 dashboards
3. ✅ Configure auto-refresh
4. ✅ Set up user permissions
5. ✅ Share dashboard links with stakeholders
6. ⏭️ Set up alerts for critical metrics
7. ⏭️ Create scheduled email reports

---

## Support & Resources

- **Superset Documentation:** https://superset.apache.org/docs/intro
- **SQL Lab:** Use for testing queries before creating charts
- **Slack Channel:** [Your team channel]
- **Admin Contact:** [Admin email]

---

**Last Updated:** 2025-11-13
**Version:** 1.0
**Dashboards:** 4 (30+ charts total)
