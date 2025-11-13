-- ============================================
-- GoodNote Analytics Platform - Database Schema
-- ============================================
-- PostgreSQL 15+ Schema Definition
-- Purpose: Store analytics results from Spark jobs
-- Created: 2025-11-13
-- ============================================

-- Enable UUID extension for potential use
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- 1. ENGAGEMENT METRICS TABLES
-- ============================================

-- Daily Active Users (DAU)
-- Stores daily active user counts and interaction summaries
CREATE TABLE IF NOT EXISTS daily_active_users (
    date DATE NOT NULL,
    dau INTEGER NOT NULL,
    total_interactions BIGINT NOT NULL,
    total_duration_ms BIGINT NOT NULL,
    avg_duration_ms DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date)
);

COMMENT ON TABLE daily_active_users IS 'Daily active user metrics with interaction counts';
COMMENT ON COLUMN daily_active_users.dau IS 'Number of unique active users on the date';
COMMENT ON COLUMN daily_active_users.total_interactions IS 'Total number of interactions on the date';
COMMENT ON COLUMN daily_active_users.total_duration_ms IS 'Sum of all interaction durations in milliseconds';


-- Monthly Active Users (MAU)
-- Stores monthly active user counts
CREATE TABLE IF NOT EXISTS monthly_active_users (
    year_month VARCHAR(7) NOT NULL,  -- Format: 'YYYY-MM'
    mau INTEGER NOT NULL,
    total_interactions BIGINT NOT NULL,
    total_duration_ms BIGINT NOT NULL,
    avg_duration_ms DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year_month)
);

COMMENT ON TABLE monthly_active_users IS 'Monthly active user metrics';
COMMENT ON COLUMN monthly_active_users.year_month IS 'Month in YYYY-MM format';
COMMENT ON COLUMN monthly_active_users.mau IS 'Number of unique active users in the month';


-- User Stickiness (DAU/MAU Ratio)
-- Measures user engagement quality
CREATE TABLE IF NOT EXISTS user_stickiness (
    date DATE NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    dau INTEGER NOT NULL,
    mau INTEGER NOT NULL,
    stickiness DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date)
);

COMMENT ON TABLE user_stickiness IS 'Daily stickiness ratio (DAU/MAU) measuring engagement quality';
COMMENT ON COLUMN user_stickiness.stickiness IS 'Stickiness ratio between 0.0 and 1.0';


-- Power Users
-- Top 1% most engaged users
CREATE TABLE IF NOT EXISTS power_users (
    user_id VARCHAR(50) NOT NULL,
    total_interactions BIGINT NOT NULL,
    total_duration_ms BIGINT NOT NULL,
    total_duration_hours DOUBLE PRECISION NOT NULL,
    avg_duration_ms DOUBLE PRECISION NOT NULL,
    country VARCHAR(10),
    device_type VARCHAR(50),
    subscription_type VARCHAR(50),
    join_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);

COMMENT ON TABLE power_users IS 'Top 1% most engaged users with metadata';
COMMENT ON COLUMN power_users.total_duration_hours IS 'Total engagement time in hours';


-- Cohort Retention
-- Weekly cohort analysis (6 months retention)
CREATE TABLE IF NOT EXISTS cohort_retention (
    cohort_week VARCHAR(10) NOT NULL,  -- Format: 'YYYY-Wnn'
    week_number INTEGER NOT NULL,       -- 0, 1, 2, ... 25 (6 months)
    cohort_size INTEGER NOT NULL,
    retained_users INTEGER NOT NULL,
    retention_rate DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cohort_week, week_number)
);

COMMENT ON TABLE cohort_retention IS 'Weekly cohort retention analysis over 6 months';
COMMENT ON COLUMN cohort_retention.cohort_week IS 'Week when users joined (YYYY-Wnn format)';
COMMENT ON COLUMN cohort_retention.week_number IS 'Weeks since joining (0-25)';
COMMENT ON COLUMN cohort_retention.retention_rate IS 'Percentage of cohort still active';


-- ============================================
-- 2. PERFORMANCE METRICS TABLES
-- ============================================

-- Performance by App Version
-- P50, P95, P99 load times by app version
CREATE TABLE IF NOT EXISTS performance_by_version (
    app_version VARCHAR(20) NOT NULL,
    metric_date DATE NOT NULL,
    total_interactions BIGINT NOT NULL,
    p50_duration_ms DOUBLE PRECISION NOT NULL,
    p95_duration_ms DOUBLE PRECISION NOT NULL,
    p99_duration_ms DOUBLE PRECISION NOT NULL,
    avg_duration_ms DOUBLE PRECISION NOT NULL,
    min_duration_ms BIGINT NOT NULL,
    max_duration_ms BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_version, metric_date)
);

COMMENT ON TABLE performance_by_version IS 'Performance percentiles (P50, P95, P99) by app version';
COMMENT ON COLUMN performance_by_version.p50_duration_ms IS 'Median load time in milliseconds';
COMMENT ON COLUMN performance_by_version.p95_duration_ms IS '95th percentile load time';
COMMENT ON COLUMN performance_by_version.p99_duration_ms IS '99th percentile load time';


-- Device Performance
-- Performance metrics by device type
CREATE TABLE IF NOT EXISTS device_performance (
    device_type VARCHAR(50) NOT NULL,
    metric_date DATE NOT NULL,
    user_count INTEGER NOT NULL,
    total_interactions BIGINT NOT NULL,
    avg_duration_ms DOUBLE PRECISION NOT NULL,
    p50_duration_ms DOUBLE PRECISION NOT NULL,
    p95_duration_ms DOUBLE PRECISION NOT NULL,
    p99_duration_ms DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (device_type, metric_date)
);

COMMENT ON TABLE device_performance IS 'Performance metrics grouped by device type';
COMMENT ON COLUMN device_performance.user_count IS 'Number of unique users on this device';


-- Performance Anomalies
-- Detected anomalies in performance metrics
CREATE TABLE IF NOT EXISTS performance_anomalies (
    anomaly_id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP NOT NULL,
    metric_date DATE NOT NULL,
    app_version VARCHAR(20),
    device_type VARCHAR(50),
    metric_name VARCHAR(50) NOT NULL,  -- 'duration_ms', 'interaction_count', etc.
    metric_value DOUBLE PRECISION NOT NULL,
    expected_value DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    severity VARCHAR(20) NOT NULL,  -- 'low', 'medium', 'high', 'critical'
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE performance_anomalies IS 'Statistical anomalies detected in performance metrics';
COMMENT ON COLUMN performance_anomalies.z_score IS 'Z-score indicating severity of anomaly';
COMMENT ON COLUMN performance_anomalies.severity IS 'Categorized severity level';


-- ============================================
-- 3. SESSION ANALYSIS TABLES
-- ============================================

-- Session Metrics
-- Detailed metrics for each session
CREATE TABLE IF NOT EXISTS session_metrics (
    session_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    session_start_time TIMESTAMP NOT NULL,
    session_end_time TIMESTAMP NOT NULL,
    session_duration_ms BIGINT NOT NULL,
    actions_count INTEGER NOT NULL,
    avg_action_duration_ms DOUBLE PRECISION NOT NULL,
    is_bounce INTEGER NOT NULL,  -- 0 or 1
    device_type VARCHAR(50),
    country VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id)
);

COMMENT ON TABLE session_metrics IS 'Metrics for each user session';
COMMENT ON COLUMN session_metrics.is_bounce IS 'Boolean flag: 1 if single-action session, 0 otherwise';
COMMENT ON COLUMN session_metrics.actions_count IS 'Number of actions/interactions in the session';


-- Bounce Rates
-- Bounce rate metrics (overall and by dimensions)
CREATE TABLE IF NOT EXISTS bounce_rates (
    metric_id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    dimension_name VARCHAR(50),      -- NULL for overall, 'device_type', 'country', etc.
    dimension_value VARCHAR(100),    -- NULL for overall, 'iPad', 'US', etc.
    total_sessions INTEGER NOT NULL,
    bounced_sessions INTEGER NOT NULL,
    bounce_rate DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metric_date, dimension_name, dimension_value)
);

COMMENT ON TABLE bounce_rates IS 'Bounce rate metrics overall and by dimensions';
COMMENT ON COLUMN bounce_rates.bounce_rate IS 'Percentage of single-action sessions (0.0 to 1.0)';
COMMENT ON COLUMN bounce_rates.dimension_name IS 'Grouping dimension or NULL for overall';


-- ============================================
-- 4. RAW/STAGING TABLES (Optional)
-- ============================================

-- User Interactions (Raw/Staging)
-- Optional: Store processed interactions if needed
CREATE TABLE IF NOT EXISTS user_interactions (
    interaction_id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    page_id VARCHAR(100) NOT NULL,
    duration_ms BIGINT NOT NULL,
    app_version VARCHAR(20) NOT NULL,
    session_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE user_interactions IS 'Raw or enriched user interactions (optional staging table)';


-- User Metadata
-- Store user metadata for reference
CREATE TABLE IF NOT EXISTS user_metadata (
    user_id VARCHAR(50) PRIMARY KEY,
    join_date DATE NOT NULL,
    country VARCHAR(10) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    subscription_type VARCHAR(50) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE user_metadata IS 'User profile and metadata information';


-- ============================================
-- 5. JOB MONITORING TABLES
-- ============================================

-- ETL Job Runs
-- Track Spark job executions
CREATE TABLE IF NOT EXISTS etl_job_runs (
    job_run_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL,  -- 'engagement', 'performance', 'session', etc.
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,  -- 'running', 'success', 'failed'
    records_processed BIGINT,
    error_message TEXT,
    spark_app_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE etl_job_runs IS 'Tracks Spark ETL job executions and status';


-- ============================================
-- 6. GRANT PERMISSIONS
-- ============================================

-- Grant SELECT to read-only user (Superset)
-- Note: Adjust based on your user management strategy
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO superset_ro;
-- GRANT INSERT ON ALL TABLES IN SCHEMA public TO spark_writer;

-- ============================================
-- END OF SCHEMA
-- ============================================
