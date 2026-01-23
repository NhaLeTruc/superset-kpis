-- ============================================
-- GoodNote Analytics Platform - Database Indexes
-- ============================================
-- PostgreSQL Indexes for Query Optimization
-- Purpose: Optimize common query patterns for dashboards
-- Created: 2025-11-13
-- ============================================

-- ============================================
-- 1. ENGAGEMENT METRICS INDEXES
-- ============================================

-- Daily Active Users
-- Primary key (date) already has index
CREATE INDEX IF NOT EXISTS idx_dau_created_at ON daily_active_users(created_at);

-- Monthly Active Users
-- Primary key (year_month) already has index
CREATE INDEX IF NOT EXISTS idx_mau_created_at ON monthly_active_users(created_at);

-- User Stickiness
-- Primary key (date) already has index
CREATE INDEX IF NOT EXISTS idx_stickiness_year_month ON user_stickiness(year_month);
CREATE INDEX IF NOT EXISTS idx_stickiness_ratio ON user_stickiness(stickiness DESC);  -- For finding high/low stickiness days

-- Power Users
-- Primary key (user_id) already has index
CREATE INDEX IF NOT EXISTS idx_power_users_country ON power_users(country);
CREATE INDEX IF NOT EXISTS idx_power_users_device ON power_users(device_type);
CREATE INDEX IF NOT EXISTS idx_power_users_subscription ON power_users(subscription_type);
CREATE INDEX IF NOT EXISTS idx_power_users_duration_hours ON power_users(total_duration_hours DESC);  -- For ranking
CREATE INDEX IF NOT EXISTS idx_power_users_interactions ON power_users(total_interactions DESC);  -- For ranking

-- Cohort Retention
-- Primary key (cohort_week, week_number) already has index
CREATE INDEX IF NOT EXISTS idx_cohort_week ON cohort_retention(cohort_week);
CREATE INDEX IF NOT EXISTS idx_cohort_retention_rate ON cohort_retention(retention_rate DESC);  -- For finding best/worst cohorts


-- ============================================
-- 2. PERFORMANCE METRICS INDEXES
-- ============================================

-- Performance by Version
-- Primary key (app_version, metric_date) already has index
CREATE INDEX IF NOT EXISTS idx_perf_version_date ON performance_by_version(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_perf_version_p99 ON performance_by_version(p99_duration_ms DESC);  -- For finding slow versions
CREATE INDEX IF NOT EXISTS idx_perf_version_created ON performance_by_version(created_at);

-- Device Performance
-- Primary key (device_type, metric_date) already has index
CREATE INDEX IF NOT EXISTS idx_device_perf_date ON device_performance(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_device_perf_p99 ON device_performance(p99_duration_ms DESC);  -- For finding slow devices

-- Performance Anomalies
-- Primary key (anomaly_id) already has index
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON performance_anomalies(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_metric_date ON performance_anomalies(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON performance_anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_app_version ON performance_anomalies(app_version);
CREATE INDEX IF NOT EXISTS idx_anomalies_device_type ON performance_anomalies(device_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_z_score ON performance_anomalies(z_score DESC);  -- For finding severe anomalies


-- ============================================
-- 3. SESSION ANALYSIS INDEXES
-- ============================================

-- Session Metrics
-- Primary key (session_id) already has index
CREATE INDEX IF NOT EXISTS idx_session_user_id ON session_metrics(user_id);
CREATE INDEX IF NOT EXISTS idx_session_start_time ON session_metrics(session_start_time DESC);
CREATE INDEX IF NOT EXISTS idx_session_device_type ON session_metrics(device_type);
CREATE INDEX IF NOT EXISTS idx_session_country ON session_metrics(country);
CREATE INDEX IF NOT EXISTS idx_session_is_bounce ON session_metrics(is_bounce);
CREATE INDEX IF NOT EXISTS idx_session_duration ON session_metrics(session_duration_ms DESC);  -- For finding long/short sessions
CREATE INDEX IF NOT EXISTS idx_session_actions_count ON session_metrics(actions_count DESC);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_session_device_bounce ON session_metrics(device_type, is_bounce);
CREATE INDEX IF NOT EXISTS idx_session_country_bounce ON session_metrics(country, is_bounce);

-- Bounce Rates
-- Primary key (metric_id) already has index
CREATE INDEX IF NOT EXISTS idx_bounce_metric_date ON bounce_rates(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_bounce_dimension ON bounce_rates(dimension_name, dimension_value);
CREATE INDEX IF NOT EXISTS idx_bounce_rate_value ON bounce_rates(bounce_rate DESC);  -- For finding high bounce rates


-- ============================================
-- 4. RAW/STAGING TABLES INDEXES
-- ============================================

-- User Interactions (if used)
CREATE INDEX IF NOT EXISTS idx_interactions_user_id ON user_interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_timestamp ON user_interactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_interactions_action_type ON user_interactions(action_type);
CREATE INDEX IF NOT EXISTS idx_interactions_session_id ON user_interactions(session_id);
CREATE INDEX IF NOT EXISTS idx_interactions_app_version ON user_interactions(app_version);

-- Composite index for user timeline queries
CREATE INDEX IF NOT EXISTS idx_interactions_user_time ON user_interactions(user_id, timestamp DESC);

-- User Metadata
-- Primary key (user_id) already has index
CREATE INDEX IF NOT EXISTS idx_metadata_country ON user_metadata(country);
CREATE INDEX IF NOT EXISTS idx_metadata_device ON user_metadata(device_type);
CREATE INDEX IF NOT EXISTS idx_metadata_subscription ON user_metadata(subscription_type);
CREATE INDEX IF NOT EXISTS idx_metadata_registration_date ON user_metadata(registration_date);


-- ============================================
-- 5. JOB MONITORING INDEXES
-- ============================================

-- ETL Job Runs
CREATE INDEX IF NOT EXISTS idx_job_runs_name ON etl_job_runs(job_name);
CREATE INDEX IF NOT EXISTS idx_job_runs_type ON etl_job_runs(job_type);
CREATE INDEX IF NOT EXISTS idx_job_runs_start_time ON etl_job_runs(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON etl_job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_created ON etl_job_runs(created_at DESC);


-- ============================================
-- 6. DASHBOARD-SPECIFIC OPTIMIZATIONS
-- ============================================

-- Dashboard 1: Executive Overview
-- Optimize time series queries for DAU/MAU
CREATE INDEX IF NOT EXISTS idx_dau_date_desc ON daily_active_users(date DESC);
CREATE INDEX IF NOT EXISTS idx_mau_yearmonth_desc ON monthly_active_users(year_month DESC);

-- Dashboard 2: User Engagement Deep Dive
-- Optimize cohort heatmap queries
CREATE INDEX IF NOT EXISTS idx_cohort_week_weeknum ON cohort_retention(cohort_week, week_number);

-- Dashboard 3: Performance Monitoring
-- Optimize version performance comparison
CREATE INDEX IF NOT EXISTS idx_perf_version_date_composite ON performance_by_version(app_version, metric_date DESC);

-- Dashboard 4: Session Analytics
-- Optimize bounce rate by device queries
CREATE INDEX IF NOT EXISTS idx_bounce_device_date ON bounce_rates(metric_date DESC, dimension_value)
    WHERE dimension_name = 'device_type';


-- ============================================
-- 7. MAINTENANCE INDEXES
-- ============================================

-- For efficient data cleanup/archival
-- Note: CURRENT_DATE is not immutable, so these indexes cannot use it in WHERE clause
-- Instead, create regular indexes on created_at which can still be used for archival queries
CREATE INDEX IF NOT EXISTS idx_dau_created_archival ON daily_active_users(created_at);

CREATE INDEX IF NOT EXISTS idx_sessions_created_archival ON session_metrics(created_at);


-- ============================================
-- 8. VACUUM AND ANALYZE
-- ============================================

-- Run VACUUM and ANALYZE after creating indexes
-- VACUUM ANALYZE daily_active_users;
-- VACUUM ANALYZE monthly_active_users;
-- VACUUM ANALYZE user_stickiness;
-- VACUUM ANALYZE power_users;
-- VACUUM ANALYZE cohort_retention;
-- VACUUM ANALYZE performance_by_version;
-- VACUUM ANALYZE device_performance;
-- VACUUM ANALYZE performance_anomalies;
-- VACUUM ANALYZE session_metrics;
-- VACUUM ANALYZE bounce_rates;
-- VACUUM ANALYZE user_interactions;
-- VACUUM ANALYZE user_metadata;
-- VACUUM ANALYZE etl_job_runs;


-- ============================================
-- 9. INDEX MONITORING QUERIES
-- ============================================

-- View index usage statistics
-- SELECT
--     schemaname,
--     tablename,
--     indexname,
--     idx_scan,
--     idx_tup_read,
--     idx_tup_fetch
-- FROM pg_stat_user_indexes
-- ORDER BY idx_scan DESC;

-- Find unused indexes
-- SELECT
--     schemaname,
--     tablename,
--     indexname,
--     pg_size_pretty(pg_relation_size(indexrelid)) as index_size
-- FROM pg_stat_user_indexes
-- WHERE idx_scan = 0
--   AND indexrelname NOT LIKE '%_pkey'
-- ORDER BY pg_relation_size(indexrelid) DESC;


-- ============================================
-- END OF INDEXES
-- ============================================
