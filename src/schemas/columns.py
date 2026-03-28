# Base interaction columns
COL_USER_ID = "user_id"
COL_TIMESTAMP = "timestamp"
COL_ACTION_TYPE = "action_type"
COL_PAGE_ID = "page_id"
COL_DURATION_MS = "duration_ms"
COL_APP_VERSION = "app_version"

# Base metadata columns
COL_DEVICE_TYPE = "device_type"
COL_COUNTRY = "country"
COL_SUBSCRIPTION_TYPE = "subscription_type"
COL_REGISTRATION_DATE = "registration_date"

# Derived columns (used across 2+ files)
COL_DATE = "date"
COL_PROCESSING_TIMESTAMP = "processing_timestamp"
COL_METRIC_DATE = "metric_date"
COL_SESSION_ID = "session_id"
COL_SESSION_DURATION_MS = "session_duration_ms"
COL_ACTION_COUNT = "action_count"
COL_IS_BOUNCE = "is_bounce"
COL_BOUNCE_RATE = "bounce_rate"
COL_SEVERITY = "severity"

# Engagement metric columns (output of engagement transforms)
COL_DAILY_ACTIVE_USERS = "daily_active_users"
COL_MONTHLY_ACTIVE_USERS = "monthly_active_users"
COL_STICKINESS_RATIO = "stickiness_ratio"
COL_HOURS_SPENT = "hours_spent"
COL_WEEK_NUMBER = "week_number"
COL_RETENTION_RATE = "retention_rate"

# Performance metric columns (output of performance transforms)
COL_P50 = "p50"
COL_P95 = "p95"
COL_P99 = "p99"
COL_P50_DURATION_MS = "p50_duration_ms"
COL_P95_DURATION_MS = "p95_duration_ms"
COL_P99_DURATION_MS = "p99_duration_ms"
COL_AVG_DURATION_MS = "avg_duration_ms"
COL_F_STATISTIC = "f_statistic"
COL_ETA_SQUARED = "eta_squared"

# Anomaly detection columns (output of anomaly detection transforms)
COL_Z_SCORE = "z_score"
COL_ANOMALY_TYPE = "anomaly_type"
COL_BASELINE_MEAN = "baseline_mean"
COL_METRIC_NAME = "metric_name"
COL_METRIC_VALUE = "metric_value"
COL_EXPECTED_VALUE = "expected_value"
COL_DETECTED_AT = "detected_at"
COL_DESCRIPTION = "description"

# Session analysis metric columns (output of session transforms)
COL_SESSION_START_TIME = "session_start_time"
COL_SESSION_END_TIME = "session_end_time"
COL_AVG_ACTION_DURATION_MS = "avg_action_duration_ms"
COL_ACTIONS_COUNT = "actions_count"  # DB schema name; Spark transform outputs COL_ACTION_COUNT
COL_TOTAL_SESSIONS = "total_sessions"
COL_AVG_SESSIONS_PER_DAY = "avg_sessions_per_day"
COL_DIMENSION_NAME = "dimension_name"
COL_DIMENSION_VALUE = "dimension_value"
