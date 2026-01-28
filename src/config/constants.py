SESSION_TIMEOUT_SECONDS = 1800
DEFAULT_PERCENTILES = [0.50, 0.95, 0.99]
HOT_KEY_THRESHOLD_PERCENTILE = 0.99
Z_SCORE_ANOMALY_THRESHOLD = 3.0

# Anomaly severity thresholds (z-score values)
ANOMALY_SEVERITY_CRITICAL = 4.0
ANOMALY_SEVERITY_HIGH = 3.5
ANOMALY_SEVERITY_MEDIUM = 3.0

# Database table names
TABLE_DAILY_ACTIVE_USERS = "daily_active_users"
TABLE_MONTHLY_ACTIVE_USERS = "monthly_active_users"
TABLE_USER_STICKINESS = "user_stickiness"
TABLE_POWER_USERS = "power_users"
TABLE_COHORT_RETENTION = "cohort_retention"
TABLE_PERFORMANCE_BY_VERSION = "performance_by_version"
TABLE_DEVICE_PERFORMANCE = "device_performance"
TABLE_DEVICE_CORRELATION = "device_correlation"
TABLE_PERFORMANCE_ANOMALIES = "performance_anomalies"
TABLE_SESSION_METRICS = "session_metrics"
TABLE_SESSION_FREQUENCY = "session_frequency"
TABLE_BOUNCE_RATES = "bounce_rates"
