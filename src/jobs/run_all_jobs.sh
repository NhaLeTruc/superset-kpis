#!/bin/bash
# ============================================
# GoodNote Analytics Pipeline Orchestrator
# ============================================
# Runs all Spark jobs in sequence
# Usage: ./src/jobs/run_all_jobs.sh [--write-to-db]
# ============================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ENRICHED_DATA_PATH="${ENRICHED_DATA_PATH:-data/processed/enriched_interactions.parquet}"
RAW_INTERACTIONS_PATH="${RAW_INTERACTIONS_PATH:-data/raw/interactions.parquet}"
RAW_METADATA_PATH="${RAW_METADATA_PATH:-data/raw/metadata.parquet}"
OUTPUT_BASE_PATH="${OUTPUT_BASE_PATH:-data/analytics}"

# Parse arguments
WRITE_TO_DB=""
if [[ "$1" == "--write-to-db" ]]; then
    WRITE_TO_DB="--write-to-db"
    echo -e "${GREEN}✅ Database writes enabled${NC}"
else
    echo -e "${YELLOW}⚠️  Database writes disabled (use --write-to-db to enable)${NC}"
fi

# Log file
LOG_DIR="logs"
mkdir -p $LOG_DIR
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/pipeline_$TIMESTAMP.log"

echo "=" | tee -a $LOG_FILE
echo "🚀 GoodNote Analytics Pipeline" | tee -a $LOG_FILE
echo "=" | tee -a $LOG_FILE
echo "Start Time: $(date)" | tee -a $LOG_FILE
echo "Log File: $LOG_FILE" | tee -a $LOG_FILE
echo "=" | tee -a $LOG_FILE

# Function to run a job with error handling
run_job() {
    local job_name=$1
    local job_script=$2
    shift 2
    local job_args="$@"

    echo "" | tee -a $LOG_FILE
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a $LOG_FILE
    echo -e "${BLUE}▶ Running: $job_name${NC}" | tee -a $LOG_FILE
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a $LOG_FILE

    START_TIME=$(date +%s)

    if python $job_script $job_args 2>&1 | tee -a $LOG_FILE; then
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        echo -e "${GREEN}✅ $job_name completed successfully (${DURATION}s)${NC}" | tee -a $LOG_FILE
        return 0
    else
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        echo -e "${RED}❌ $job_name failed after ${DURATION}s${NC}" | tee -a $LOG_FILE
        return 1
    fi
}

# Total pipeline start time
PIPELINE_START=$(date +%s)

# ============================================
# JOB 1: DATA PROCESSING
# ============================================
if run_job "Job 1: Data Processing" \
    "src/jobs/01_data_processing.py" \
    --interactions-path "$RAW_INTERACTIONS_PATH" \
    --metadata-path "$RAW_METADATA_PATH" \
    --output-path "$ENRICHED_DATA_PATH"; then

    JOB1_SUCCESS=true
else
    echo -e "${RED}❌ Pipeline failed at Job 1${NC}" | tee -a $LOG_FILE
    exit 1
fi

# ============================================
# JOB 2: USER ENGAGEMENT
# ============================================
if run_job "Job 2: User Engagement" \
    "src/jobs/02_user_engagement.py" \
    --enriched-path "$ENRICHED_DATA_PATH" \
    $WRITE_TO_DB \
    --output-path "$OUTPUT_BASE_PATH/engagement"; then

    JOB2_SUCCESS=true
else
    echo -e "${RED}❌ Pipeline failed at Job 2${NC}" | tee -a $LOG_FILE
    exit 1
fi

# ============================================
# JOB 3: PERFORMANCE METRICS
# ============================================
if run_job "Job 3: Performance Metrics" \
    "src/jobs/03_performance_metrics.py" \
    --enriched-path "$ENRICHED_DATA_PATH" \
    $WRITE_TO_DB \
    --output-path "$OUTPUT_BASE_PATH/performance"; then

    JOB3_SUCCESS=true
else
    echo -e "${RED}❌ Pipeline failed at Job 3${NC}" | tee -a $LOG_FILE
    exit 1
fi

# ============================================
# JOB 4: SESSION ANALYSIS
# ============================================
if run_job "Job 4: Session Analysis" \
    "src/jobs/04_session_analysis.py" \
    --enriched-path "$ENRICHED_DATA_PATH" \
    $WRITE_TO_DB \
    --output-path "$OUTPUT_BASE_PATH/sessions"; then

    JOB4_SUCCESS=true
else
    echo -e "${RED}❌ Pipeline failed at Job 4${NC}" | tee -a $LOG_FILE
    exit 1
fi

# ============================================
# PIPELINE SUMMARY
# ============================================
PIPELINE_END=$(date +%s)
TOTAL_DURATION=$((PIPELINE_END - PIPELINE_START))
MINUTES=$((TOTAL_DURATION / 60))
SECONDS=$((TOTAL_DURATION % 60))

echo "" | tee -a $LOG_FILE
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a $LOG_FILE
echo -e "${GREEN}✅ PIPELINE COMPLETED SUCCESSFULLY${NC}" | tee -a $LOG_FILE
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a $LOG_FILE
echo "End Time: $(date)" | tee -a $LOG_FILE
echo "Total Duration: ${MINUTES}m ${SECONDS}s" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Jobs Completed:" | tee -a $LOG_FILE
echo "  ✅ Job 1: Data Processing" | tee -a $LOG_FILE
echo "  ✅ Job 2: User Engagement" | tee -a $LOG_FILE
echo "  ✅ Job 3: Performance Metrics" | tee -a $LOG_FILE
echo "  ✅ Job 4: Session Analysis" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Output Locations:" | tee -a $LOG_FILE
echo "  Enriched Data: $ENRICHED_DATA_PATH" | tee -a $LOG_FILE
echo "  Analytics: $OUTPUT_BASE_PATH" | tee -a $LOG_FILE
if [[ -n "$WRITE_TO_DB" ]]; then
    echo "  Database: PostgreSQL tables updated" | tee -a $LOG_FILE
fi
echo "" | tee -a $LOG_FILE
echo "Log File: $LOG_FILE" | tee -a $LOG_FILE
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a $LOG_FILE

exit 0
