#!/bin/bash
# ============================================
# GoodNote Spark Job Runner
# ============================================
# Runs PySpark jobs inside the Docker Spark cluster
#
# Usage:
#   ./scripts/run_spark_job.sh <job_script> [job_arguments...]
#
# Examples:
#   # Job 1: Data Processing
#   ./scripts/run_spark_job.sh src/jobs/01_data_processing.py \
#       --interactions-path /app/data/raw/interactions.parquet \
#       --metadata-path /app/data/raw/metadata.parquet \
#       --output-path /app/data/processed/enriched_interactions.parquet
#
#   # Job 2: User Engagement
#   ./scripts/run_spark_job.sh src/jobs/02_user_engagement.py \
#       --enriched-path /app/data/processed/enriched_interactions.parquet \
#       --write-to-db
#
#   # Job 3: Performance Metrics
#   ./scripts/run_spark_job.sh src/jobs/03_performance_metrics.py \
#       --enriched-path /app/data/processed/enriched_interactions.parquet \
#       --write-to-db
#
#   # Job 4: Session Analysis
#   ./scripts/run_spark_job.sh src/jobs/04_session_analysis.py \
#       --enriched-path /app/data/processed/enriched_interactions.parquet \
#       --write-to-db
#
# ============================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
SPARK_MASTER_CONTAINER="${CONTAINER_SPARK_MASTER:-goodnote-spark-master}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[*]}"

# Check arguments
if [ $# -lt 1 ]; then
    echo -e "${RED}Error: No job script specified${NC}"
    echo ""
    echo "Usage: $0 <job_script> [job_arguments...]"
    echo ""
    echo "Example:"
    echo "  $0 src/jobs/01_data_processing.py --interactions-path /app/data/raw/interactions.parquet ..."
    exit 1
fi

JOB_SCRIPT="$1"
shift
JOB_ARGS="$@"

# Convert local path to container path
# Local: src/jobs/... -> Container: /opt/spark-apps/src/jobs/...
CONTAINER_SCRIPT="/opt/spark-apps/${JOB_SCRIPT}"

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}▶ Running Spark Job${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo "Script: ${JOB_SCRIPT}"
echo "Container: ${SPARK_MASTER_CONTAINER}"
echo "Arguments: ${JOB_ARGS}"
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${SPARK_MASTER_CONTAINER}$"; then
    echo -e "${RED}Error: Spark master container '${SPARK_MASTER_CONTAINER}' is not running${NC}"
    echo "Start the cluster with: docker-compose up -d spark-master spark-worker-1 spark-worker-2"
    exit 1
fi

# Run the job using spark-submit
# Set PYTHONPATH so 'from src.xxx import' works
docker exec "${SPARK_MASTER_CONTAINER}" \
    /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    --conf "spark.pyspark.python=python3" \
    --conf "spark.pyspark.driver.python=python3" \
    --conf "spark.driver.extraPythonPath=/opt/spark-apps" \
    --conf "spark.executor.extraPythonPath=/opt/spark-apps" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    "${CONTAINER_SCRIPT}" \
    ${JOB_ARGS}

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Job completed successfully${NC}"
else
    echo -e "${RED}❌ Job failed with exit code ${EXIT_CODE}${NC}"
fi

exit $EXIT_CODE
