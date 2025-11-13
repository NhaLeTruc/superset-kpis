#!/bin/bash
# Run pytest inside Docker container

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}🧪 Running tests in Docker...${NC}"
echo ""

# Check if container is running
if ! docker ps | grep -q goodnote-spark-dev; then
    echo -e "${YELLOW}⚠️  Container not running. Starting services...${NC}"
    docker compose up -d spark-dev
    echo -e "${GREEN}✅ Waiting for container to be ready...${NC}"
    sleep 5
fi

# Run pytest with coverage
# Set environment variables to ensure writable temp directory for coverage data
echo -e "${GREEN}📊 Running pytest with coverage...${NC}"
docker exec \
    -e TMPDIR=/tmp \
    -e COVERAGE_FILE=/tmp/.coverage \
    goodnote-spark-dev \
    pytest tests/ -v --cov=src --cov-report=term-missing "$@"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✅ All tests passed!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
else
    echo ""
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}❌ Tests failed!${NC}"
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
fi

exit $EXIT_CODE
