#!/bin/bash
# Start Docker development environment

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🐳 Starting GoodNote Analytics Development Environment${NC}"
echo ""

# Build and start services
echo -e "${YELLOW}📦 Building Docker images...${NC}"
docker-compose build

echo ""
echo -e "${YELLOW}🚀 Starting services...${NC}"
docker-compose up -d

echo ""
echo -e "${GREEN}✅ Development environment is ready!${NC}"
echo ""
echo "Available services:"
echo "  • Spark Dev    : docker exec -it goodnote-spark-dev bash"
echo "  • PostgreSQL   : localhost:5432 (user: analytics_user, pass: analytics_pass)"
echo ""
echo "Quick commands:"
echo "  • Run tests    : ./docker/run-tests.sh"
echo "  • Stop all     : docker-compose down"
echo "  • View logs    : docker-compose logs -f"
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
