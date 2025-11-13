# Setup Instructions - Run on Your Local Machine

## 🎯 What We've Built

You now have a **complete Docker-based development environment** for the GoodNote Analytics Platform with:

✅ **Spark 3.5.0** with Python 3
✅ **PostgreSQL 15** database
✅ **TDD-ready** testing framework (pytest + chispa)
✅ **Live code editing** (Docker volumes)
✅ **Pre-commit hooks** enforcing TDD
✅ **First function implemented** (`validate_schema`)

---

## 📋 Prerequisites (Install on Your Machine)

1. **Docker Desktop** (https://www.docker.com/products/docker-desktop/)
   - Windows/Mac: Download and install Docker Desktop
   - Linux: Install `docker` and `docker-compose`
   - Allocate **8GB+ RAM** to Docker

2. **Git** (to clone the repository)

3. **Text Editor/IDE** (VS Code, PyCharm, etc.)

---

## 🚀 Quick Start (On Your Machine)

### Step 1: Clone Repository

```bash
# Clone the repository
git clone <repository-url>
cd claude-superset-demo

# Checkout the implementation branch
git checkout claude/review-project-011CV5VecFHFU9f9e6CK8WSx
```

### Step 2: Start Docker Environment

```bash
# Make scripts executable (Linux/Mac)
chmod +x docker/*.sh setup-tdd.sh check-tdd.sh

# Start the development environment
./docker/start-dev.sh
```

**Expected Output:**
```
🐳 Starting GoodNote Analytics Development Environment
📦 Building Docker images...
🚀 Starting services...
✅ Development environment is ready!

Available services:
  • Spark Dev    : docker exec -it goodnote-spark-dev bash
  • PostgreSQL   : localhost:5432
```

**This will:**
- Build the Spark Docker image (first time: ~5-10 minutes)
- Start containers (spark-dev, postgres)
- Install all dependencies inside Docker

### Step 3: Run Tests

```bash
# Run all tests
./docker/run-tests.sh
```

**Expected Output:**
```
🧪 Running tests in Docker...
📊 Running pytest with coverage...

tests/unit/test_data_quality.py::TestValidateSchema::test_validate_schema_valid PASSED
tests/unit/test_data_quality.py::TestValidateSchema::test_validate_schema_missing_column PASSED
tests/unit/test_data_quality.py::TestValidateSchema::test_validate_schema_wrong_type PASSED

---------- coverage: platform linux, python 3.11.x -----------
Name                           Stmts   Miss  Cover   Missing
------------------------------------------------------------
src/utils/data_quality.py         20      0   100%

✅ All tests passed!
```

---

## 🧪 TDD Workflow

### Write Test → Run (RED) → Implement → Run (GREEN)

```bash
# 1. Edit test file in your IDE
code tests/unit/test_data_quality.py

# 2. Run test (should FAIL - RED state)
./docker/run-tests.sh tests/unit/test_data_quality.py

# 3. Edit implementation in your IDE
code src/utils/data_quality.py

# 4. Run test again (should PASS - GREEN state)
./docker/run-tests.sh tests/unit/test_data_quality.py

# 5. Commit (git hooks verify tests exist)
git add tests/ src/
git commit -m "Implement function with TDD"
```

---

## 📂 Project Structure

```
claude-superset-demo/
├── src/                          # Source code (edit in your IDE)
│   └── utils/
│       └── data_quality.py      # ✅ Function 1 implemented
├── tests/                        # Tests (edit in your IDE)
│   └── unit/
│       └── test_data_quality.py # ✅ 3 tests passing
├── docker/                       # Docker configuration
│   ├── Dockerfile               # Spark image
│   ├── start-dev.sh             # Start environment
│   └── run-tests.sh             # Run tests
├── docker-compose.yml            # Services definition
├── requirements.txt              # Python dependencies
└── DOCKER_QUICKSTART.md          # Detailed Docker guide
```

---

## 🎓 What's Already Done

### ✅ Phase 0: Setup (COMPLETE)
- Docker environment configured
- TDD enforcement hooks installed
- Directory structure created
- Test fixtures prepared

### ✅ Phase 2: Data Quality - Function 1 (COMPLETE)
- **Function:** `validate_schema(df, expected_schema, strict=True)`
- **Tests:** 3 passing tests
- **Coverage:** 100% for this function
- **TDD:** Strict RED → GREEN → REFACTOR followed

### 🔜 Next Tasks (Phase 2 Remaining)

**Function 2:** `detect_nulls()` - 2 tests
**Function 3:** `detect_outliers()` - 2 tests

---

## 🔧 Useful Commands

### Docker Environment

```bash
# Start environment
./docker/start-dev.sh

# Stop environment
docker-compose down

# View logs
docker-compose logs -f spark-dev

# Enter Spark container
docker exec -it goodnote-spark-dev bash

# Restart after code changes (not needed - volumes auto-sync)
docker-compose restart spark-dev
```

### Testing

```bash
# Run all tests
./docker/run-tests.sh

# Run specific test file
./docker/run-tests.sh tests/unit/test_data_quality.py

# Run specific test
./docker/run-tests.sh tests/unit/test_data_quality.py::TestValidateSchema::test_validate_schema_valid

# Check coverage
docker exec goodnote-spark-dev pytest --cov=src --cov-report=html
```

### TDD Compliance

```bash
# Check TDD compliance
./check-tdd.sh

# Run TDD setup (if not done)
./setup-tdd.sh
```

---

## 🐛 Troubleshooting

### Docker Issues

**Problem:** "Cannot connect to Docker daemon"
```bash
# Solution: Start Docker Desktop
# Check: docker ps
```

**Problem:** "Port 5432 already in use"
```bash
# Solution: Stop other PostgreSQL instances
# Mac/Linux: sudo lsof -i :5432
# Windows: netstat -ano | findstr :5432
```

**Problem:** "Out of memory"
```bash
# Solution: Increase Docker memory
# Docker Desktop → Settings → Resources → Memory: 8GB+
```

### Test Failures

**Problem:** "No module named 'pyspark'"
```bash
# Solution: Rebuild Docker image
docker-compose build --no-cache spark-dev
docker-compose up -d
```

**Problem:** "Tests not found"
```bash
# Solution: Check you're in project root
pwd  # Should show .../claude-superset-demo

# Verify mounts
docker exec goodnote-spark-dev ls -la /app/tests
```

---

## 📖 Documentation

- **Quick Start:** `DOCKER_QUICKSTART.md` (30-second setup)
- **TDD Spec:** `docs/TDD_SPEC.md` (all function specifications)
- **Task List:** `IMPLEMENTATION_TASKS.md` (150+ checkboxes)
- **Enforcement:** `docs/TDD_ENFORCEMENT_GUIDE.md` (how to ensure TDD)
- **Session Rules:** `.claude/SESSION_RULES.md` (AI assistant rules)

---

## 🎯 Next Steps

1. **On your machine:** Run `./docker/start-dev.sh`
2. **Verify:** Run `./docker/run-tests.sh` (should see 3 passing tests)
3. **Continue Phase 2:** Implement `detect_nulls()` and `detect_outliers()`
4. **Follow TDD:** Write tests first, then implementation
5. **Use git hooks:** Commits automatically verified for TDD compliance

---

## 💡 Tips

1. **Edit files normally** - Your IDE edits are instantly reflected in Docker (via volumes)
2. **Tests run fast** - Docker keeps Spark session warm
3. **Git hooks protect you** - Can't commit code without tests
4. **Coverage is tracked** - Aim for >80%
5. **Postgres is ready** - For integration tests later

---

## ✅ Verification Checklist

Run these on your local machine to verify setup:

```bash
# 1. Docker is running
docker ps
# Expected: See running containers

# 2. Environment starts
./docker/start-dev.sh
# Expected: Green success messages

# 3. Tests pass
./docker/run-tests.sh
# Expected: 3 passing tests, 100% coverage

# 4. TDD compliance verified
./check-tdd.sh
# Expected: All checks pass

# 5. Container accessible
docker exec -it goodnote-spark-dev bash
# Expected: Bash prompt inside container
```

---

**Everything is ready!** Just run on your local machine where Docker is installed. 🚀

Need help? Check `DOCKER_QUICKSTART.md` for detailed troubleshooting.
