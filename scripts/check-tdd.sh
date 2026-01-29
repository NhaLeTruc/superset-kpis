#!/bin/bash
# Manual TDD compliance checker
# Use this to verify TDD adherence at any time

echo "🔍 TDD Compliance Audit"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASSED=0
FAILED=0
WARNINGS=0

# Check 1: Do all source files have tests?
echo "${BLUE}[1] Checking: Do all source files have corresponding tests?${NC}"
SRC_FILES=$(find src -name '*.py' -not -name '__init__.py' -not -path '*/config/*')

while IFS= read -r src_file; do
    if [ -z "$src_file" ]; then
        continue
    fi

    filename=$(basename "$src_file" .py)

    if [[ "$src_file" == src/transforms/* ]]; then
        test_file="tests/unit/test_${filename}.py"
    elif [[ "$src_file" == src/utils/* ]]; then
        test_file="tests/unit/test_${filename}.py"
    elif [[ "$src_file" == src/jobs/* ]]; then
        test_file="tests/integration/test_${filename}.py"
    else
        test_file="tests/unit/test_${filename}.py"
    fi

    if [ -f "$test_file" ]; then
        echo "  ${GREEN}✅${NC} $src_file → $test_file"
        PASSED=$((PASSED + 1))
    else
        echo "  ${RED}❌${NC} $src_file → ${YELLOW}MISSING: $test_file${NC}"
        FAILED=$((FAILED + 1))
    fi
done <<< "$SRC_FILES"

echo ""

# Check 2: Are there orphaned test files?
echo "${BLUE}[2] Checking: Are there test files without corresponding source?${NC}"
TEST_FILES=$(find tests -name 'test_*.py' -not -name 'conftest.py')

ORPHANED=0
while IFS= read -r test_file; do
    if [ -z "$test_file" ]; then
        continue
    fi

    filename=$(basename "$test_file" .py | sed 's/^test_//')

    # Check possible source locations
    possible_src_files=(
        "src/transforms/${filename}.py"
        "src/utils/${filename}.py"
        "src/jobs/${filename}.py"
        "src/${filename}.py"
    )

    found=false
    for src_file in "${possible_src_files[@]}"; do
        if [ -f "$src_file" ]; then
            found=true
            break
        fi
    done

    if [ "$found" = true ]; then
        echo "  ${GREEN}✅${NC} $test_file → $src_file"
    else
        echo "  ${YELLOW}⚠️${NC}  $test_file → ${YELLOW}No matching source file${NC}"
        WARNINGS=$((WARNINGS + 1))
        ORPHANED=$((ORPHANED + 1))
    fi
done <<< "$TEST_FILES"

echo ""

# Check 3: Test coverage
echo "${BLUE}[3] Checking: Test coverage (target: >80%)${NC}"
if command -v pytest &> /dev/null; then
    if [ -f "pytest.ini" ]; then
        # Run pytest with coverage
        coverage_output=$(pytest --cov=src --cov-report=term-missing --tb=no -q 2>&1 || true)

        # Extract coverage percentage
        coverage_pct=$(echo "$coverage_output" | grep -oP 'TOTAL.*\K\d+(?=%)' | tail -1)

        if [ -n "$coverage_pct" ]; then
            if [ "$coverage_pct" -ge 80 ]; then
                echo "  ${GREEN}✅ Coverage: ${coverage_pct}% (≥80% required)${NC}"
            else
                echo "  ${RED}❌ Coverage: ${coverage_pct}% (<80% required)${NC}"
                FAILED=$((FAILED + 1))
            fi
        else
            echo "  ${YELLOW}⚠️  Could not determine coverage${NC}"
            WARNINGS=$((WARNINGS + 1))
        fi
    else
        echo "  ${YELLOW}⚠️  pytest.ini not found (run scripts/setup-tdd.sh)${NC}"
        WARNINGS=$((WARNINGS + 1))
    fi
else
    echo "  ${YELLOW}⚠️  pytest not installed${NC}"
    WARNINGS=$((WARNINGS + 1))
fi

echo ""

# Check 4: Are git hooks installed?
echo "${BLUE}[4] Checking: Are TDD enforcement hooks installed?${NC}"
hooks_path=$(git config core.hooksPath)
if [ "$hooks_path" = ".githooks" ]; then
    if [ -x ".githooks/pre-commit" ]; then
        echo "  ${GREEN}✅ Pre-commit hook installed and executable${NC}"
    else
        echo "  ${RED}❌ Pre-commit hook not executable${NC}"
        echo "     Fix: chmod +x .githooks/pre-commit"
        FAILED=$((FAILED + 1))
    fi
else
    echo "  ${RED}❌ Git hooks not configured${NC}"
    echo "     Fix: scripts/setup-tdd.sh"
    FAILED=$((FAILED + 1))
fi

echo ""

# Check 5: Key documentation exists?
echo "${BLUE}[5] Checking: Required TDD documentation exists?${NC}"
required_docs=(
    ".claude/SESSION_RULES.md"
    "docs/TESTING_GUIDE.md"
    "tests/conftest.py"
)

for doc in "${required_docs[@]}"; do
    if [ -f "$doc" ]; then
        echo "  ${GREEN}✅${NC} $doc"
    else
        echo "  ${RED}❌${NC} ${YELLOW}MISSING: $doc${NC}"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Final Summary
if [ $FAILED -eq 0 ]; then
    echo "${GREEN}✅ TDD COMPLIANCE: PASSED${NC}"
    echo "   All checks passed!"
    if [ $WARNINGS -gt 0 ]; then
        echo "   ${YELLOW}⚠️  $WARNINGS warning(s) - review above${NC}"
    fi
    exit 0
else
    echo "${RED}❌ TDD COMPLIANCE: FAILED${NC}"
    echo "   $FAILED check(s) failed"
    if [ $WARNINGS -gt 0 ]; then
        echo "   $WARNINGS warning(s)"
    fi
    echo ""
    echo "Action required:"
    echo "  1. Review failures above"
    echo "  2. Write missing tests"
    echo "  3. Run: scripts/setup-tdd.sh (if hooks not installed)"
    echo "  4. Run: scripts/check-tdd.sh (to verify fixes)"
    exit 1
fi
