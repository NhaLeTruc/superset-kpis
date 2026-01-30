# Documentation Policy

## Rule: All Markdown Files Must Be in docs/

**MANDATORY:** All `.md` files (except `README.md`) MUST be placed in the `docs/` directory.

### Exception

- `README.md` - Must stay in project root (standard convention)

### Rationale

1. **Organization** - Keeps repository root clean
2. **Discoverability** - All documentation in one place
3. **Consistency** - Clear structure for team members
4. **Maintainability** - Easier to manage and update docs

### Directory Structure

```
superset-kpis/
├── README.md                      ✅ Root (exception)
├── docs/                          ✅ All other .md files here
   ├── TDD_SPEC.md
   ├── TDD_ENFORCEMENT_GUIDE.md
   ├── IMPLEMENTATION_PLAN.md
   ├── IMPLEMENTATION_TASKS.md
   ├── ARCHITECTURE.md
   ├── PROJECT_STRUCTURE.md
   ├── SETUP_GUIDE.md                 # Troubleshooting guide
   └── SUPERSET_DASHBOARDS.md         # Dashboard specifications
```

### Enforcement

**Automated:**
- Git pre-commit hook checks for `.md` files in root (except README.md)
- Commits are blocked if rule is violated

**Manual Check:**
```bash
# Check for violations
find . -maxdepth 1 -name "*.md" ! -name "README.md"
# Should return nothing
```

### If You Create New Documentation

1. **Create in docs/** - Always create new `.md` files in `docs/`
   ```bash
   touch docs/NEW_FEATURE.md
   ```

2. **Reference with path** - When linking to docs, use `docs/` prefix
   ```markdown
   See [Feature Docs](docs/NEW_FEATURE.md) for details.
   ```

3. **Update README.md** - Add link in main README if important

### Migrating Existing Files

If you find a `.md` file in the root (except README.md):

```bash
# 1. Move file to docs/
git mv FILENAME.md docs/

# 2. Update all references (search project-wide)
grep -r "FILENAME.md" .

# 3. Update references to include docs/
# Change: FILENAME.md
# To: docs/FILENAME.md

# 4. Commit
git add -A
git commit -m "Move FILENAME.md to docs/ per documentation policy"
```

### Pre-commit Hook Enforcement

The `.githooks/pre-commit` file includes this check:

```bash
# Check for .md files in root (except README.md)
ROOT_MD_FILES=$(git diff --cached --name-only --diff-filter=ACM |
                grep -E '^[^/]+\.md$' |
                grep -v '^README\.md$')

if [ ! -z "$ROOT_MD_FILES" ]; then
    echo "❌ ERROR: Markdown files must be in docs/ directory"
    echo "Found: $ROOT_MD_FILES"
    echo ""
    echo "Fix: git mv <file>.md docs/"
    exit 1
fi
```

### Exceptions Process

If you have a legitimate reason for a root-level `.md` file:

1. Discuss with team/maintainer
2. Document the exception here
3. Update pre-commit hook to allow specific file

**Current Exceptions:**
- `README.md` - Project entry point (standard convention)

---

**Last Updated:** 2025-11-13
**Status:** Active and enforced
