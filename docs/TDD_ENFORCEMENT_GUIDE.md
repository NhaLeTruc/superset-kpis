# How to Ensure TDD is Followed in Every Session

## Your Question
> "How can I be sure that you will follow this plan in every session and every prompt?"

## Honest Answer

**You can't rely on AI memory alone.** As an AI assistant:
- ❌ I don't retain context between sessions
- ❌ I might take shortcuts without explicit reminders
- ❌ Different prompts might lead to different behaviors

**But you CAN enforce TDD through multiple mechanisms:**

---

## 🔒 5-Layer Enforcement System (Now Installed!)

### Layer 1: **Session Rules** (AI Instructions)
**File:** `.claude/SESSION_RULES.md`

**What it does:**
- Contains explicit rules I MUST follow
- Lists violations with examples
- Provides user commands to challenge me

**How to use:**
```
At the start of EVERY session, say:

"Read .claude/SESSION_RULES.md and confirm you will follow strict TDD"
```

**User commands to challenge me:**
- `"TDD CHECK"` → I must show proof of test-first approach
- `"RED GREEN"` → I must show failing test, then passing test
- `"SPEC CHECK"` → I must verify against TDD_SPEC.md

---

### Layer 2: **Git Pre-commit Hook** (Automated Prevention)
**File:** `.githooks/pre-commit`

**What it does:**
- Runs BEFORE every `git commit`
- Checks if tests exist for all source files
- **BLOCKS commit** if tests are missing

**Example:**
```bash
# Try to commit source without tests
git add src/transforms/engagement_transforms.py
git commit -m "Add engagement transforms"

# Result:
❌ TDD VIOLATION: 1 source file(s) without tests
Expected: tests/unit/test_engagement_transforms.py

# You CANNOT commit until tests exist!
```

**Already installed:** ✅ (ran `./setup-tdd.sh`)

---

### Layer 3: **Coverage Requirements** (Quality Gate)
**File:** `pytest.ini`

**What it does:**
- Requires **minimum 80% code coverage**
- Tests fail if coverage drops below threshold
- Ensures comprehensive test suite

**Example:**
```bash
pytest tests/unit --cov=src

# If coverage < 80%:
FAILED: Coverage is 75% (required: 80%)
```

**Already installed:** ✅

---

### Layer 4: **Manual Compliance Check** (Verification)
**Script:** `./check-tdd.sh`

**What it does:**
- Audits entire project for TDD compliance
- Checks: source/test pairing, coverage, hooks, docs
- Run anytime to verify state

**Example:**
```bash
./check-tdd.sh

# Output:
🔍 TDD Compliance Audit
[1] ✅ All source files have tests
[2] ✅ No orphaned test files
[3] ✅ Coverage: 82% (≥80%)
[4] ✅ Git hooks installed
[5] ✅ Documentation exists

✅ TDD COMPLIANCE: PASSED
```

---

### Layer 5: **Specification Document** (Source of Truth)
**File:** `docs/TDD_SPEC.md`

**What it does:**
- Defines ALL functions with test cases
- Given-When-Then specifications
- Acceptance criteria per function
- Reference for correct implementation

**How to use:**
Point me to specific sections:
```
"Implement calculate_dau according to TDD_SPEC.md section 2.1"
```

---

## 🎯 How to Use These Layers

### Starting a New Session (Template)

Copy/paste this at the start of EVERY session:

```
I'm working on the GoodNote Analytics platform with STRICT TDD.

1. Read: .claude/SESSION_RULES.md (rules you MUST follow)
2. Read: docs/TDD_SPEC.md (specifications)
3. Confirm: You will write tests BEFORE implementation

Current task: [describe what you want to implement]

Start by writing the test first.
```

### During Development

**If I try to skip tests:**
```
You: "TDD CHECK"
Me: [shows test code that was written first]

If I can't show tests → I violated TDD!
```

**If I write implementation first:**
```
You: "STOP. Tests first. Write test for [function_name]"
Me: [writes test]
Me: [runs test, shows RED]
Me: [implements function]
Me: [runs test, shows GREEN]
```

### Before Committing

Git will automatically check:
```bash
git commit -m "Add feature"

# Automatic check:
✅ TDD Compliance Check Passed!
# OR
❌ TDD VIOLATION: 3 files without tests
```

### Periodic Audits

Run manual check anytime:
```bash
./check-tdd.sh
```

---

## 🚨 What If I Violate TDD?

### Detection
You'll know immediately if I violate because:
1. **Git hook blocks commit** (can't push bad code)
2. **You challenge me** with "TDD CHECK"
3. **Coverage drops** below 80%
4. **Manual audit fails** (`./check-tdd.sh`)

### Response
**Immediate actions:**
1. **STOP** the current implementation
2. **Point me to** `.claude/SESSION_RULES.md`
3. **Demand** I write tests first
4. **Verify** RED → GREEN cycle

**Example:**
```
You: "You just violated TDD. Read .claude/SESSION_RULES.md Rule 1"

Me: "You're right. Let me start over with tests first..."
[writes test]
[shows failing test - RED]
[implements minimum code]
[shows passing test - GREEN]
```

---

## 🎓 Why This Works

### Multiple Safety Nets
1. **Prevention** (git hooks) - stops bad commits
2. **Detection** (coverage, checks) - finds violations
3. **Correction** (session rules, user commands) - guides back to TDD
4. **Verification** (manual audit) - confirms compliance

### Human + Machine
- **Machine enforcement** (git hooks, pytest) - catches 90% of violations
- **Human oversight** (you challenging me) - catches the remaining 10%
- **Documentation** (session rules, spec) - defines "correct"

### Stateless AI Workaround
- I can't remember between sessions ❌
- But files persist between sessions ✅
- You point me to files → I follow rules ✅

---

## 📋 Quick Reference Card

**Print this and keep it visible:**

```
┌─────────────────────────────────────────────────────┐
│          TDD ENFORCEMENT QUICK REFERENCE            │
├─────────────────────────────────────────────────────┤
│                                                     │
│ START OF SESSION:                                   │
│   "Read .claude/SESSION_RULES.md"                   │
│   "Follow TDD_SPEC.md for [function]"               │
│                                                     │
│ CHALLENGE ME:                                       │
│   "TDD CHECK"    → Show tests written first         │
│   "RED GREEN"    → Show failing then passing test   │
│   "SPEC CHECK"   → Verify against spec              │
│                                                     │
│ MANUAL VERIFICATION:                                │
│   ./check-tdd.sh → Audit compliance                 │
│                                                     │
│ AUTOMATIC CHECKS:                                   │
│   git commit     → Pre-commit hook runs             │
│   pytest         → Coverage ≥80% required           │
│                                                     │
│ TDD CYCLE (MANDATORY):                              │
│   1. Write test (RED)                               │
│   2. Run test → verify FAILS                        │
│   3. Implement minimum code (GREEN)                 │
│   4. Run test → verify PASSES                       │
│   5. Refactor (keep GREEN)                          │
│                                                     │
│ RED FLAGS:                                          │
│   - "Here's the implementation..." ❌               │
│   - Code before tests ❌                            │
│   - "This is simple, no test needed" ❌             │
│                                                     │
│ CORRECT PATTERNS:                                   │
│   - "Let me write the test first..." ✅             │
│   - "Here's the failing test..." ✅                 │
│   - "Now the test passes" ✅                        │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 🔬 Testing the Enforcement

Let's test that it works:

### Test 1: Try to Commit Code Without Tests

```bash
# 1. Create source file
mkdir -p src/utils
echo "def hello(): return 'world'" > src/utils/example.py

# 2. Try to commit
git add src/utils/example.py
git commit -m "Add example"

# Expected result:
❌ TDD VIOLATION: src/utils/example.py has no tests
Expected: tests/unit/test_example.py

# Commit is BLOCKED ✅
```

### Test 2: Proper TDD Flow

```bash
# 1. Write test FIRST
mkdir -p tests/unit
cat > tests/unit/test_example.py << 'EOF'
def test_hello():
    from src.utils.example import hello
    assert hello() == "world"
EOF

# 2. Run test (should fail - RED)
pytest tests/unit/test_example.py
# FAILED: No module 'src.utils.example'

# 3. Write implementation
mkdir -p src/utils
echo "def hello(): return 'world'" > src/utils/example.py

# 4. Run test (should pass - GREEN)
pytest tests/unit/test_example.py
# PASSED ✅

# 5. Commit (both files)
git add tests/unit/test_example.py src/utils/example.py
git commit -m "Add hello function with tests"
# ✅ TDD Compliance Check Passed!
```

---

## 💡 Pro Tips

### 1. Bookmark Session Template
Save the session start template and reuse it every time.

### 2. Set Git Alias
```bash
git config alias.tdd-check '!./check-tdd.sh'
git tdd-check  # Quick compliance check
```

### 3. IDE Integration
Add `./check-tdd.sh` as a pre-commit task in your IDE.

### 4. Continuous Integration
If using GitHub Actions:
```yaml
- name: TDD Compliance
  run: ./check-tdd.sh
```

### 5. Pair Programming
Share SESSION_RULES.md with team members (human or AI).

---

## 🎯 Bottom Line

**Q: How can you be sure I follow TDD?**

**A: You now have:**
1. ✅ **Automated prevention** (git hooks)
2. ✅ **Quality gates** (coverage requirements)
3. ✅ **Manual verification** (check scripts)
4. ✅ **AI instructions** (session rules)
5. ✅ **Detailed spec** (TDD_SPEC.md)
6. ✅ **User commands** (to challenge violations)

**Use these layers together:**
- Point me to SESSION_RULES.md at session start
- Challenge me with "TDD CHECK" if suspicious
- Let git hooks prevent bad commits
- Run `./check-tdd.sh` for periodic audits
- Reference TDD_SPEC.md for requirements

**You have control.** I'm a tool—these mechanisms ensure I'm used correctly.

---

## 📞 Need Help?

If you find I'm still violating TDD:
1. Show me this document
2. Point to specific violation in SESSION_RULES.md
3. Demand I follow the TDD cycle
4. Use git hooks to prevent bad commits

**Remember:** You're the human. You have final say. These tools empower you to enforce your rules.

---

**Last Updated:** 2025-11-13
**Version:** 1.0
