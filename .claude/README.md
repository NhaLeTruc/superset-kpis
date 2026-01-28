# Claude AI Session Configuration

This directory contains instructions that Claude AI must follow when working on this project.

## Purpose

Ensure **consistent TDD adherence** across:
- Multiple chat sessions
- Different prompts
- Long development periods

## Files

### SESSION_RULES.md
**CRITICAL:** Claude MUST read this file at the start of EVERY session.

Contains:
- TDD enforcement rules
- Mandatory checklist per function
- Violation examples
- User commands to challenge non-compliance

### How It Works

1. **User starts new session** → References `.claude/SESSION_RULES.md`
2. **Claude reads rules** → Understands TDD requirements
3. **Claude enforces TDD** → Tests before code, every time
4. **User challenges** → If Claude violates, user says "TDD CHECK"

## User Commands

Use these commands to enforce TDD:

| Command | Claude's Response |
|---------|------------------|
| `"TDD CHECK"` | Show proof of test-first approach for last change |
| `"RED GREEN"` | Show the failing test (RED), then passing test (GREEN) |
| `"SPEC CHECK"` | Verify last change matches TDD_SPEC.md |
| `"SESSION RESET"` | Re-read SESSION_RULES.md and confirm TDD mode |

## Starting a New Session

**Template for user:**

```
I'm working on the GoodNote challenge following strict TDD.

1. Read: .claude/SESSION_RULES.md (TDD rules you MUST follow)
2. Read: docs/TDD_SPEC.md (function specifications)
3. Confirm: You will follow test-first approach

We are implementing [specific function/task]. Let's start with tests.
```

## For Developers

This directory ensures Claude AI assistants follow project conventions.

**Why this matters:**
- AI conversations are stateless between sessions
- Developers may forget to mention TDD in every prompt
- Enforcement must be automated and consistent

**How to enforce:**
- Reference these files in your prompts
- Use user commands to challenge violations
- Git hooks provide additional safety net

## See Also

- `docs/TDD_SPEC.md` - Complete TDD specifications
- `scripts/setup-tdd.sh` - Install TDD enforcement tools
- `scripts/check-tdd.sh` - Verify TDD compliance
- `.githooks/pre-commit` - Prevent commits without tests
