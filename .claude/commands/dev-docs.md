---
description: Create a comprehensive strategic plan with structured task breakdown
argument-hint: Describe what you need planned (e.g., "add ship name filtering", "refactor processor module")
---

You are an elite strategic planning specialist for Rust projects. Create a comprehensive, actionable plan for: $ARGUMENTS

## Instructions

1. **Analyze the request** and determine the scope of planning needed
2. **Examine relevant files** in the codebase to understand current state
3. **Create a structured plan** with:
   - Executive Summary
   - Current State Analysis
   - Proposed Future State
   - Implementation Phases (broken into sections)
   - Detailed Tasks (actionable items with clear acceptance criteria)
   - Risk Assessment and Mitigation Strategies
   - Success Metrics

4. **Task Breakdown Structure**:
   - Each major section represents a phase or component
   - Number and prioritize tasks within sections
   - Include clear acceptance criteria for each task
   - Specify dependencies between tasks
   - Estimate effort levels (S/M/L/XL)

5. **Create task management structure**:
   - Create directory: `.claude/dev-docs/active/[task-name]/`
   - Generate three files:
     - `[task-name]-plan.md` - The comprehensive plan
     - `[task-name]-context.md` - Key files, decisions, dependencies
     - `[task-name]-tasks.md` - Checklist format for tracking progress
   - Include "Last Updated: YYYY-MM-DD" in each file

## Quality Standards
- Plans must be self-contained with all necessary context
- Use clear, actionable language
- Include specific technical details (file paths, function names)
- Consider Rust idioms and async patterns
- Account for potential compilation errors and edge cases

## Context References
- Check `.claude/dev-docs/OVERVIEW.md` for architecture overview
- Consult `.claude/skills/rust-dev-guidelines/SKILL.md` for Rust patterns
- Reference `.claude/dev-docs/WORKSTREAM-*.md` for related work
- Use existing code patterns from `src/` as reference

## Project-Specific Considerations
- This is a Rust async project using Tokio
- Discord integration via Serenity 0.11
- ESI API integration with caching
- Filter-based killmail matching system

**Note**: This command creates persistent task structure that survives context resets.
