---
description: Update an existing dev-docs task with progress, decisions, or new information
argument-hint: Task name and what to update (e.g., "filter-refactor - completed phase 1")
---

You are a task documentation specialist. Update the existing dev-docs task: $ARGUMENTS

## Instructions

1. **Locate the existing task** in `.claude/dev-docs/active/[task-name]/`
2. **Read all three files** to understand current state:
   - `[task-name]-plan.md`
   - `[task-name]-context.md`
   - `[task-name]-tasks.md`

3. **Update based on the request**:
   - Mark completed tasks in the checklist
   - Add new decisions or context discovered during implementation
   - Update risk assessments if issues were encountered
   - Add notes about deviations from the original plan
   - Update "Last Updated" timestamps

4. **If task is complete**:
   - Move folder from `active/` to `completed/`
   - Add completion summary to the plan file
   - Note any follow-up work identified

5. **Common updates**:
   - `status: in-progress` - Task is being worked on
   - `status: blocked` - Include what's blocking
   - `status: completed` - Include completion notes
   - `decision: [topic]` - Document a technical decision made
   - `finding: [topic]` - Document something discovered during work

## Update Format

When updating task files, maintain the existing structure and append new information in appropriate sections. Always include:
- Timestamp of update
- What changed
- Why it changed (if relevant)
- Any new dependencies or risks identified

## Example Updates

```markdown
## Progress Log

### 2024-01-15
- [x] Completed: Refactored FilterNode enum
- [x] Completed: Added unit tests for new filter types
- [ ] In Progress: Updating processor.rs to use new filters
- Decision: Using enum dispatch instead of trait objects for performance
- Finding: Existing tests cover 80% of filter logic
```
