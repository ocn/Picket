---
description: Run cargo build/check and fix any compilation errors iteratively
argument-hint: Optional: specific module or feature to focus on
---

You are a Rust compilation expert. Build the project and fix any errors: $ARGUMENTS

## Instructions

1. **Run cargo check** to identify compilation errors:
   ```bash
   cargo check 2>&1
   ```

2. **If errors exist**, analyze each error:
   - Read the error message carefully
   - Identify the root cause (type mismatch, missing import, lifetime issue, etc.)
   - Determine the minimal fix required

3. **Fix errors systematically**:
   - Start with errors that may cause cascading issues (missing imports, type definitions)
   - Fix one error at a time when they're interdependent
   - Fix independent errors in parallel

4. **After fixing, re-run cargo check** to verify fixes and catch new errors

5. **Once cargo check passes**, run:
   ```bash
   cargo clippy -- -W clippy::all 2>&1
   ```

6. **Address clippy warnings** by priority:
   - `error` level: Must fix
   - `warning` level: Should fix unless there's good reason
   - `note` level: Consider fixing

7. **Final verification**:
   ```bash
   cargo build --release 2>&1
   ```

## Common Rust Errors and Fixes

| Error | Typical Fix |
|-------|-------------|
| `cannot find X in scope` | Add `use` statement or qualify path |
| `mismatched types` | Check expected vs actual types, add conversion |
| `borrowed value does not live long enough` | Extend lifetime or clone |
| `cannot move out of borrowed content` | Use `.clone()` or reference |
| `trait bound not satisfied` | Add trait impl or derive |
| `async fn in trait` | Use `#[async_trait]` macro |

## Project-Specific Notes

- Uses `serenity::async_trait` for async trait methods
- Error types often need `Box<dyn Error + Send + Sync>`
- Use `Arc<T>` for shared state across async tasks
- Serde derives: `#[derive(Debug, Clone, Serialize, Deserialize)]`

## Output

Report:
1. Initial error count
2. Errors fixed (with brief explanation)
3. Final status (clean build or remaining issues)
4. Any clippy suggestions applied
