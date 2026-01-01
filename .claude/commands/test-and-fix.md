---
description: Run cargo test and fix any failing tests iteratively
argument-hint: Optional: specific test name or module to run
---

You are a Rust testing expert. Run tests and fix any failures: $ARGUMENTS

## Instructions

1. **Run tests** to identify failures:
   ```bash
   cargo test $ARGUMENTS 2>&1
   ```

2. **If tests fail**, analyze each failure:
   - Read the assertion error or panic message
   - Compare expected vs actual values
   - Determine if it's a test bug or implementation bug

3. **Fix strategy**:
   - If implementation is wrong: Fix the source code
   - If test is wrong: Fix the test expectations
   - If test is flaky: Add proper async handling or mocking

4. **After fixing, re-run failed tests**:
   ```bash
   cargo test [test_name] 2>&1
   ```

5. **Run full test suite** once individual fixes pass:
   ```bash
   cargo test 2>&1
   ```

## Common Test Issues

| Issue | Fix |
|-------|-----|
| Async test not running | Use `#[tokio::test]` attribute |
| Test data not found | Check relative paths from test location |
| Race condition | Add proper synchronization or use `tokio::sync` |
| Mock not working | Verify mock setup order |

## Test Patterns in This Project

```rust
// Async test
#[tokio::test]
async fn test_async_function() {
    let result = async_fn().await;
    assert!(result.is_ok());
}

// Test with tracing
#[test_log::test(tokio::test)]
async fn test_with_logging() {
    // Logs will be captured
}
```

## Output

Report:
1. Initial test results (passed/failed/ignored)
2. Failures analyzed and fixed
3. Final test status
4. Any tests that need investigation
