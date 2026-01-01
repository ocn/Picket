# Error Handling in Rust

## Table of Contents

1. [Custom Error Types](#custom-error-types)
2. [Using thiserror](#using-thiserror)
3. [Error Conversion](#error-conversion)
4. [Anyhow for Applications](#anyhow-for-applications)
5. [Best Practices](#best-practices)

---

## Custom Error Types

### Define Domain-Specific Errors

```rust
#[derive(Debug)]
pub enum KillmailError {
    NotFound(u64),
    RateLimited { retry_after: u64 },
    NetworkError(String),
    ParseError(String),
}

impl std::fmt::Display for KillmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "Killmail {} not found", id),
            Self::RateLimited { retry_after } => {
                write!(f, "Rate limited, retry after {} seconds", retry_after)
            }
            Self::NetworkError(msg) => write!(f, "Network error: {}", msg),
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for KillmailError {}
```

---

## Using thiserror

The `thiserror` crate eliminates boilerplate for custom errors:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EsiError {
    #[error("ESI request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("Failed to parse ESI response: {0}")]
    ParseError(#[from] serde_json::Error),

    #[error("Resource not found: {resource} with id {id}")]
    NotFound { resource: String, id: u64 },

    #[error("Rate limited by ESI (retry after {0}s)")]
    RateLimited(u64),

    #[error("ESI returned error: {status} - {message}")]
    ApiError { status: u16, message: String },
}
```

### Key Features

- `#[error("...")]` - Automatically implements `Display`
- `#[from]` - Automatically implements `From<T>` for error conversion
- `#[source]` - Wraps underlying errors for the error chain

---

## Error Conversion

### Using `map_err` for Custom Conversion

```rust
async fn fetch_killmail(&self, id: u64) -> Result<Killmail, EsiError> {
    let url = format!("{}/killmails/{}/", self.base_url, id);

    let response = self.client
        .get(&url)
        .send()
        .await?;  // reqwest::Error auto-converts via #[from]

    if response.status() == 404 {
        return Err(EsiError::NotFound {
            resource: "killmail".to_string(),
            id,
        });
    }

    if response.status() == 429 {
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);
        return Err(EsiError::RateLimited(retry_after));
    }

    response.json().await.map_err(EsiError::from)
}
```

### Converting Between Error Types

```rust
// Convert string error to custom type
fn parse_config(path: &str) -> Result<Config, ConfigError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::IoError(e.to_string()))?;

    serde_json::from_str(&content)
        .map_err(|e| ConfigError::ParseError(e.to_string()))
}
```

---

## Anyhow for Applications

For application code (not libraries), `anyhow` simplifies error handling:

```rust
use anyhow::{Context, Result, bail, ensure};

async fn process_killmail(id: u64) -> Result<()> {
    let killmail = fetch_killmail(id)
        .await
        .context("Failed to fetch killmail from ESI")?;

    ensure!(killmail.victim.is_some(), "Killmail has no victim data");

    if killmail.attackers.is_empty() {
        bail!("Killmail {} has no attackers", id);
    }

    Ok(())
}
```

### When to Use Each

| Crate | Use Case |
|-------|----------|
| `thiserror` | Libraries, when callers need to match on error variants |
| `anyhow` | Applications, when you just need to propagate errors with context |
| Manual impl | When you need full control over error behavior |

---

## Best Practices

### DO

```rust
// Return Result for fallible operations
pub fn parse_id(s: &str) -> Result<u64, ParseError> {
    s.parse().map_err(|_| ParseError::InvalidId(s.to_string()))
}

// Add context to errors
let config = load_config()
    .context("Failed to load application config")?;

// Use specific error types in public APIs
pub async fn get_character(&self, id: u64) -> Result<Character, EsiError>
```

### DON'T

```rust
// Don't use unwrap in production code paths
let value = map.get(&key).unwrap();  // BAD: panics if key missing

// Don't use expect without good reason
let data = response.json().await.expect("should parse");  // BAD

// Don't ignore errors
let _ = send_message().await;  // BAD: silently ignores failure
```

### When `unwrap`/`expect` IS Acceptable

```rust
// 1. Tests
#[test]
fn test_parse() {
    let result = parse("valid").unwrap();
    assert_eq!(result, expected);
}

// 2. Provably infallible
let regex = Regex::new(r"^\d+$").unwrap();  // Compile-time constant pattern

// 3. Setup code where failure is fatal
let client = Client::builder()
    .build()
    .expect("Failed to build HTTP client");  // App can't run without this
```

---

## Error Handling Checklist

- [ ] All public functions that can fail return `Result`
- [ ] Custom error types are defined for domain-specific failures
- [ ] Errors include enough context to debug
- [ ] No `unwrap()` or `expect()` in normal code paths
- [ ] Error messages are actionable
- [ ] Error variants are specific enough for callers to handle