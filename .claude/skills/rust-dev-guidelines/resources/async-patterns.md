# Async Patterns with Tokio

## Table of Contents

1. [Basic Async/Await](#basic-asyncawait)
2. [Spawning Tasks](#spawning-tasks)
3. [Concurrency Primitives](#concurrency-primitives)
4. [Channels](#channels)
5. [Timeouts and Delays](#timeouts-and-delays)
6. [Select and Racing](#select-and-racing)
7. [Shared State](#shared-state)

---

## Basic Async/Await

### Async Functions

```rust
// Async function that returns a Future
pub async fn fetch_data(url: &str) -> Result<Data, Error> {
    let response = reqwest::get(url).await?;
    let data = response.json().await?;
    Ok(data)
}

// Calling async functions requires .await
let data = fetch_data("https://api.example.com").await?;
```

### The `#[tokio::main]` Macro

```rust
#[tokio::main]
async fn main() {
    // Now we can use .await in main
    run().await;
}

// Equivalent to:
fn main() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run())
}
```

---

## Spawning Tasks

### Fire-and-Forget

```rust
// Spawn a task that runs independently
tokio::spawn(async move {
    if let Err(e) = background_work().await {
        error!("Background task failed: {}", e);
    }
});
// Execution continues immediately
```

### With Join Handle

```rust
// Spawn and wait for result
let handle = tokio::spawn(async move {
    expensive_computation().await
});

// Do other work...

// Wait for task to complete
let result = handle.await?;
```

### Spawning Multiple Tasks

```rust
let mut handles = Vec::new();

for item in items {
    let handle = tokio::spawn(async move {
        process_item(item).await
    });
    handles.push(handle);
}

// Wait for all tasks
for handle in handles {
    if let Err(e) = handle.await {
        error!("Task failed: {}", e);
    }
}
```

---

## Concurrency Primitives

### `join!` - Run Concurrently, Wait for All

```rust
use tokio::join;

// Both futures run concurrently
let (user, permissions) = join!(
    fetch_user(user_id),
    fetch_permissions(user_id)
);

// Continues only after BOTH complete
```

### `try_join!` - Short-Circuit on Error

```rust
use tokio::try_join;

// If either fails, returns early with error
let (user, perms) = try_join!(
    fetch_user(user_id),
    fetch_permissions(user_id)
)?;
```

---

## Channels

### MPSC (Multi-Producer, Single-Consumer)

```rust
use tokio::sync::mpsc;

// Create channel with buffer size
let (tx, mut rx) = mpsc::channel::<Killmail>(100);

// Producer
tokio::spawn(async move {
    tx.send(killmail).await.unwrap();
});

// Consumer
while let Some(km) = rx.recv().await {
    process(km).await;
}
```

### Oneshot (Single Value)

```rust
use tokio::sync::oneshot;

let (tx, rx) = oneshot::channel();

// Send exactly one value
tokio::spawn(async move {
    let result = compute().await;
    let _ = tx.send(result);
});

// Receive the value
let result = rx.await?;
```

### Broadcast (Multi-Consumer)

```rust
use tokio::sync::broadcast;

let (tx, _) = broadcast::channel::<Event>(16);

// Multiple subscribers
let mut rx1 = tx.subscribe();
let mut rx2 = tx.subscribe();

// All subscribers receive the message
tx.send(event)?;
```

---

## Timeouts and Delays

### Sleep

```rust
use tokio::time::{sleep, Duration};

// Pause execution
sleep(Duration::from_secs(1)).await;
```

### Timeout

```rust
use tokio::time::timeout;

// Fail if operation takes too long
match timeout(Duration::from_secs(5), fetch_data()).await {
    Ok(result) => handle(result?),
    Err(_) => error!("Operation timed out"),
}
```

### Interval

```rust
use tokio::time::{interval, Duration};

let mut interval = interval(Duration::from_secs(60));

loop {
    interval.tick().await;
    perform_periodic_task().await;
}
```

---

## Select and Racing

### `select!` - First Future Wins

```rust
use tokio::select;

select! {
    result = fetch_primary() => {
        handle_primary(result).await;
    }
    result = fetch_fallback() => {
        handle_fallback(result).await;
    }
    _ = sleep(Duration::from_secs(10)) => {
        error!("Both fetches timed out");
    }
}
```

### With Cancellation

```rust
use tokio_util::sync::CancellationToken;

let token = CancellationToken::new();
let token_clone = token.clone();

tokio::spawn(async move {
    select! {
        _ = long_running_task() => {
            info!("Task completed");
        }
        _ = token_clone.cancelled() => {
            info!("Task cancelled");
        }
    }
});

// Cancel the task
token.cancel();
```

---

## Shared State

### Arc + RwLock Pattern

```rust
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AppState {
    subscriptions: RwLock<HashMap<GuildId, Vec<Subscription>>>,
}

impl AppState {
    // Read access (many readers allowed)
    pub async fn get_subscriptions(&self, guild_id: GuildId) -> Vec<Subscription> {
        let subs = self.subscriptions.read().await;
        subs.get(&guild_id).cloned().unwrap_or_default()
    }

    // Write access (exclusive)
    pub async fn update_subscription(&self, guild_id: GuildId, sub: Subscription) {
        let mut subs = self.subscriptions.write().await;
        subs.entry(guild_id).or_default().push(sub);
    }
}

// Usage
let state = Arc::new(AppState::new());
let state_clone = state.clone();

tokio::spawn(async move {
    state_clone.update_subscription(guild_id, sub).await;
});
```

### When to Use std vs tokio Sync Primitives

| Use | When |
|-----|------|
| `std::sync::RwLock` | Lock held briefly, not across await points |
| `tokio::sync::RwLock` | Lock may be held across await points |
| `std::sync::Mutex` | Simple exclusive access, no async |
| `tokio::sync::Mutex` | Need to hold lock across await |

---

## Common Patterns in This Project

### Main Loop Pattern

```rust
loop {
    match listener.listen().await {
        Ok(Some(data)) => {
            // Process data
            process(&data).await;
            sleep(Duration::from_secs(1)).await;
        }
        Ok(None) => {
            // No data, poll again
            sleep(Duration::from_secs(1)).await;
        }
        Err(e) => {
            error!("Error: {}", e);
            sleep(Duration::from_secs(5)).await;  // Backoff on error
        }
    }
}
```

### Graceful Shutdown

```rust
use tokio::signal;

tokio::select! {
    _ = run_server() => {
        info!("Server stopped");
    }
    _ = signal::ctrl_c() => {
        info!("Received Ctrl+C, shutting down");
    }
}

// Cleanup
cleanup().await;
```