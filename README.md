# Ferrokv

**The embedded, async, Redis-inspired key-value store for Rust.**

Ferrokv is designed to fill a specific gap in the ecosystem: a persistent database that offers the ergonomic semantics of Redis (TTL, atomic increments, lists) but runs **embedded** within your application binary, fully integrated with the `tokio` async runtime.

## The Conviction

We believe you shouldn't have to choose between **data safety** (ACID) and **system performance** (Non-blocking I/O).

If you need a persistent store but don't want the operational overhead of managing an external Redis server sidecar, Ferrokv is built for you. It respects the filesystem as the source of truth and the executor as the source of concurrency.

## Core Guarantees

Ferrokv is built on six non-negotiable pillars:

1.  **Async Native:** Built for `tokio`. I/O operations utilize `io_uring` (on supported Linux) or non-blocking thread pools. `db.set().await` never blocks your executor.
2.  **ACID Compliant:** Durability is not optional. Writes are committed to a Write-Ahead Log (WAL) with strict `fsync` guarantees before acknowledgment.
3.  **Redis Interface:** Implements a high-level interface mirroring familiar Redis commands (`SETEX`, `INCR`, `GET`).
4.  **High Performance:** Uses a Log-Structured Merge-tree (LSM) architecture to maximize write throughput.
5.  **First-Class TTL:** Expiration is handled natively via lazy checks and compaction filtering. No manual "cleanup" threads required.
6.  **Persistent Only:** No volatile-only modes. If the power goes out, your data is safe.

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
ferrokv = "1"
tokio = { version = "1", features = ["full"] }
```

### Usage

Ferrokv feels like a Redis client, but the database lives inside your app.

```rust
use std::time::Duration;

use ferrokv::FerroKv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open the database at the given directory
    // with default settings (creates ./data/ directory if missing)
    let db = FerroKv::open("./data").await?;

    // 2. Standard Key-Value ops
    db.set(b"user:101", b"Alice").await?;

    // 3. With TTL
    // The key auto-expires from disk after 60 seconds.
    db.set_ex(b"session:xyz", b"active", Duration::from_secs(60)).await?;

    // 4. Atomic Increment
    let visits = db.incr(b"site:visits").await?;
    println!("Visits: {visits}");

    // 5. Delete
    db.del(b"user:101").await?;

    Ok(())
}
```

## Architecture

Ferrokv avoids complex locking schemes by using **MVCC (Multi-Version Concurrency Control)**.

- **Writes** go to an append-only WAL and a lock-free Memtable.
- **Reads** are snapshot-isolated; they never block writes.
- **Storage** relies on an LSM-tree structure (SSTables) designed for prefix compression and fast scanning.

For a deep dive into the design philosophy and file format, strictly read the [WHITEPAPER.md](WHITEPAPER.md).

## Allocator Strategy

We use [Mimalloc](https://github.com/microsoft/mimalloc) as the default allocator.

The standard system allocator often suffers from lock contention when used with the "work-stealing" patterns of the `tokio` runtime. Mimalloc is required to eliminate this contention and guarantee the low-latency performance constraints of this project.

## License

This project is licensed under the [MIT License](LICENSE).
