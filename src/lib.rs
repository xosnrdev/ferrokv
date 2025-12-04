//! # Ferrokv
//!
//! **Embedded, async, Redis-inspired key-value store for Rust.**
//!
//! Ferrokv brings Redis semantics to embedded applications with no external server, no network latency,
//! no deployment complexity. Write to disk with ACID guarantees using async I/O that never blocks
//! your `tokio` executor.
//!
//! ## Quick Start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ferrokv::FerroKv;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let db = FerroKv::open("./data").await?;
//!
//!     // Write
//!     db.set(b"user:100", b"Alice").await?;
//!
//!     // Read
//!     let value = db.get(b"user:100").await?;
//!     assert_eq!(value.as_deref(), Some(&b"Alice"[..]));
//!
//!     // Write with TTL
//!     db.set_ex(b"session:xyz", b"active", Duration::from_secs(3600)).await?;
//!
//!     // Atomic increment
//!     let count = db.incr(b"visits").await?;
//!
//!     // Scan
//!     let results = db.scan(..).await?;
//!     for (key, value) in &results {
//!         println!("{} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
//!     }
//!
//!     // Delete
//!     db.del(b"user:100").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Core Guarantees
//!
//! - **ACID Compliant**: Write-Ahead Log with `fsync` before acknowledgment
//! - **Async Native**: Non-blocking I/O via `tokio::fs` — never blocks executor
//! - **Snapshot Isolation**: MVCC ensures readers never block writers
//! - **Native TTL**: Automatic expiration without manual cleanup
//! - **Persistent Only**: Data survives crashes and restarts
//!
//! ## Configuration
//!
//! Tune memory and compaction thresholds:
//!
//! ```no_run
//! # use ferrokv::FerroKv;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = FerroKv::builder("./data")
//!     .memtable_size(128 * 1024 * 1024) // 128MB memtable
//!     .l0_compaction_threshold(8) // Compact after 8 files
//!     .sstable_size(8 * 1024 * 1024) // 8MB SSTable files
//!     .open()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Examples
//!
//! See [`examples/`](https://github.com/xosnrdev/ferrokv/tree/master/examples) for complete scenarios:
//!
//! - [Basic operations](https://github.com/xosnrdev/ferrokv/blob/master/examples/no_ttl.rs)
//! - [TTL expiration](https://github.com/xosnrdev/ferrokv/blob/master/examples/ttl.rs)
//! - [Atomic counters](https://github.com/xosnrdev/ferrokv/blob/master/examples/incr.rs)
//! - [Explicit deletion](https://github.com/xosnrdev/ferrokv/blob/master/examples/del.rs)
//! - [Batch writes](https://github.com/xosnrdev/ferrokv/blob/master/examples/batch.rs)
//! - [Custom configuration](https://github.com/xosnrdev/ferrokv/blob/master/examples/config.rs)
//!
//! ## Error Handling
//!
//! All operations return `Result<T, FerroError>`:
//!
//! ```no_run
//! # use ferrokv::{FerroKv, FerroError};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = FerroKv::open("./data").await?;
//!
//! match db.get(b"key").await {
//!     Ok(Some(value)) => println!("Found: {:?}", value),
//!     Ok(None) => println!("Not found"),
//!     Err(FerroError::Io(e)) => eprintln!("I/O error: {e}"),
//!     Err(FerroError::Corruption(msg)) => eprintln!("Data corruption: {msg}"),
//!     Err(FerroError::InvalidData(msg)) => eprintln!("Invalid data: {msg}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Architecture
//!
//! Ferrokv uses an LSM-tree (Log-Structured Merge-tree) with:
//! - Lock-free in-memory **Memtable** (`SkipMap`)
//! - Immutable on-disk **`SSTables`** (Sorted String Tables)
//! - **Write-Ahead Log** for durability
//! - **MVCC** versioning for snapshot isolation
//! - Background **compaction** for space reclamation
//!
//! Read the [WHITEPAPER](https://github.com/xosnrdev/ferrokv/blob/master/WHITEPAPER.md)
//! for implementation details and design decisions.

#![warn(clippy::pedantic)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::missing_errors_doc,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::too_many_arguments
)]

pub(crate) mod batch;
pub(crate) mod bloom;
pub(crate) mod compaction;
pub(crate) mod config;
pub(crate) mod errors;
pub(crate) mod helpers;
pub(crate) mod memtable;
pub(crate) mod mvcc;
pub(crate) mod sstable;
pub(crate) mod storage;
pub(crate) mod ttl;
pub(crate) mod wal;

pub use batch::WriteBatch;
pub use errors::FerroError;
use mimalloc::MiMalloc;
pub use storage::FerroKv;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
