# Ferrokv: The Async, Embedded, Redis-Inspired Store

## 1\. Abstract

Ferrokv ("Iron Key-Value") is a high-performance, persistent-only, embedded key-value store designed for the Rust ecosystem. It fills a critical gap in the current database landscape: the need for a **Redis-inspired interface** that runs **embedded** within the application binary, while maintaining strict **ACID compliance** and **asynchronous non-blocking I/O**.

Unlike existing solutions which often compromise on durability for speed, or force synchronous blocking operations, Ferrokv leverages a modern Log-Structured Merge-tree (LSM) architecture to provide high write throughput, native Time-To-Live (TTL) support, and profound data safety without the operational overhead of managing an external database server.

## 2\. The Vision & Problem Statement

### The "Sidecar" Problem

Modern distributed systems frequently require a persistent key-value store for caching, session management, and state coordination. Developers often default to **Redis**. While Redis is excellent, it introduces significant operational complexity:

- It requires a separate process (sidecar) or server.
- It introduces network latency (TCP/IP).
- It adds deployment friction.

### The Embedded Gap

Existing embedded Rust databases (e.g., `Sled`, `Redb`, `RocksDB` bindings) excel at storage but fail to meet the specific requirements of modern async applications:

1.  **Blocking I/O:** Most use synchronous APIs that block the generic executor thread.
2.  **No Native TTL:** Developers must implement complex manual expiration logic.
3.  **Complex APIs:** They expose low-level B-Tree/Page cursors rather than high-level commands (`SET`, `GET`, `EXPIRE`).

### The Ferrokv Solution

Ferrokv removes the sidecar. It offers the **semantics of Redis** (including TTL and atomic increments) with the **simplicity of a library**, all backed by a **persistent, ACID-compliant disk engine** that plays perfectly with Rust’s `async/await` ecosystem.

## 3\. Core Constraints & Guarantees

We adhere to six non-negotiable pillars that define the engineering constraints of Ferrokv:

| Pillar                   | Implementation Strategy                                                                                                       |
| :----------------------- | :---------------------------------------------------------------------------------------------------------------------------- |
| **1. Highly Performant** | **LSM-Tree Architecture**: Converts random writes into sequential disk I/O.                                                   |
| **2. ACID Compliant**    | **Write-Ahead Log (WAL)** with Group Commit. Data is durable before it is visible.                                            |
| **3. Concurrent**        | **MVCC** (Multi-Version Concurrency Control). Readers never block writers; writers never block readers.                       |
| **4. Non-blocking**      | **Async-First Design**. All I/O operations are offloaded (via `tokio::fs` or `io_uring`), ensuring the executor never stalls. |
| **5. Persistent-only**   | No "in-memory only" modes. We respect the filesystem as the source of truth.                                                  |
| **6. TTL Support**       | **First-Class Citizen**. Expiration is handled natively via lazy checks and compaction filtering.                             |

## 4\. System Architecture

### 4.1 Storage Engine: The Async LSM-Tree

Ferrokv utilizes a **Log-Structured Merge-tree (LSM)** rather than a B-Tree.

- **Write Path:** Writes are appended to a Write-Ahead Log (WAL) for durability, then inserted into an in-memory **Memtable** (Lock-free SkipList).
- **Read Path:** Reads query the Memtable first, then cascade through immutable **SSTables (Sorted String Tables)** on disk.
- **Compaction:** Background threads merge older SSTables to reclaim space and enforce TTL eviction.

### 4.2 The Concurrency Model (MVCC)

To support high concurrency without global locking:

- **Versioning:** Every key update creates a new version of the record (e.g., `Key_v10`).
- **Snapshot Isolation:** A read transaction sees a consistent snapshot of the database at the time it started, ignoring newer writes.
- **Watermarking:** A background process tracks the oldest active transaction to safely garbage-collect obsolete versions.

### 4.3 The "Zero-Cost" TTL Strategy

Handling Time-To-Live without "stop-the-world" pauses is achieved through a three-tier strategy:

1.  **Lazy Eviction:** On `GET`, if `current_time > expiry`, return `None` and schedule a deletion.
2.  **Compaction Filtering:** Expired keys are dropped during background merge operations, never copied to new files.
3.  **Expiry Min-Heap:** A lightweight in-memory structure tracks imminent expirations for proactive cleanup of cold data.

## 5\. File Format Specification: FerroSST

The on-disk format (`.sst`) is designed for zero-copy reads and fast scanning.

**Block Layout:**

```text
[ Data Block 1 | Data Block 2 | ... | Filter Block | Index Block | Footer ]
```

**Record Structure:**
Crucially, the **TTL Timestamp** is stored in the record header, allowing the scanner to determine expiration _before_ reading the full value payload.

```text
| Flags (1B) | Expire_At (8B, Opt) | Key_Len | Val_Len | Key_Bytes | Value_Bytes |
```

## 6\. Public API Design

The API surface aims for ergonomic simplicity, mirroring the Redis command set.

```rust
/// The Ferrokv Primary Interface
impl Ferrokv {
    /// Open database with default configuration
    pub async fn open(path: PathBuf) -> Result<Self>;

    /// Write with Durability Guarantee
    pub async fn set(&self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Write with Automatic Expiration
    pub async fn set_ex(&self, key: &[u8], val: &[u8], ttl: Duration) -> Result<()>;

    /// Read the value of `key`
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scan a range of keys
    pub async fn scan(&self, range: impl RangeBounds<&[u8]) -> Result<Vec<(Bytes, Bytes)>>;

    /// Delete with Tombstone Marking
    pub async fn del(&self, key: &[u8]) -> Result<bool>;

    /// Atomic Increment (Read-Modify-Write)
    pub async fn incr(&self, key: &[u8]) -> Result<i64>;
}
```

## 7\. Roadmap & Phases

- **Phase 1: The Skeleton**
  - Implement the Write-Ahead Log (WAL).
  - Implement the In-Memory Memtable (SkipList).
  - Basic `get`/`set` flow (Memory only).
- **Phase 2: Persistence**
  - Implement SSTable serialization/deserialization.
  - Implement the Flush mechanism (Memory -\> Disk).
- **Phase 3: The Engine**
  - Implement Background Compaction.
  - Implement MVCC Read logic.
- **Phase 4: Feature Parity**
  - Implement TTL logic (Lazy + Compaction).
  - Implement the primary interface fully.

## 8\. Conclusion

Ferrokv is not just another database; it is a declaration that **embedded persistence does not require sacrificing performance or developer experience.** By rigorously adhering to ACID principles while embracing modern async Rust patterns, Ferrokv provides the solid foundation required for the next generation of resilient, distributed applications.
