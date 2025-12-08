use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use crate::batch::BatchEntry;
use crate::compaction::merge_sstables;
use crate::config::{Builder, Config};
use crate::errors::Result;
use crate::helpers::{DB_PATH, get_now};
use crate::memtable::{LookupResult, Memtable};
use crate::sstable::SSTable;
use crate::ttl::ExpiryHeap;
use crate::wal::{BatchWalEntry, Wal};
use crate::{Stats, WriteBatch};

/// Type alias for merged record: (value, version, ttl, `is_tombstone`)
type MergedRecord = (Arc<[u8]>, u64, Option<u64>, bool);

/// Type alias for scan result pair
type ScanPair = (Arc<[u8]>, Arc<[u8]>);

/// Type alias for scan range bounds
pub type ScanBounds = (Bound<Arc<[u8]>>, Bound<Arc<[u8]>>);

pub struct FerroKv {
    wal: Mutex<Wal>,
    memtable: Arc<Memtable>,
    sstables: Arc<ArcSwap<Vec<Arc<SSTable>>>>,
    next_sst_id: Arc<AtomicU64>,
    db_dir: PathBuf,
    expiry_heap: Arc<ExpiryHeap>,
    incr_lock: Mutex<()>,
    config: Arc<Config>,
}

impl FerroKv {
    /// Open database with default configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ferrokv::FerroKv;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let db = FerroKv::new().await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// For custom configuration, see [`Builder::new()`]
    pub async fn new() -> Result<Self> {
        Self::with_config(DB_PATH.into(), Config::default()).await
    }

    /// Open database with default configuration
    ///
    /// For custom configuration call the builder method:
    /// ```no_run
    /// use ferrokv::FerroKv;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = FerroKv::builder("./data").memtable_size(128 * 1024 * 1024).open().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(since = "1.0.2", note = "Use `FerroKv::with_path` instead")]
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::with_config(path.into(), Config::default()).await
    }

    /// Open database with the specified directory path.
    ///
    /// Uses the same default configuration as `FerroKv::new()`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ferrokv::FerroKv;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let db = FerroKv::with_path("./data").await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// For custom configuration, see [`Builder::new()`]
    pub async fn with_path(path: impl Into<PathBuf>) -> Result<Self> {
        Self::with_config(path.into(), Config::default()).await
    }

    /// Builder for custom configuration
    #[deprecated(since = "1.0.2", note = "Use `Builder::new().path(..)` instead")]
    pub fn builder(path: impl Into<PathBuf>) -> Builder {
        Builder::default().path(path)
    }

    /// Open database with configuration
    pub(crate) async fn with_config(path: PathBuf, config: Config) -> Result<Self> {
        tokio::fs::create_dir_all(&path).await?;
        let wal_path = path.join("wal.log");

        let recovered_entries = Wal::recover(&wal_path).await?;
        let memtable = Arc::new(Memtable::default());

        // Tracks max version during recovery for version counter restoration
        let mut max_version = 0;

        for entry in recovered_entries {
            let version = if entry.is_tombstone {
                memtable.insert_tombstone(&entry.key)
            } else {
                memtable.insert(&entry.key, &entry.value, entry.ttl)
            };
            max_version = max_version.max(version);
        }

        // Restore version counter to continue from max recovered version
        if max_version > 0 {
            let current = memtable.current_version();
            for _ in current..max_version {
                let _ = memtable.next_version();
            }
        }

        let mut wal = Wal::new(wal_path).await?;
        wal.truncate().await?;

        // Scan existing .sst files and load metadata
        let mut sstables = Vec::new();
        let mut max_sst_id = 0;

        if let Ok(mut entries) = tokio::fs::read_dir(&path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let entry_path = entry.path();
                if entry_path.extension().and_then(|s| s.to_str()).is_some_and(|ext| ext == "sst") {
                    match SSTable::open(entry_path).await {
                        Ok(sst) => {
                            max_sst_id = max_sst_id.max(sst.id());
                            sstables.push(sst);
                        }
                        Err(err) => {
                            eprintln!("Warning: Failed to load SSTable: {err}");
                        }
                    }
                }
            }
        }

        // Sort SSTables by level and ID for optimal read performance
        sstables.sort_by(|a, b| a.level().cmp(&b.level()).then_with(|| a.id().cmp(&b.id())));

        let sstables = Arc::new(ArcSwap::from_pointee(sstables));
        let next_sst_id = Arc::new(AtomicU64::new(max_sst_id + 1));
        let expiry_heap = Arc::new(ExpiryHeap::default());

        let db = Self {
            wal: Mutex::new(wal),
            memtable: Arc::clone(&memtable),
            sstables,
            next_sst_id,
            db_dir: path,
            expiry_heap: Arc::clone(&expiry_heap),
            incr_lock: Mutex::new(()),
            config: Arc::new(config),
        };

        // Spawn background expiry cleanup task
        let memtable = Arc::clone(&memtable);
        let expiry_heap = Arc::clone(&expiry_heap);
        tokio::spawn(async move {
            Self::run_expiry_cleanup(memtable, expiry_heap).await;
        });

        Ok(db)
    }

    /// Insert or update a key with the given value
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        #[cfg(debug_assertions)]
        println!("SET {} {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));

        let mut wal = self.wal.lock().await;
        wal.append(key, value, None).await?;
        drop(wal);

        self.memtable.insert(key, value, None);

        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(())
    }

    /// Insert or update a key with the given value and a TTL expiration
    pub async fn set_ex(&self, key: &[u8], value: &[u8], ttl: Duration) -> Result<()> {
        let ttl = ttl.as_secs();

        #[cfg(debug_assertions)]
        println!(
            "SETEX {} {} {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
            ttl
        );

        let expire_at = get_now() + ttl;

        let mut wal = self.wal.lock().await;
        wal.append(key, value, Some(expire_at)).await?;
        drop(wal);

        self.memtable.insert(key, value, Some(expire_at));

        // Schedule proactive expiry cleanup
        let key: Arc<[u8]> = key.to_vec().into();
        self.expiry_heap.schedule(key, expire_at).await;

        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(())
    }

    /// Get the value associated with `key`
    pub async fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        let snapshot = self.memtable.current_version();
        match self.memtable.get(key, snapshot) {
            LookupResult::Value(value) => return Ok(Some(value)),
            LookupResult::Tombstone => return Ok(None),
            LookupResult::NotFound => {}
        }

        let sstables = self.sstables.load();
        for sst in sstables.iter().rev() {
            if !sst.contains_key_range(key) {
                continue;
            }

            if let Some((value, ..)) = sst.get(key).await? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Delete the `key`
    /// Returns true if key existed, otherwise false
    pub async fn del(&self, key: &[u8]) -> Result<bool> {
        #[cfg(debug_assertions)]
        println!("DEL {}", String::from_utf8_lossy(key));

        let existed = self.get(key).await?.is_some();

        // Write tombstone to WAL for durability
        let mut wal = self.wal.lock().await;
        wal.append_tombstone(key).await?;
        drop(wal);

        // Write tombstone to Memtable
        self.memtable.insert_tombstone(key);

        // Trigger flush if needed
        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(existed)
    }

    /// Scan key-value pairs in the given range
    /// Returns a Vec of (key, value) pairs
    pub async fn scan(&self, range: impl RangeBounds<&[u8]>) -> Result<Vec<ScanPair>> {
        let bounds = Self::to_arc_bounds(&range);
        let snapshot_version = self.memtable.current_version();
        let now = get_now();

        let mut merged: BTreeMap<Arc<[u8]>, MergedRecord> = BTreeMap::new();

        for record in self.memtable.range(&bounds, snapshot_version) {
            match merged.get(&record.key) {
                Some((_, existing_version, ..)) if *existing_version >= record.version => {}
                _ => {
                    merged.insert(
                        record.key,
                        (record.value, record.version, record.ttl, record.is_tombstone),
                    );
                }
            }
        }

        let sstables = self.sstables.load();
        for sst in sstables.iter() {
            if sst.overlaps_range(&bounds) {
                for record in sst.scan_range(&bounds, now).await? {
                    if record.version > snapshot_version {
                        continue;
                    }
                    match merged.get(&record.key) {
                        Some((_, existing_version, ..)) if *existing_version >= record.version => {}
                        _ => {
                            merged.insert(
                                record.key,
                                (record.value, record.version, record.ttl, record.is_tombstone),
                            );
                        }
                    }
                }
            }
        }
        drop(sstables);

        // Filter tombstones
        Ok(merged
            .into_iter()
            .filter_map(|(key, (value, _, ttl, is_tombstone))| {
                if is_tombstone {
                    return None;
                }

                if let Some(expire_at) = ttl
                    && now >= expire_at
                {
                    return None;
                }
                Some((key, value))
            })
            .collect())
    }

    /// Convert range bounds to `Arc<[u8]>`
    fn to_arc_bounds<'a>(range: &impl RangeBounds<&'a [u8]>) -> ScanBounds {
        let start = match range.start_bound() {
            Bound::Included(k) => Bound::Included(Arc::from(*k)),
            Bound::Excluded(k) => Bound::Excluded(Arc::from(*k)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(k) => Bound::Included(Arc::from(*k)),
            Bound::Excluded(k) => Bound::Excluded(Arc::from(*k)),
            Bound::Unbounded => Bound::Unbounded,
        };

        (start, end)
    }

    /// Return all keys in the database.
    /// Excludes tombstones and expired keys.
    ///
    /// **NOTE**: This is an O(N) operation—scans all keys.
    pub async fn keys(&self) -> Result<Vec<Arc<[u8]>>> {
        self.scan(..).await.map(|pairs| pairs.into_iter().map(|(k, _)| k).collect())
    }

    /// Count total keys in the database
    ///
    /// Returns the exact number of keys at the current snapshot.
    /// Excludes tombstones and expired keys.
    ///
    /// **Note**: This is O(N) operation—scans all keys.
    pub async fn len(&self) -> Result<usize> {
        Ok(self.keys().await?.len())
    }

    /// Check if database is empty
    ///
    /// Returns `true` if the database contains no keys.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Get database statistics
    ///
    /// For exact key count, use `len()`.
    pub fn stats(&self) -> Stats {
        Stats::new(&self.memtable, &self.sstables)
    }

    /// Increment the `key` atomically
    pub async fn incr(&self, key: &[u8]) -> Result<i64> {
        #[cfg(debug_assertions)]
        println!("INCR {}", String::from_utf8_lossy(key));

        // Acquire lock to serialize Read-Modify-Write operations
        let _lock = self.incr_lock.lock().await;

        // Read current value (with snapshot consistency)
        let current_val = self.get(key).await?;

        // Parse as i64 (default to 0 if missing or invalid)
        let current_num = if let Some(val_bytes) = current_val {
            std::str::from_utf8(&val_bytes)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_default()
        } else {
            0
        };

        // Increment
        let new_num = current_num.saturating_add(1);
        let new_val = new_num.to_string();
        let new_val = new_val.as_bytes();

        // Write back atomically via WAL
        let mut wal = self.wal.lock().await;
        wal.append(key, new_val, None).await?;
        drop(wal);

        self.memtable.insert(key, new_val, None);
        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(new_num)
    }

    /// Execute a batch of writes with a single fsync.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use ferrokv::{FerroKv, WriteBatch};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let db = FerroKv::with_path("./data").await?;
    ///
    ///     let mut batch = WriteBatch::new();
    ///     batch
    ///         .set(b"foo:1", b"bar")
    ///         .set(b"foo:2", b"baz")
    ///         .set_ex(b"foo:3", b"qux", Duration::from_secs(3600))
    ///         .del(b"foo:2");
    ///
    ///     // Single fsync for all 4 operations
    ///     db.write_batch(batch).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let wal_entries: Vec<BatchWalEntry<'_>> = batch
            .entries
            .iter()
            .map(|entry| match entry {
                BatchEntry::Set { key, value, ttl } => {
                    (key.as_slice(), value.as_slice(), *ttl, false)
                }
                BatchEntry::Del { key } => (key.as_slice(), &[][..], None, true),
            })
            .collect();

        let mut wal = self.wal.lock().await;
        wal.append_batch(&wal_entries).await?;
        drop(wal);

        for entry in &batch.entries {
            match entry {
                BatchEntry::Set { key, value, ttl } => {
                    self.memtable.insert(key, value, *ttl);
                    if let Some(expire_at) = ttl {
                        let key: Arc<[u8]> = key.clone().into();
                        self.expiry_heap.schedule(key, *expire_at).await;
                    }
                }
                BatchEntry::Del { key } => {
                    self.memtable.insert_tombstone(key);
                }
            }
        }

        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(())
    }

    /// Proactive TTL cleanup
    async fn run_expiry_cleanup(memtable: Arc<Memtable>, expiry_heap: Arc<ExpiryHeap>) {
        loop {
            let sleep_duration = if let Some((expire_at, _)) = expiry_heap.peek_next().await {
                let now = get_now();
                if expire_at <= now {
                    // Expired entry ready
                    Duration::from_secs(0)
                } else {
                    // Sleep until expiration
                    Duration::from_secs(expire_at.saturating_sub(now))
                }
            } else {
                // No entries, wait for notification
                Duration::from_secs(3600)
            };

            tokio::select! {
                () = tokio::time::sleep(sleep_duration) => {
                    // Time to check for expired keys
                    let now = get_now();
                    while let Some(key) = expiry_heap.pop_expired(now).await {
                        memtable.delete(&key);
                    }
                }
                () = expiry_heap.notifier().notified() => {}
            }
        }
    }

    /// Flush Memtable to `SSTable` when config threshold reached
    pub(crate) async fn flush_memtable(&self) -> Result<()> {
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);

        // Flush to Level 0 (fresh flushes)
        let sst = SSTable::flush(&self.db_dir, &self.memtable, 0, sst_id).await?;

        // Add to SSTable list atomically using RCU pattern
        let should_compact = {
            let old = self.sstables.load();
            let mut new_list = (**old).clone();
            new_list.push(sst);
            let l0_count = new_list.iter().filter(|s| s.level() == 0).count();
            self.sstables.store(Arc::new(new_list));
            l0_count > self.config.l0_compaction_threshold
        };

        // Clear Memtable and truncate WAL
        self.memtable.clear();
        let mut wal = self.wal.lock().await;
        wal.truncate().await?;

        // Trigger compaction if needed
        if should_compact {
            self.maybe_trigger_compaction();
        }

        Ok(())
    }

    /// Trigger background compaction
    fn maybe_trigger_compaction(&self) {
        let sstables = Arc::clone(&self.sstables);
        let db_dir = self.db_dir.clone();
        let next_id = Arc::clone(&self.next_sst_id);
        let config = Arc::clone(&self.config);

        tokio::spawn(async move {
            if let Err(err) = Self::run_compaction(sstables, db_dir, next_id, config).await {
                eprintln!("Compaction failed: {err}");
            }
        });
    }

    /// Background compaction to merge L0 `SSTables` to L1
    async fn run_compaction(
        sstables: Arc<ArcSwap<Vec<Arc<SSTable>>>>,
        db_dir: PathBuf,
        next_id: Arc<AtomicU64>,
        config: Arc<Config>,
    ) -> Result<()> {
        // Select L0 SSTables for compaction
        let l0_sstables: Vec<Arc<SSTable>> = {
            let guard = sstables.load();
            guard.iter().filter(|s| s.level() == 0).map(Arc::clone).collect()
        };

        if l0_sstables.len() <= config.l0_compaction_threshold {
            return Ok(());
        }

        // Multi-way merge L0 -> L1
        let new_sstables =
            merge_sstables(&l0_sstables, 1, &db_dir, &next_id, config.sstable_size).await?;

        // Atomic commit using RCU pattern
        {
            let old = sstables.load();
            let mut new_list = (**old).clone();
            new_list.retain(|sst| !l0_sstables.iter().any(|old| old.path() == sst.path()));
            new_list.extend(new_sstables);
            sstables.store(Arc::new(new_list));
        }

        // Delete old SSTables ONLY AFTER new ones visible
        for old_sst in l0_sstables {
            let _ = tokio::fs::remove_file(old_sst.path()).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_compaction_drops_expired_keys() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Write 100 keys with TTL=1 second (will expire)
        for i in 0..100 {
            let key = format!("expired_{i:03}");
            db.set_ex(key.as_bytes(), b"value", Duration::from_secs(1)).await.unwrap();
        }

        // Write 100 keys with no TTL (permanent)
        for i in 0..100 {
            let key = format!("permanent_{i:03}");
            db.set(key.as_bytes(), b"value").await.unwrap();
        }

        // Force flushes to create multiple L0 SSTables
        for _ in 0..5 {
            if !db.memtable.is_empty() {
                db.flush_memtable().await.unwrap();
            }
            // Write filler to ensure next flush has data
            db.set(b"filler", b"value").await.unwrap();
        }

        // Wait for expiration + compaction to finish
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify expired keys are gone
        for i in 0..100 {
            let key = format!("expired_{i:03}");
            assert_eq!(
                db.get(key.as_bytes()).await.unwrap(),
                None,
                "Expired key should be dropped"
            );
        }

        // Verify permanent keys still exist
        for i in 0..100 {
            let key = format!("permanent_{i:03}");
            assert_eq!(
                db.get(key.as_bytes()).await.unwrap().as_deref(),
                Some(&b"value"[..]),
                "Permanent key should survive compaction"
            );
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_compaction_reduces_read_amplification() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Create 10 L0 SSTables
        for batch in 0..10 {
            for i in 0..10 {
                let key = format!("key_{batch:03}_{i:03}");
                db.set(key.as_bytes(), b"value").await.unwrap();
            }
            db.flush_memtable().await.unwrap();
        }

        // Wait for compaction to finish
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify L0 count reduced (read amplification improved)
        let sstables = db.sstables.load();
        let l0_count = sstables.iter().filter(|s| s.level() == 0).count();
        assert!(l0_count <= 4, "Compaction should reduce L0 count to <= 4, got {l0_count}");

        // Verify data integrity
        for batch in 0..10 {
            for i in 0..10 {
                let key = format!("key_{batch:03}_{i:03}");
                assert_eq!(db.get(key.as_bytes()).await.unwrap().as_deref(), Some(&b"value"[..]));
            }
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_compaction_deduplication() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Write same key 3 times with different values
        db.set(b"key1", b"v1").await.unwrap();
        db.flush_memtable().await.unwrap();

        db.set(b"key1", b"v2").await.unwrap();
        db.flush_memtable().await.unwrap();

        db.set(b"key1", b"v3").await.unwrap();
        db.flush_memtable().await.unwrap();

        // Create more L0 SSTables to trigger compaction
        for i in 0..3 {
            let key = format!("filler_{i}");
            db.set(key.as_bytes(), b"value").await.unwrap();
            db.flush_memtable().await.unwrap();
        }

        // Wait for compaction
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify only latest value exists
        assert_eq!(
            db.get(b"key1").await.unwrap().as_deref(),
            Some(&b"v3"[..]),
            "Should have latest value after deduplication"
        );

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_compaction_non_blocking_writes() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Create L0 SSTables to trigger compaction
        for i in 0..6 {
            for j in 0..10 {
                let key = format!("key_{i}_{j}");
                db.set(key.as_bytes(), b"value").await.unwrap();
            }
            db.flush_memtable().await.unwrap();
        }

        // Write new keys during compaction (should not block)
        for i in 0..100 {
            let key = format!("new_key_{i}");
            db.set(key.as_bytes(), b"new_value").await.unwrap();
        }

        // Verify all writes succeeded
        for i in 0..100 {
            let key = format!("new_key_{i}");
            assert_eq!(db.get(key.as_bytes()).await.unwrap().as_deref(), Some(&b"new_value"[..]));
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_compaction_concurrent_reads() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Create data
        for i in 0..50 {
            let key = format!("key_{i:03}");
            db.set(key.as_bytes(), b"value").await.unwrap();
        }

        // Create L0 SSTables to trigger compaction
        for _ in 0..6 {
            if !db.memtable.is_empty() {
                db.flush_memtable().await.unwrap();
            }
            // Write data to ensure next flush has content
            db.set(b"filler", b"value").await.unwrap();
        }

        // Spawn concurrent readers during compaction
        let mut handles = Vec::new();
        for _ in 0..10 {
            let db = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                for i in 0..50 {
                    let key = format!("key_{i:03}");
                    let result = db.get(key.as_bytes()).await.unwrap();
                    assert_eq!(result.as_deref(), Some(&b"value"[..]));
                }
            }));
        }

        // All reads must succeed without blocking
        for handle in handles {
            handle.await.unwrap();
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_info_statistics() {
        let db_dir = tempdir().unwrap().keep();
        let db = FerroKv::with_path(&db_dir).await.unwrap();

        let info_empty = db.stats();
        assert_eq!(info_empty.memtable_keys, 0);
        assert_eq!(info_empty.sstable_count, 0);

        db.set(b"key", b"value").await.unwrap();

        let info = db.stats();
        assert!(info.memtable_keys > 0);
        assert!(info.memtable_size > 0);
        assert_eq!(info.sstable_count, 0); // No flush yet
        assert!(info.current_version > 0);

        // Flush to create SSTable
        db.flush_memtable().await.unwrap();

        let info_after_flush = db.stats();
        assert_eq!(info_after_flush.memtable_keys, 0); // Cleared after flush
        assert_eq!(info_after_flush.sstable_count, 1);
        assert!(info_after_flush.total_disk > 0);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_scan_deduplication() {
        let db_dir = tempdir().unwrap().keep();
        let db = FerroKv::with_path(&db_dir).await.unwrap();

        // Write same key multiple times
        db.set(b"key_a", b"v1").await.unwrap();
        db.flush_memtable().await.unwrap();

        db.set(b"key_a", b"v2").await.unwrap();
        db.flush_memtable().await.unwrap();

        db.set(b"key_a", b"v3").await.unwrap();

        // Scan should return only the latest value
        let results = db.scan(..).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_ref(), b"key_a");
        assert_eq!(results[0].1.as_ref(), b"v3");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_scan_across_memtable_and_sstable() {
        let db_dir = tempdir().unwrap().keep();
        let db = FerroKv::with_path(&db_dir).await.unwrap();

        // Write and flush to SSTable
        db.set(b"key_1", b"sstable_value").await.unwrap();
        db.set(b"key_2", b"sstable_value").await.unwrap();
        db.flush_memtable().await.unwrap();

        // Write to memtable (not flushed)
        db.set(b"key_3", b"memtable_value").await.unwrap();
        db.set(b"key_4", b"memtable_value").await.unwrap();

        // Scan should include both SSTable and Memtable data
        let results = db.scan(..).await.unwrap();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0.as_ref(), b"key_1");
        assert_eq!(results[1].0.as_ref(), b"key_2");
        assert_eq!(results[2].0.as_ref(), b"key_3");
        assert_eq!(results[3].0.as_ref(), b"key_4");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_compaction_cleanup() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Write + flush
        db.set(b"key1", b"value1").await.unwrap();
        db.flush_memtable().await.unwrap();

        // Delete + flush (tombstone in SSTable)
        db.del(b"key1").await.unwrap();
        db.flush_memtable().await.unwrap();

        // Create more L0 SSTables to trigger compaction
        for i in 0..5 {
            let key = format!("filler_{i}");
            db.set(key.as_bytes(), b"value").await.unwrap();
            db.flush_memtable().await.unwrap();
        }

        // Wait for compaction to complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        // After compaction, tombstone should have dropped both old value and itself
        assert_eq!(
            db.get(b"key1").await.unwrap(),
            None,
            "Key should remain deleted after compaction"
        );

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_sstable_cascade() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::with_path(&db_dir).await.unwrap();

        // Write and flush to SSTable
        db.set(b"key1", b"value1").await.unwrap();
        db.flush_memtable().await.unwrap();

        // Delete (tombstone in Memtable)
        db.del(b"key1").await.unwrap();

        // Read should find tombstone first (cascade stops at Memtable)
        assert_eq!(db.get(b"key1").await.unwrap(), None, "Tombstone should hide SSTable value");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_sstable_persistence_across_restart() {
        let db_dir = tempdir().unwrap().keep();

        // Write data and flush to SSTable
        {
            let db = FerroKv::with_path(&db_dir).await.unwrap();

            // Write enough data to force flush
            for i in 0..100 {
                let key = format!("key_{i:03}");
                let value = format!("value_{i:03}");
                db.set(key.as_bytes(), value.as_bytes()).await.unwrap();
            }

            // Manually flush to SSTable
            db.flush_memtable().await.unwrap();

            // Verify SSTable exists
            let sstables = db.sstables.load();
            assert!(!sstables.is_empty(), "SSTable should be created");
        } // Drop DB (simulates restart)

        // Reopen database and verify SSTable data is loaded
        {
            let db = FerroKv::with_path(&db_dir).await.unwrap();

            // Verify SSTables were loaded
            let sstables = db.sstables.load();
            assert!(!sstables.is_empty(), "SSTables should be loaded on restart");

            // Verify data is accessible (not just in WAL)
            for i in 0..100 {
                let key = format!("key_{i:03}");
                let expected = format!("value_{i:03}");
                let result = db.get(key.as_bytes()).await.unwrap();
                assert_eq!(
                    result.as_deref(),
                    Some(expected.as_bytes()),
                    "SSTable data should survive restart"
                );
            }
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_ttl_strategy_proactive_cleanup() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

        // Write key with 100ms TTL
        db.set_ex(b"cold_key", b"cold_value", Duration::from_millis(100)).await.unwrap();

        // Verify scheduled in expiry heap
        let next = db.expiry_heap.peek_next().await;
        assert!(next.is_some(), "Expiry should be scheduled");

        // Wait for background cleanup
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Key should be proactively deleted
        assert_eq!(db.get(b"cold_key").await.unwrap(), None);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }
}
