use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};

use crate::compaction::merge_sstables;
use crate::config::{Config, FerroKvBuilder};
use crate::errors::Result;
use crate::helpers::get_now;
use crate::memtable::{LookupResult, Memtable};
use crate::sstable::SSTable;
use crate::ttl::ExpiryHeap;
use crate::wal::Wal;

pub struct FerroKv {
    wal: Mutex<Wal>,
    memtable: Arc<Memtable>,
    sstables: Arc<RwLock<Vec<Arc<SSTable>>>>,
    next_sst_id: Arc<AtomicU64>,
    db_dir: PathBuf,
    expiry_heap: Arc<ExpiryHeap>,
    incr_lock: Mutex<()>,
    config: Arc<Config>,
}

impl FerroKv {
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
    pub async fn open<P: Into<PathBuf>>(path: P) -> Result<Self> {
        Self::open_with_config(path.into(), Config::default()).await
    }

    /// Create a new builder for custom configuration
    pub fn builder<P: Into<PathBuf>>(path: P) -> FerroKvBuilder {
        FerroKvBuilder::default().path(path.into())
    }

    /// Open database with custom configuration
    pub(crate) async fn open_with_config(path: PathBuf, config: Config) -> Result<Self> {
        tokio::fs::create_dir_all(&path).await?;
        let wal_path = path.join("data.wal");

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

        if let Ok(entries) = tokio::fs::read_dir(&path).await {
            let mut entries = entries;
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

        let sstables = Arc::new(RwLock::new(sstables));
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
        let memtable_clone = Arc::clone(&memtable);
        let expiry_heap_clone = Arc::clone(&expiry_heap);
        tokio::spawn(async move {
            Self::run_expiry_cleanup(memtable_clone, expiry_heap_clone).await;
        });

        Ok(db)
    }

    /// Write with durability guarantee
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut wal = self.wal.lock().await;
        wal.append(key, value, None).await?;
        wal.sync().await?;
        drop(wal);

        self.memtable.insert(key, value, None);

        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(())
    }

    /// Write with TTL expiration
    pub async fn set_ex(&self, key: &[u8], value: &[u8], ttl: Duration) -> Result<()> {
        let expire_at = get_now() + ttl.as_secs();

        let mut wal = self.wal.lock().await;
        wal.append(key, value, Some(expire_at)).await?;
        wal.sync().await?;
        drop(wal);

        self.memtable.insert(key, value, Some(expire_at));

        // Schedule proactive expiry cleanup
        let key_arc: Arc<[u8]> = key.to_vec().into();
        self.expiry_heap.schedule(key_arc, expire_at).await;

        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(())
    }

    /// Read with cascade: Memtable -> `SSTables`
    pub async fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        let snapshot = self.memtable.current_version();
        match self.memtable.get(key, snapshot) {
            LookupResult::Value(value) => return Ok(Some(value)),
            LookupResult::Tombstone => return Ok(None),
            LookupResult::NotFound => {}
        }

        let sstables = self.sstables.read().await;
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

    /// Delete a key
    /// Returns true if key existed, false if not found
    pub async fn del(&self, key: &[u8]) -> Result<bool> {
        let existed = self.get(key).await?.is_some();

        // Write tombstone to WAL for durability
        let mut wal = self.wal.lock().await;
        wal.append_tombstone(key).await?;
        wal.sync().await?;
        drop(wal);

        // Write tombstone to Memtable
        self.memtable.insert_tombstone(key);

        // Trigger flush if needed
        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(existed)
    }

    /// Atomic increment
    pub async fn incr(&self, key: &[u8]) -> Result<i64> {
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
        wal.sync().await?;
        drop(wal);

        self.memtable.insert(key, new_val, None);
        if self.memtable.should_flush(self.config.memtable_size) {
            self.flush_memtable().await?;
        }

        Ok(new_num)
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

    /// Flush Memtable to `SSTable` when 64MB threshold reached
    pub(crate) async fn flush_memtable(&self) -> Result<()> {
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);

        // Flush to Level 0 (fresh flushes)
        let sst = SSTable::flush(&self.db_dir, &self.memtable, 0, sst_id).await?;

        // Add to SSTable list
        let mut sstables = self.sstables.write().await;
        sstables.push(sst);

        // Check if compaction needed
        let l0_count = sstables.iter().filter(|s| s.level() == 0).count();
        let should_compact = l0_count > self.config.l0_compaction_threshold;
        drop(sstables);

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
        let sstables_clone = Arc::clone(&self.sstables);
        let db_dir = self.db_dir.clone();
        let next_id = Arc::clone(&self.next_sst_id);
        let config = Arc::clone(&self.config);

        tokio::spawn(async move {
            if let Err(err) = Self::run_compaction(sstables_clone, db_dir, next_id, config).await {
                eprintln!("Compaction failed: {err}");
            }
        });
    }

    /// Background compaction: merge L0 `SSTables` to L1
    async fn run_compaction(
        sstables: Arc<RwLock<Vec<Arc<SSTable>>>>,
        db_dir: PathBuf,
        next_id: Arc<AtomicU64>,
        config: Arc<Config>,
    ) -> Result<()> {
        // Select L0 SSTables for compaction
        let l0_sstables = {
            let guard = sstables.read().await;
            guard.iter().filter(|s| s.level() == 0).map(Arc::clone).collect::<Vec<_>>()
        };

        if l0_sstables.len() <= config.l0_compaction_threshold {
            return Ok(());
        }

        // Multi-way merge L0 -> L1
        let new_sstables =
            merge_sstables(&l0_sstables, 1, &db_dir, &next_id, config.sstable_size).await?;

        // Atomic commit
        let mut guard = sstables.write().await;
        guard.retain(|sst| !l0_sstables.iter().any(|old| old.path() == sst.path()));
        guard.extend(new_sstables);
        drop(guard);

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
    async fn test_acid_durability() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();
        db.set(b"key1", b"value1").await.unwrap();

        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result.as_deref(), Some(&b"value1"[..]));

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_crash_recovery() {
        let db_dir = tempdir().unwrap().keep();

        {
            let db = FerroKv::open(db_dir.clone()).await.unwrap();
            db.set(b"key1", b"value1").await.unwrap();
            db.set(b"key2", b"value2").await.unwrap();
        } // Drop simulates crash

        let db2 = FerroKv::open(db_dir.clone()).await.unwrap();
        assert_eq!(db2.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));
        assert_eq!(db2.get(b"key2").await.unwrap().as_deref(), Some(&b"value2"[..]));

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_ttl_persistence() {
        let db_dir = tempdir().unwrap().keep();

        {
            let db = FerroKv::open(db_dir.clone()).await.unwrap();
            db.set_ex(b"key1", b"value1", Duration::from_secs(3600)).await.unwrap();
        }

        let db2 = FerroKv::open(db_dir.clone()).await.unwrap();
        assert_eq!(db2.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Set TTL to 1 second and wait 2 seconds to ensure expiration
        db.set_ex(b"key1", b"value1", Duration::from_secs(1)).await.unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(db.get(b"key1").await.unwrap(), None);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());
        let mut handles = Vec::new();

        for i in 0..10 {
            let db_clone = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                let key = format!("key{i}");
                let value = format!("value{i}");
                db_clone.set(key.as_bytes(), value.as_bytes()).await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        for i in 0..10 {
            let key = format!("key{i}");
            let expected = format!("value{i}");
            let result = db.get(key.as_bytes()).await.unwrap();
            assert_eq!(result.as_deref(), Some(expected.as_bytes()));
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_empty_get() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();
        assert_eq!(db.get(b"nonexistent").await.unwrap(), None);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_sstable_cascade_read() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Write keys to Memtable
        db.set(b"key1", b"value1").await.unwrap();
        db.set(b"key2", b"value2").await.unwrap();

        // Manually flush to SSTable
        db.flush_memtable().await.unwrap();

        // Memtable should be empty, but data persists in SSTable
        assert!(db.memtable.is_empty());

        // Read should cascade to SSTable
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result.as_deref(), Some(&b"value1"[..]));

        let result = db.get(b"key2").await.unwrap();
        assert_eq!(result.as_deref(), Some(&b"value2"[..]));

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_compaction_drops_expired_keys() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

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

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

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
        let sstables = db.sstables.read().await;
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

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

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

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

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

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

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
            let db_clone = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                for i in 0..50 {
                    let key = format!("key_{i:03}");
                    let result = db_clone.get(key.as_bytes()).await.unwrap();
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
    async fn test_ttl_strategy_proactive_cleanup() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

        // Write key with 100ms TTL
        db.set_ex(b"cold_key", b"cold_value", Duration::from_millis(100)).await.unwrap();

        // Verify scheduled in expiry heap
        let next = db.expiry_heap.peek_next().await;
        assert!(next.is_some(), "Expiry should be scheduled");

        // Wait for background cleanup
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Key should be proactively deleted (not lazy - no GET called)
        assert_eq!(db.get(b"cold_key").await.unwrap(), None);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_ttl_non_blocking() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

        // Schedule 100 expirations
        for i in 0..100 {
            let key = format!("expire_{i}");
            db.set_ex(key.as_bytes(), b"value", Duration::from_millis(50)).await.unwrap();
        }

        // Concurrent writes during cleanup
        for i in 0..100 {
            let key = format!("write_{i}");
            db.set(key.as_bytes(), b"value").await.unwrap();
        }

        // All writes must succeed without blocking
        for i in 0..100 {
            let key = format!("write_{i}");
            assert_eq!(db.get(key.as_bytes()).await.unwrap().as_deref(), Some(&b"value"[..]));
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_sstable_persistence_across_restart() {
        let db_dir = tempdir().unwrap().keep();

        // Write data and flush to SSTable
        {
            let db = FerroKv::open(db_dir.clone()).await.unwrap();

            // Write enough data to force flush
            for i in 0..100 {
                let key = format!("key_{i:03}");
                let value = format!("value_{i:03}");
                db.set(key.as_bytes(), value.as_bytes()).await.unwrap();
            }

            // Manually flush to SSTable
            db.flush_memtable().await.unwrap();

            // Verify SSTable exists
            let sstables = db.sstables.read().await;
            assert!(!sstables.is_empty(), "SSTable should be created");
        } // Drop DB (simulates restart)

        // Reopen database and verify SSTable data is loaded
        {
            let db = FerroKv::open(db_dir.clone()).await.unwrap();

            // Verify SSTables were loaded
            let sstables = db.sstables.read().await;
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
    async fn test_incr() {
        let db_dir = tempdir().unwrap().keep();
        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Increment non-existent key (should start at 0)
        let val = db.incr(b"counter").await.unwrap();
        assert_eq!(val, 1);

        // Increment again
        let val = db.incr(b"counter").await.unwrap();
        assert_eq!(val, 2);

        // Increment multiple times
        for i in 3..=10 {
            let val = db.incr(b"counter").await.unwrap();
            assert_eq!(val, i);
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_incr_durability() {
        let db_dir = tempdir().unwrap().keep();

        {
            let db = FerroKv::open(db_dir.clone()).await.unwrap();
            db.incr(b"counter").await.unwrap();
            db.incr(b"counter").await.unwrap();
            db.incr(b"counter").await.unwrap();
        } // Drop (simulates crash)

        // Reopen and verify counter persisted
        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Read as string to verify value
        let val = db.get(b"counter").await.unwrap().unwrap();
        let val_str = std::str::from_utf8(&val).unwrap();
        assert_eq!(val_str, "3");

        // Continue incrementing
        let val = db.incr(b"counter").await.unwrap();
        assert_eq!(val, 4);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_incr_concurrent() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

        // Spawn 10 concurrent incrementers
        let mut handles = Vec::new();
        for _ in 0..10 {
            let db_clone = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                for _ in 0..10 {
                    db_clone.incr(b"shared_counter").await.unwrap();
                }
            }));
        }

        // Wait for all increments
        for handle in handles {
            handle.await.unwrap();
        }

        // Final value should be 100 (10 tasks × 10 increments)
        let val = db.get(b"shared_counter").await.unwrap().unwrap();
        let val_str = std::str::from_utf8(&val).unwrap();
        assert_eq!(val_str, "100", "All increments should be atomic");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_incr_invalid_value() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Set non-numeric value
        db.set(b"bad_counter", b"not_a_number").await.unwrap();

        // Increment should default to 0 and return 1
        let val = db.incr(b"bad_counter").await.unwrap();
        assert_eq!(val, 1);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_existing_key() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Set then delete
        db.set(b"key1", b"value1").await.unwrap();
        let existed = db.del(b"key1").await.unwrap();
        assert!(existed, "Key should exist before deletion");

        // Verify deleted
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, None, "Key should be deleted");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_nonexistent_key() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // Delete non-existent key
        let existed = db.del(b"nonexistent").await.unwrap();
        assert!(!existed, "Key should not exist");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_durability() {
        let db_dir = tempdir().unwrap().keep();

        {
            let db = FerroKv::open(db_dir.clone()).await.unwrap();
            db.set(b"key1", b"value1").await.unwrap();
            db.del(b"key1").await.unwrap();
        } // Drop simulates crash

        // Reopen and verify deletion persisted via WAL
        let db = FerroKv::open(db_dir.clone()).await.unwrap();
        assert_eq!(db.get(b"key1").await.unwrap(), None, "Deletion should survive restart");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_sstable_cascade() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

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
    async fn test_del_compaction_cleanup() {
        let db_dir = tempdir().unwrap().keep();

        let db = Arc::new(FerroKv::open(db_dir.clone()).await.unwrap());

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
    async fn test_del_idempotent() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        db.set(b"key1", b"value1").await.unwrap();

        // First delete
        let existed1 = db.del(b"key1").await.unwrap();
        assert!(existed1, "First delete should return true");

        // Second delete (idempotent)
        let existed2 = db.del(b"key1").await.unwrap();
        assert!(!existed2, "Second delete should return false");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_del_set_del_sequence() {
        let db_dir = tempdir().unwrap().keep();

        let db = FerroKv::open(db_dir.clone()).await.unwrap();

        // SET -> DEL -> SET sequence
        db.set(b"key1", b"value1").await.unwrap();
        assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));

        db.del(b"key1").await.unwrap();
        assert_eq!(db.get(b"key1").await.unwrap(), None);

        db.set(b"key1", b"value2").await.unwrap();
        assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value2"[..]));

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }
}
