use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::helpers::get_now;
use crate::mvcc::VersionCounter;
use crate::storage::ScanBounds;

/// Result of a memtable lookup operation
#[derive(Debug, PartialEq, Eq)]
pub enum LookupResult {
    /// Key not found in memtable or version too new
    NotFound,
    /// Key exists but is deleted (tombstone) or expired
    Tombstone,
    /// Key exists with a value
    Value(Arc<[u8]>),
}

/// In-memory entry with MVCC versioning
#[derive(Debug)]
struct MemtableEntry {
    value: Arc<[u8]>,
    version: u64,
    ttl: Option<u64>,
    is_tombstone: bool,
}

/// Record returned by Memtable iterator
#[derive(Debug)]
pub struct MemtableRecord {
    pub key: Arc<[u8]>,
    pub value: Arc<[u8]>,
    pub version: u64,
    pub ttl: Option<u64>,
    pub is_tombstone: bool,
}

/// Lock-free in-memory store
#[derive(Default)]
pub struct Memtable {
    data: SkipMap<Arc<[u8]>, MemtableEntry>,
    size: AtomicUsize,
    version_counter: Arc<VersionCounter>,
}

impl Memtable {
    /// Insert a key-value pair with optional TTL
    /// Returns the version number assigned to this write
    pub fn insert(&self, key: &[u8], value: &[u8], ttl: Option<u64>) -> u64 {
        let version = self.version_counter.next_version();

        let key: Arc<[u8]> = key.to_vec().into();
        let value: Arc<[u8]> = value.to_vec().into();

        let entry = MemtableEntry { value, version, ttl, is_tombstone: false };

        // key + value + metadata
        let entry_size = key.len() + entry.value.len() + 16;

        self.data.insert(key, entry);

        self.size.fetch_add(entry_size, Ordering::Relaxed);

        version
    }

    /// Get value for key at given snapshot version
    /// Returns `NotFound` if key doesn't exist or version too new
    /// Returns `Tombstone` if key exists but is deleted or expired
    /// Returns `Value` if key exists with a valid value
    #[inline]
    pub fn get(&self, key: &[u8], snapshot_version: u64) -> LookupResult {
        let Some(entry) = self.data.get(key) else {
            return LookupResult::NotFound;
        };
        let entry_val = entry.value();

        if entry_val.version > snapshot_version {
            return LookupResult::NotFound;
        }

        // Check tombstone before TTL
        if entry_val.is_tombstone {
            return LookupResult::Tombstone;
        }

        if let Some(ttl) = entry_val.ttl {
            let now = get_now();
            if now > ttl {
                return LookupResult::Tombstone;
            }
        }

        LookupResult::Value(Arc::clone(&entry_val.value))
    }

    /// Get current memory usage in bytes
    /// Used to trigger flush to `SSTable`
    #[inline]
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get current version (for snapshot creation)
    #[inline]
    pub fn current_version(&self) -> u64 {
        self.version_counter.current_version()
    }

    /// Advance version counter without inserting (for WAL recovery)
    pub fn next_version(&self) -> u64 {
        self.version_counter.next_version()
    }

    /// Check if flush threshold is reached
    pub fn should_flush(&self, flush_threshold: usize) -> bool {
        self.size() >= flush_threshold
    }

    /// Iterate over all entries (for `SSTable` flush)
    /// Returns iterator of records in sorted order
    pub fn iter(&self) -> impl Iterator<Item = MemtableRecord> + '_ {
        self.data.iter().map(|entry| {
            let key = Arc::clone(entry.key());
            let val = entry.value();
            let value = Arc::clone(&val.value);
            MemtableRecord {
                key,
                value,
                version: val.version,
                ttl: val.ttl,
                is_tombstone: val.is_tombstone,
            }
        })
    }

    /// Iterate over entries in a key range with MVCC filtering
    /// Returns records visible at the given snapshot version
    pub fn range(&self, range: &ScanBounds, snapshot_version: u64) -> Vec<MemtableRecord> {
        let start_bound = match &range.0 {
            Bound::Included(k) => Bound::Included(Arc::clone(k)),
            Bound::Excluded(k) => Bound::Excluded(Arc::clone(k)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match &range.1 {
            Bound::Included(k) => Bound::Included(Arc::clone(k)),
            Bound::Excluded(k) => Bound::Excluded(Arc::clone(k)),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.data
            .range((start_bound, end_bound))
            .filter_map(|entry| {
                let val = entry.value();

                // Skip if version is newer than snapshot
                if val.version > snapshot_version {
                    return None;
                }

                Some(MemtableRecord {
                    key: Arc::clone(entry.key()),
                    value: Arc::clone(&val.value),
                    version: val.version,
                    ttl: val.ttl,
                    is_tombstone: val.is_tombstone,
                })
            })
            .collect()
    }

    /// Clear all entries (to be called after successful `SSTable` flush)
    pub fn clear(&self) {
        self.data.clear();
        self.size.store(0, Ordering::Relaxed);
    }

    /// Get number of entries in memtable
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty (for testing/monitoring)
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Insert tombstone marker for deleted key
    /// Returns the version number assigned to this deletion
    pub fn insert_tombstone(&self, key: &[u8]) -> u64 {
        let version = self.version_counter.next_version();

        let key: Arc<[u8]> = key.to_vec().into();

        let entry =
            MemtableEntry { value: Arc::from(Vec::new()), version, ttl: None, is_tombstone: true };

        let entry_size = key.len() + 16;

        self.data.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        version
    }

    /// Delete key (for proactive TTL cleanup)
    pub fn delete(&self, key: &[u8]) {
        if let Some(entry) = self.data.remove(key) {
            let entry_size = entry.key().len() + entry.value().value.len() + 16;
            self.size.fetch_sub(entry_size, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::helpers::MEBI;

    #[test]
    fn test_version_tracking() {
        let memtable = Memtable::default();

        // Write v1
        let v1 = memtable.insert(b"key1", b"value_v1", None);

        // Write v2 (overwrites v1 in SkipMap)
        let v2 = memtable.insert(b"key1", b"value_v2", None);

        // Version must increment (monotonic counter)
        assert!(v2 > v1, "Version must increment");

        // Latest value is visible
        let snapshot = memtable.current_version();
        let val = memtable.get(b"key1", snapshot);
        let expected: Arc<[u8]> = b"value_v2".to_vec().into();
        assert_eq!(val, LookupResult::Value(expected));

        // Reads can't see writes that happened after snapshot
        let past_snapshot = v2 - 1;
        let val_filtered = memtable.get(b"key1", past_snapshot);
        assert_eq!(val_filtered, LookupResult::NotFound);
    }

    #[test]
    fn test_flush_threshold() {
        let memtable = Memtable::default();
        let flush_threshold = 64 * MEBI; // 64MB

        // Initially should not flush
        assert!(!memtable.should_flush(flush_threshold));

        // Insert large value to exceed threshold
        let large_value = vec![0; 65 * MEBI]; // 65MB
        memtable.insert(b"large_key", &large_value, None);

        // Should trigger flush
        assert!(memtable.should_flush(flush_threshold));
    }

    #[test]
    fn test_clear_after_flush() {
        let memtable = Memtable::default();

        memtable.insert(b"key1", b"value1", None);
        memtable.insert(b"key2", b"value2", None);
        let version_before_clear = memtable.current_version();

        assert_eq!(memtable.len(), 2);
        assert!(memtable.size() > 0);

        // Clear (simulates post-flush)
        memtable.clear();

        assert_eq!(memtable.len(), 0);
        assert_eq!(memtable.size(), 0);

        // Version counter NOT reset (monotonically increasing)
        assert_eq!(memtable.current_version(), version_before_clear);
    }

    #[test]
    fn test_sorted_iteration() {
        let memtable = Memtable::default();

        // Insert out of order
        memtable.insert(b"key3", b"value3", None);
        memtable.insert(b"key1", b"value1", None);
        memtable.insert(b"key2", b"value2", None);

        // Iteration should be sorted
        let keys: Vec<_> = memtable.iter().map(|rec| rec.key).collect();
        let expected: Vec<Arc<[u8]>> =
            vec![b"key1".to_vec().into(), b"key2".to_vec().into(), b"key3".to_vec().into()];
        assert_eq!(keys, expected);
    }
}
