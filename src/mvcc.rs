use std::sync::atomic::{AtomicU64, Ordering};

/// Version counter for MVCC versioning
pub struct VersionCounter {
    counter: AtomicU64,
}

impl VersionCounter {
    /// Create new version counter starting at version 1
    pub fn new() -> Self {
        Self { counter: AtomicU64::new(1) }
    }

    /// Get next version number (monotonically increasing)
    pub fn next_version(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current version without incrementing
    pub fn current_version(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

impl Default for VersionCounter {
    fn default() -> Self {
        Self::new()
    }
}
