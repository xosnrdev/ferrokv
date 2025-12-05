use std::sync::atomic::{AtomicU64, Ordering};

/// Version counter for MVCC versioning
pub struct VersionCounter {
    counter: AtomicU64,
}

impl Default for VersionCounter {
    fn default() -> Self {
        Self { counter: AtomicU64::new(1) }
    }
}

impl VersionCounter {
    /// Get next version number (monotonically increasing)
    #[inline]
    pub fn next_version(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current version without incrementing
    #[inline]
    pub fn current_version(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}
