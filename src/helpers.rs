use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const KIBI: usize = 1024;
pub const MEBI: usize = KIBI * KIBI;

/// Data Block size as per OS page size (4KB)
pub const BLOCK_SIZE: usize = 4 * KIBI;

/// Magic number for `SSTable` footer validation
pub const FOOTER_MAGIC: u32 = 0xFE77_0557;

/// Footer size: `filter_offset(8)` + `index_offset(8)` + magic(4)
pub const FOOTER_SIZE: usize = 20;

pub fn get_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Global cached timestamp for hot-path TTL checks
/// Updated by background task, avoids syscall overhead on reads
static CACHED_TIMESTAMP: AtomicU64 = AtomicU64::new(0);

/// Get cached Unix timestamp in seconds
/// Returns cached value for TTL expiry checks in hot read paths
#[inline]
pub fn get_cached_now() -> u64 {
    let cached = CACHED_TIMESTAMP.load(Ordering::Relaxed);
    if cached == 0 {
        let now = get_now();
        CACHED_TIMESTAMP.store(now, Ordering::Relaxed);
        now
    } else {
        cached
    }
}

/// Refresh the cached timestamp (called by background task)
#[inline]
pub fn refresh_cached_timestamp() {
    CACHED_TIMESTAMP.store(get_now(), Ordering::Relaxed);
}
