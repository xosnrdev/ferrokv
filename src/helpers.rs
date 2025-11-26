use std::sync::LazyLock;
use std::time::Instant;

pub const KIBI: usize = 1024;
pub const MEBI: usize = KIBI * KIBI;

/// Data Block size (4KB aligns with OS page cache)
pub const BLOCK_SIZE: usize = 4 * KIBI;

/// Magic number for `SSTable` footer validation
pub const FOOTER_MAGIC: u32 = 0xFE77_0557;

pub fn get_now() -> u64 {
    static START_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);
    START_TIME.elapsed().as_secs()
}
