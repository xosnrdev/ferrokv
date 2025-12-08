use std::time::{SystemTime, UNIX_EPOCH};

pub const KIBI: usize = 1024;
pub const MEBI: usize = KIBI * KIBI;

/// Data Block size as per OS page size (4KB)
pub const BLOCK_SIZE: usize = 4 * KIBI;

/// Magic number for `SSTable` footer validation
pub const FOOTER_MAGIC: u32 = 0xFE77_0557;

/// Footer size: `filter_offset(8)` + `index_offset(8)` + magic(4)
pub const FOOTER_SIZE: usize = 20;

/// Default database path
pub const DB_PATH: &str = "./ferrokv";

pub fn get_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
