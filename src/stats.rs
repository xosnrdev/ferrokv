use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::memtable::Memtable;
use crate::sstable::SSTable;

/// Ferrokv statistics and metadata
pub struct FerroStats {
    /// Number of entries in memtable
    pub memtable_keys: usize,

    /// Memory used by memtable in bytes
    pub memtable_size: usize,

    /// Number of `SSTable` files on disk
    pub sstable_count: usize,

    /// Total disk space used by `SSTables` in bytes
    pub total_disk: u64,

    /// Current MVCC version
    pub current_version: u64,
}

impl FerroStats {
    pub(crate) fn new(memtable: &Memtable, sstables: &Arc<ArcSwap<Vec<Arc<SSTable>>>>) -> Self {
        let memtable_keys = memtable.len();
        let memtable_size = memtable.size();
        let current_version = memtable.current_version();

        let sstables = sstables.load();
        let sstable_count = sstables.len();
        let total_disk = sstables.iter().map(|sst| sst.size).sum();

        Self { memtable_keys, memtable_size, sstable_count, total_disk, current_version }
    }
}
