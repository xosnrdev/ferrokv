use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::errors::{FerroError, Result};
use crate::helpers::{BLOCK_SIZE, FOOTER_MAGIC, get_now};
use crate::sstable::{FLAG_HAS_TTL, Record, SSTIterator, SSTable};

/// Heap entry for multi-way merge
struct HeapEntry {
    record: Record,
    source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.record.key == other.record.key
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.record.key.cmp(&self.record.key)
    }
}

/// `SSTable` builder for compaction output
struct SSTBuilder {
    dir: Arc<Path>,
    level: u8,
    id: u64,
    block_buffer: Vec<u8>,
    index_entries: Vec<(Arc<[u8]>, u64)>,
    first_key: Option<Arc<[u8]>>,
    last_key: Arc<[u8]>,
    current_offset: u64,
    written_sstables: Vec<Arc<SSTable>>,
    next_id: Arc<AtomicU64>,
}

impl SSTBuilder {
    fn new(dir: &Path, level: u8, id: u64, next_id: Arc<AtomicU64>) -> Self {
        Self {
            dir: Arc::from(dir),
            level,
            id,
            block_buffer: Vec::with_capacity(BLOCK_SIZE),
            index_entries: Vec::new(),
            first_key: None,
            last_key: Arc::from(Vec::new()),
            current_offset: 0,
            written_sstables: Vec::new(),
            next_id,
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8], version: u64, ttl: Option<u64>) {
        // Track first/last keys
        if self.first_key.is_none() {
            self.first_key = Some(Arc::from(key));
        }
        self.last_key = Arc::from(key);

        // Build record
        let mut flags = 0;
        if ttl.is_some() {
            flags |= FLAG_HAS_TTL;
        }

        let record_size = 1 // flags
            + 8 // version
            + (if ttl.is_some() { 8 } else { 0 }) // expire_at
            + 4 // key_len
            + 4 // val_len
            + key.len()
            + value.len();

        // Flush block if adding this record would exceed BLOCK_SIZE
        if !self.block_buffer.is_empty() && self.block_buffer.len() + record_size > BLOCK_SIZE {
            self.current_offset = self.block_buffer.len() as u64;
        }

        // Record first key of new block for index
        if self.block_buffer.is_empty() {
            self.index_entries.push((Arc::from(key), self.current_offset));
        }

        // Write record to block buffer
        self.block_buffer.push(flags);
        self.block_buffer.extend_from_slice(&version.to_le_bytes());
        if let Some(expire_at) = ttl {
            self.block_buffer.extend_from_slice(&expire_at.to_le_bytes());
        }
        self.block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.block_buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.block_buffer.extend_from_slice(key);
        self.block_buffer.extend_from_slice(value);
    }

    fn size(&self) -> u64 {
        self.current_offset + self.block_buffer.len() as u64
    }

    fn is_empty(&self) -> bool {
        self.first_key.is_none()
    }

    async fn flush(&mut self) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let filename = format!("L{}-{:05}.sst", self.level, self.id);
        let path = self.dir.join(&filename);
        let temp_path = self.dir.join(format!("{filename}.tmp"));

        let mut file = File::create(&temp_path).await?;
        let mut total_offset = 0;

        // Write accumulated blocks
        if !self.block_buffer.is_empty() {
            file.write_all(&self.block_buffer).await?;
            total_offset += self.block_buffer.len() as u64;
        }

        let index_offset = total_offset;

        // Write Index Block
        let index_count = self.index_entries.len() as u32;
        file.write_all(&index_count.to_le_bytes()).await?;
        total_offset += 4;

        for (key, offset) in &self.index_entries {
            let key_len = key.len() as u32;
            file.write_all(&key_len.to_le_bytes()).await?;
            file.write_all(key).await?;
            file.write_all(&offset.to_le_bytes()).await?;
            total_offset += 4 + u64::from(key_len) + 8;
        }

        // Write Footer
        file.write_all(&index_offset.to_le_bytes()).await?;
        file.write_all(&FOOTER_MAGIC.to_le_bytes()).await?;
        total_offset += 12;

        // ACID: Durability guarantee
        file.sync_all().await?;

        // Atomic rename
        tokio::fs::rename(&temp_path, &path).await?;

        let first_key = self
            .first_key
            .take()
            .ok_or_else(|| FerroError::InvalidData("Cannot flush empty SSTable builder".into()))?;
        let sstable = SSTable::from_metadata(
            path,
            self.level,
            self.id,
            index_offset,
            first_key,
            Arc::clone(&self.last_key),
            total_offset,
        );

        self.written_sstables.push(Arc::new(sstable));

        // Reset for next SSTable
        self.id = self.next_id.fetch_add(1, AtomicOrdering::SeqCst);
        self.block_buffer.clear();
        self.index_entries.clear();
        self.first_key = None;
        self.last_key = Arc::from(Vec::new());
        self.current_offset = 0;

        Ok(())
    }

    fn into_sstables(self) -> Vec<Arc<SSTable>> {
        self.written_sstables
    }
}

/// Merge multiple `SSTables` into fewer `SSTables` at target level
pub async fn merge_sstables(
    sstables: &[Arc<SSTable>],
    output_level: u8,
    db_dir: &Path,
    next_id: &Arc<AtomicU64>,
    max_sstable_size: usize,
) -> Result<Vec<Arc<SSTable>>> {
    if sstables.is_empty() {
        return Ok(Vec::new());
    }

    // Open iterators for all input SSTables
    let mut iterators = Vec::with_capacity(sstables.len());
    for sst in sstables {
        let iter = SSTIterator::new(sst).await?;
        iterators.push(iter);
    }

    // Min-heap for multi-way merge
    let mut heap = BinaryHeap::new();

    // Initialize heap with first record from each SSTable
    for (idx, iter) in iterators.iter_mut().enumerate() {
        if let Some(record) = iter.next_record()? {
            heap.push(HeapEntry { record, source_idx: idx });
        }
    }

    let first_id = next_id.fetch_add(1, AtomicOrdering::SeqCst);
    let mut builder = SSTBuilder::new(db_dir, output_level, first_id, Arc::clone(next_id));
    let mut last_key: Option<Arc<[u8]>> = None;

    while let Some(entry) = heap.pop() {
        let record = entry.record;

        // FILTER #1: Skip duplicate keys (keep first = newest)
        if let Some(ref prev_key) = last_key
            && prev_key.as_ref() == record.key.as_ref()
        {
            // Refill heap from same source
            if let Some(next_record) = iterators[entry.source_idx].next_record()? {
                heap.push(HeapEntry { record: next_record, source_idx: entry.source_idx });
            }
            continue;
        }

        // FILTER #2: Drop tombstones (deleted keys)
        if record.is_tombstone {
            last_key = Some(record.key);
            // Refill heap
            if let Some(next_record) = iterators[entry.source_idx].next_record()? {
                heap.push(HeapEntry { record: next_record, source_idx: entry.source_idx });
            }
            continue;
        }

        // FILTER #3: Drop expired keys
        if let Some(expire_at) = record.ttl
            && get_now() >= expire_at
        {
            last_key = Some(record.key);
            // Refill heap
            if let Some(next_record) = iterators[entry.source_idx].next_record()? {
                heap.push(HeapEntry { record: next_record, source_idx: entry.source_idx });
            }
            continue;
        }

        // Write valid record to output
        builder.add(&record.key, &record.value, record.version, record.ttl);
        last_key = Some(record.key);

        // Refill heap from same source
        if let Some(next_record) = iterators[entry.source_idx].next_record()? {
            heap.push(HeapEntry { record: next_record, source_idx: entry.source_idx });
        }

        // Split output if exceeds max SSTable size
        if builder.size() >= max_sstable_size as u64 {
            builder.flush().await?;
        }
    }

    // Flush final SSTable
    if !builder.is_empty() {
        builder.flush().await?;
    }

    Ok(builder.into_sstables())
}
