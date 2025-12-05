use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::bloom::BloomFilter;
use crate::errors::{FerroError, Result};
use crate::helpers::{BLOCK_SIZE, FOOTER_MAGIC, FOOTER_SIZE, get_cached_now};
use crate::memtable::Memtable;
use crate::storage::ScanBounds;

/// Record flags
pub const FLAG_HAS_TTL: u8 = 0x01;
const FLAG_TOMBSTONE: u8 = 0x02;

/// Type alias for `SSTable` read result to make clippy happy: (value, version, ttl)
type ReadResult = Option<(Arc<[u8]>, u64, Option<u64>)>;

/// Decoded record from Data Block
#[derive(Debug)]
pub struct Record {
    pub key: Arc<[u8]>,
    pub value: Arc<[u8]>,
    pub version: u64,
    pub ttl: Option<u64>,
    pub is_tombstone: bool,
}

/// Index Block entry
#[derive(Debug)]
struct IndexEntry {
    first_key: Arc<[u8]>,
    offset: u64,
}

/// `SSTable` metadata (immutable on-disk file)
pub struct SSTable {
    path: PathBuf,
    level: u8,
    id: u64,
    filter_offset: u64,
    index_offset: u64,
    first_key: Arc<[u8]>,
    last_key: Arc<[u8]>,
    size: u64,
}

impl SSTable {
    /// Flush Memtable to disk as `SSTable`
    pub async fn flush(dir: &Path, memtable: &Memtable, level: u8, id: u64) -> Result<Arc<Self>> {
        let filename = format!("L{level}-{id:05}.sst");
        let path = dir.join(&filename);
        let temp_path = dir.join(format!("{filename}.tmp"));

        let mut file = File::create(&temp_path).await?;
        let mut current_offset = 0;
        let mut block_buffer = Vec::with_capacity(BLOCK_SIZE);
        let mut index_entries = Vec::new();
        let mut first_key_opt = None;
        let mut last_key = Arc::from(Vec::new());

        // Collect all keys for Bloom filter
        let mut all_keys = Vec::new();

        // Write Data Blocks
        for record in memtable.iter() {
            let key = &record.key;
            let value = &record.value;
            let version = record.version;
            let ttl = record.ttl;

            // Collect key for Bloom filter
            all_keys.push(Arc::clone(key));

            // Track first/last keys for metadata
            if first_key_opt.is_none() {
                first_key_opt = Some(Arc::clone(&record.key));
            }
            last_key = Arc::clone(&record.key);

            // Build record
            let mut flags = 0;
            if ttl.is_some() {
                flags |= FLAG_HAS_TTL;
            }
            if record.is_tombstone {
                flags |= FLAG_TOMBSTONE;
            }

            let record_size = 1 // flags
                + 8 // version
                + (if ttl.is_some() { 8 } else { 0 }) // expire_at
                + 4 // key_len
                + 4 // val_len
                + key.len()
                + value.len();

            // Flush block if adding this record would exceed BLOCK_SIZE
            if !block_buffer.is_empty() && block_buffer.len() + record_size > BLOCK_SIZE {
                file.write_all(&block_buffer).await?;
                current_offset += block_buffer.len() as u64;
                block_buffer.clear();
            }

            // Record first key of new block for index
            if block_buffer.is_empty() {
                index_entries
                    .push(IndexEntry { first_key: Arc::clone(key), offset: current_offset });
            }

            // Write record to block buffer
            block_buffer.push(flags);
            block_buffer.extend_from_slice(&version.to_le_bytes());
            if let Some(expire_at) = ttl {
                block_buffer.extend_from_slice(&expire_at.to_le_bytes());
            }
            block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(key);
            block_buffer.extend_from_slice(value);
        }

        // Flush final block
        if !block_buffer.is_empty() {
            file.write_all(&block_buffer).await?;
            current_offset += block_buffer.len() as u64;
        }

        // Write Filter Block
        let filter_offset = current_offset;
        let mut bloom_filter = BloomFilter::new(all_keys.len(), 0.01);
        for key in &all_keys {
            bloom_filter.insert(key);
        }
        let filter_data = bloom_filter.serialize();
        file.write_all(&filter_data).await?;
        current_offset += filter_data.len() as u64;

        // Write Index Block
        let index_offset = current_offset;
        let index_count = index_entries.len() as u32;
        file.write_all(&index_count.to_le_bytes()).await?;
        current_offset += 4;

        for entry in &index_entries {
            let key_len = entry.first_key.len() as u32;
            file.write_all(&key_len.to_le_bytes()).await?;
            file.write_all(&entry.first_key).await?;
            file.write_all(&entry.offset.to_le_bytes()).await?;
            current_offset += 4 + u64::from(key_len) + 8;
        }

        // Write Footer: filter_offset(8) + index_offset(8) + magic(4) = 20 bytes
        file.write_all(&filter_offset.to_le_bytes()).await?;
        file.write_all(&index_offset.to_le_bytes()).await?;
        file.write_all(&FOOTER_MAGIC.to_le_bytes()).await?;
        current_offset += FOOTER_SIZE as u64;

        // ACID: Durability guarantee
        file.sync_all().await?;

        // Atomic rename
        tokio::fs::rename(&temp_path, &path).await?;

        let first_key =
            first_key_opt.ok_or(FerroError::InvalidData("Cannot flush empty Memtable".into()))?;

        // It's safe to drop here, as file is opened on each read
        drop(file);

        Ok(Arc::new(Self {
            path,
            level,
            id,
            filter_offset,
            index_offset,
            first_key,
            last_key,
            size: current_offset,
        }))
    }

    /// Point read with binary search
    /// Returns (value, version, ttl) for MVCC support
    pub async fn get(&self, key: &[u8]) -> Result<ReadResult> {
        if !self.check_bloom_filter(key).await? {
            return Ok(None);
        }

        // Load Index Block
        let index_entries = self.load_index().await?;

        // Binary search for containing block
        let block_idx =
            match index_entries.binary_search_by(|entry| entry.first_key.as_ref().cmp(key)) {
                Ok(idx) => idx,
                Err(idx) => {
                    if idx == 0 {
                        return Ok(None);
                    }
                    idx - 1
                }
            };

        let block_offset = index_entries[block_idx].offset;
        let next_offset = if block_idx + 1 < index_entries.len() {
            index_entries[block_idx + 1].offset
        } else {
            self.filter_offset
        };

        let path = self.path.clone();
        let key = key.to_vec();

        // mmap operations are blocking, we delegate them to blocking thread pool to maintain non-blocking guarantee
        tokio::task::spawn_blocking(move || {
            let file_std = std::fs::File::open(&path)?;
            let mmap = unsafe { Mmap::map(&file_std)? };

            let block_start = block_offset as usize;
            let block_end = next_offset as usize;
            let block_buf = &mmap[block_start..block_end];

            // Linear scan block
            let mut cursor = 0;
            while cursor < block_buf.len() {
                let flags = block_buf[cursor];
                cursor += 1;

                // Parse version
                if cursor + 8 > block_buf.len() {
                    break;
                }
                let version = u64::from_le_bytes(block_buf[cursor..cursor + 8].try_into()?);
                cursor += 8;

                let ttl = if flags & FLAG_HAS_TTL != 0 {
                    if cursor + 8 > block_buf.len() {
                        break;
                    }
                    let expire_at = u64::from_le_bytes(block_buf[cursor..cursor + 8].try_into()?);
                    cursor += 8;
                    Some(expire_at)
                } else {
                    None
                };

                if cursor + 8 > block_buf.len() {
                    break;
                }

                let key_len =
                    u32::from_le_bytes(block_buf[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;
                let val_len =
                    u32::from_le_bytes(block_buf[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;

                if cursor + key_len + val_len > block_buf.len() {
                    break;
                }

                let record_key = &block_buf[cursor..cursor + key_len];
                cursor += key_len;
                let record_val = &block_buf[cursor..cursor + val_len];
                cursor += val_len;

                if record_key == key {
                    // Check tombstone (deletion marker)
                    if flags & FLAG_TOMBSTONE != 0 {
                        return Ok(None);
                    }

                    // TTL lazy eviction with cached timestamp
                    if let Some(expire_at) = ttl
                        && get_cached_now() >= expire_at
                    {
                        return Ok(None);
                    }

                    let value: Arc<[u8]> = record_val.to_vec().into();
                    return Ok(Some((value, version, ttl)));
                }
            }

            Ok(None)
        })
        .await
        .map_err(|err| {
            FerroError::Io(std::io::Error::other(format!("SSTable read task failed: {err}")))
        })?
    }

    /// Check if key may exist in this `SSTable` using Bloom filter
    async fn check_bloom_filter(&self, key: &[u8]) -> Result<bool> {
        let path = self.path.clone();
        let filter_offset = self.filter_offset;
        let index_offset = self.index_offset;
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            let file_std = std::fs::File::open(&path)?;
            let mmap = unsafe { Mmap::map(&file_std)? };

            // Filter Block is between filter_offset and index_offset
            let filter_start = filter_offset as usize;
            let filter_end = index_offset as usize;
            let filter_data = &mmap[filter_start..filter_end];

            let bloom = BloomFilter::deserialize(filter_data)?;
            Ok(bloom.may_contain(&key))
        })
        .await
        .map_err(|err| {
            FerroError::Io(std::io::Error::other(format!("Bloom filter check failed: {err}")))
        })?
    }

    /// Load Index Block into memory
    async fn load_index(&self) -> Result<Vec<IndexEntry>> {
        let path = self.path.clone();
        let size = self.size;
        let filter_offset = self.filter_offset;
        let index_offset = self.index_offset;

        // mmap operations are blocking, we delegate them to blocking thread pool to maintain non-blocking guarantee
        tokio::task::spawn_blocking(move || {
            let file_std = std::fs::File::open(&path)?;
            let mmap = unsafe { Mmap::map(&file_std)? };

            // Read Footer (last 20 bytes): filter_offset(8) + index_offset(8) + magic(4)
            let footer_start = (size - FOOTER_SIZE as u64) as usize;
            let footer = &mmap[footer_start..footer_start + FOOTER_SIZE];

            let stored_filter_offset = u64::from_le_bytes(footer[0..8].try_into()?);
            let stored_index_offset = u64::from_le_bytes(footer[8..16].try_into()?);
            let magic = u32::from_le_bytes(footer[16..20].try_into()?);

            // ACID Corruption detection
            if magic != FOOTER_MAGIC {
                return Err(FerroError::Corruption(
                    format!(
                        "Invalid SSTable magic number: expected 0x{FOOTER_MAGIC:08X}, got 0x{magic:08X}",
                    )
                    .into(),
                ));
            }

            if stored_filter_offset != filter_offset {
                return Err(FerroError::Corruption("Filter offset mismatch in Footer".into()));
            }

            if stored_index_offset != index_offset {
                return Err(FerroError::Corruption("Index offset mismatch in Footer".into()));
            }

            // Read Index Block
            let index_start = index_offset as usize;
            let index_end = (size - FOOTER_SIZE as u64) as usize;
            let index_buf = &mmap[index_start..index_end];

            let mut cursor = 0;
            let index_count = u32::from_le_bytes(index_buf[cursor..cursor + 4].try_into()?) as usize;
            cursor += 4;

            let mut entries = Vec::with_capacity(index_count);
            for _ in 0..index_count {
                let key_len = u32::from_le_bytes(index_buf[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;

                let key: Arc<[u8]> = index_buf[cursor..cursor + key_len].to_vec().into();
                cursor += key_len;

                let offset = u64::from_le_bytes(index_buf[cursor..cursor + 8].try_into()?);
                cursor += 8;

                entries.push(IndexEntry { first_key: key, offset });
            }

            Ok(entries)
        }).await.map_err(|err| FerroError::Io(std::io::Error::other(
            format!("Index load task failed: {err}")
        )))?
    }

    /// Get `SSTable` path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get `SSTable` level (for compaction)
    pub fn level(&self) -> u8 {
        self.level
    }

    /// Get `SSTable` ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if key might be in this `SSTable` (range check)
    pub fn contains_key_range(&self, key: &[u8]) -> bool {
        key >= self.first_key.as_ref() && key <= self.last_key.as_ref()
    }

    /// Check if this `SSTable` might contain keys in the given range
    pub fn overlaps_range(&self, range: &ScanBounds) -> bool {
        // Check if SSTable's [first_key, last_key] overlaps with the query range
        let range_start_before_sst_end = match &range.0 {
            Bound::Included(start) => start.as_ref() <= self.last_key.as_ref(),
            Bound::Excluded(start) => start.as_ref() < self.last_key.as_ref(),
            Bound::Unbounded => true,
        };

        let range_end_after_sst_start = match &range.1 {
            Bound::Included(end) => end.as_ref() >= self.first_key.as_ref(),
            Bound::Excluded(end) => end.as_ref() > self.first_key.as_ref(),
            Bound::Unbounded => true,
        };

        range_start_before_sst_end && range_end_after_sst_start
    }

    /// Scan records in the given key range
    pub async fn scan_range(&self, range: &ScanBounds, now: u64) -> Result<Vec<Record>> {
        let path = self.path.clone();
        let filter_offset = self.filter_offset;
        let range_start = range.0.clone();
        let range_end = range.1.clone();

        tokio::task::spawn_blocking(move || {
            let file_std = std::fs::File::open(&path)?;
            let mmap = unsafe { Mmap::map(&file_std)? };

            let data_end = filter_offset as usize;
            let mut records = Vec::new();
            let mut cursor = 0;

            while cursor < data_end {
                // Parse record
                if cursor + 1 > data_end {
                    break;
                }

                let flags = mmap[cursor];
                cursor += 1;

                // Parse version
                if cursor + 8 > data_end {
                    break;
                }
                let version = u64::from_le_bytes(mmap[cursor..cursor + 8].try_into()?);
                cursor += 8;

                // Parse TTL if present
                let ttl = if flags & FLAG_HAS_TTL != 0 {
                    if cursor + 8 > data_end {
                        break;
                    }
                    let expire_at = u64::from_le_bytes(mmap[cursor..cursor + 8].try_into()?);
                    cursor += 8;
                    Some(expire_at)
                } else {
                    None
                };

                // Parse key and value lengths
                if cursor + 8 > data_end {
                    break;
                }

                let key_len = u32::from_le_bytes(mmap[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;

                let val_len = u32::from_le_bytes(mmap[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;

                // Parse key and value
                if cursor + key_len + val_len > data_end {
                    break;
                }

                // Get key slice
                let key_slice = &mmap[cursor..cursor + key_len];

                let past_end = Self::slice_past_end(key_slice, &range_end);
                if past_end {
                    break;
                }

                let in_range = Self::slice_in_range(key_slice, &range_start, &range_end);
                if !in_range {
                    cursor += key_len + val_len;
                    continue;
                }

                // Check TTL
                if let Some(expire_at) = ttl
                    && now >= expire_at
                {
                    cursor += key_len + val_len;
                    continue;
                }

                let key: Arc<[u8]> = key_slice.to_vec().into();
                cursor += key_len;
                let value: Arc<[u8]> = mmap[cursor..cursor + val_len].to_vec().into();
                cursor += val_len;

                records.push(Record {
                    key,
                    value,
                    version,
                    ttl,
                    is_tombstone: flags & FLAG_TOMBSTONE != 0,
                });
            }

            Ok(records)
        })
        .await
        .map_err(|err| {
            FerroError::Io(std::io::Error::other(format!("SSTable scan failed: {err}")))
        })?
    }

    /// Check if key slice is past the end bound
    fn slice_past_end(key: &[u8], end: &Bound<Arc<[u8]>>) -> bool {
        match end {
            Bound::Included(e) => key > e.as_ref(),
            Bound::Excluded(e) => key >= e.as_ref(),
            Bound::Unbounded => false,
        }
    }

    /// Check if key slice falls within range bounds
    fn slice_in_range(key: &[u8], start: &Bound<Arc<[u8]>>, end: &Bound<Arc<[u8]>>) -> bool {
        let after_start = match start {
            Bound::Included(s) => key >= s.as_ref(),
            Bound::Excluded(s) => key > s.as_ref(),
            Bound::Unbounded => true,
        };

        let before_end = match end {
            Bound::Included(e) => key <= e.as_ref(),
            Bound::Excluded(e) => key < e.as_ref(),
            Bound::Unbounded => true,
        };

        after_start && before_end
    }

    /// Create `SSTable` from metadata (for compaction)
    pub fn from_metadata(
        path: PathBuf,
        level: u8,
        id: u64,
        filter_offset: u64,
        index_offset: u64,
        first_key: Arc<[u8]>,
        last_key: Arc<[u8]>,
        size: u64,
    ) -> Self {
        Self { path, level, id, filter_offset, index_offset, first_key, last_key, size }
    }

    /// Open existing `SSTable` from disk (for startup recovery)
    pub async fn open(path: PathBuf) -> Result<Arc<Self>> {
        // Parse filename: L{level}-{id:05}.sst
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(FerroError::InvalidData("Invalid SSTable filename".into()))?;

        let parts: Vec<&str> = filename.trim_end_matches(".sst").split('-').collect();
        if parts.len() != 2 {
            return Err(FerroError::InvalidData("Invalid SSTable filename format".into()));
        }

        let level = parts[0]
            .trim_start_matches('L')
            .parse::<u8>()
            .map_err(|_| FerroError::InvalidData("Invalid level in filename".into()))?;

        let id = parts[1]
            .parse::<u64>()
            .map_err(|_| FerroError::InvalidData("Invalid ID in filename".into()))?;

        // Get file size
        let metadata = tokio::fs::metadata(&path).await?;
        let size = metadata.len();

        if size < FOOTER_SIZE as u64 {
            return Err(FerroError::InvalidData("SSTable file too small".into()));
        }

        let path_clone = path.clone();

        // mmap operations are blocking, we delegate them to blocking thread pool to maintain non-blocking guarantee
        tokio::task::spawn_blocking(move || {
            let file_std = std::fs::File::open(&path_clone)?;
            let mmap = unsafe { Mmap::map(&file_std)? };

            // Read Footer (last 20 bytes): filter_offset(8) + index_offset(8) + magic(4)
            let footer_start = (size - FOOTER_SIZE as u64) as usize;
            let footer = &mmap[footer_start..footer_start + FOOTER_SIZE];

            let filter_offset = u64::from_le_bytes(footer[0..8].try_into()?);
            let index_offset = u64::from_le_bytes(footer[8..16].try_into()?);
            let magic = u32::from_le_bytes(footer[16..20].try_into()?);

            if magic != FOOTER_MAGIC {
                return Err(FerroError::Corruption(
                    format!(
                        "Invalid SSTable magic: expected 0x{FOOTER_MAGIC:08X}, got 0x{magic:08X}"
                    )
                    .into(),
                ));
            }

            // Read Index Block to get first_key and last_key
            let index_start = index_offset as usize;
            let index_end = (size - FOOTER_SIZE as u64) as usize;
            let index_buf = &mmap[index_start..index_end];

            let mut cursor = 0;
            let index_count =
                u32::from_le_bytes(index_buf[cursor..cursor + 4].try_into()?) as usize;
            cursor += 4;

            if index_count == 0 {
                return Err(FerroError::InvalidData("Empty SSTable index".into()));
            }

            // Read first key from first index entry
            let key_len = u32::from_le_bytes(index_buf[cursor..cursor + 4].try_into()?) as usize;
            cursor += 4;
            let first_key: Arc<[u8]> = index_buf[cursor..cursor + key_len].to_vec().into();
            cursor += key_len;
            cursor += 8;

            // Skip remaining index entries (we'll scan data blocks for last key)
            for _ in 1..index_count {
                let key_len =
                    u32::from_le_bytes(index_buf[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;
                cursor += key_len;
                cursor += 8;
            }

            // Scan all data blocks to find the actual last key
            // (Index only contains first keys of blocks, not the last key of SSTable)
            // Data blocks end at filter_offset (Filter Block starts there)
            let data_start = 0;
            let data_end = filter_offset as usize;
            let data_buf = &mmap[data_start..data_end];

            let mut last_key = Arc::clone(&first_key);
            let mut cursor = 0;
            while cursor < data_buf.len() {
                let flags = data_buf[cursor];
                cursor += 1;

                // Parse version
                if cursor + 8 > data_buf.len() {
                    break;
                }
                cursor += 8;

                // Parse TTL if present
                if flags & FLAG_HAS_TTL != 0 {
                    if cursor + 8 > data_buf.len() {
                        break;
                    }
                    cursor += 8;
                }

                // Parse key_len and val_len
                if cursor + 8 > data_buf.len() {
                    break;
                }
                let key_len = u32::from_le_bytes(data_buf[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;
                let val_len = u32::from_le_bytes(data_buf[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;

                if cursor + key_len + val_len > data_buf.len() {
                    break;
                }
                last_key = data_buf[cursor..cursor + key_len].to_vec().into();
                cursor += key_len;
                cursor += val_len;
            }

            Ok(Arc::new(Self {
                path,
                level,
                id,
                filter_offset,
                index_offset,
                first_key,
                last_key,
                size,
            }))
        })
        .await
        .map_err(|err| {
            FerroError::Io(std::io::Error::other(format!("SSTable open task failed: {err}")))
        })?
    }
}

/// Iterator for sequential `SSTable` record scanning for compaction
pub struct SSTIterator {
    mmap: Mmap,
    data_end: usize,
    cursor: usize,
}

impl SSTIterator {
    /// Create iterator from `SSTable`
    pub async fn new(sstable: &SSTable) -> Result<Self> {
        let path = sstable.path.clone();
        let data_end = sstable.filter_offset as usize;

        // mmap operations are blocking, we delegate them to blocking thread pool to maintain non-blocking guarantee
        tokio::task::spawn_blocking(move || {
            let file_std = std::fs::File::open(&path)?;
            let mmap = unsafe { Mmap::map(&file_std)? };
            Ok(Self { mmap, data_end, cursor: 0 })
        })
        .await
        .map_err(|err| {
            FerroError::Io(std::io::Error::other(format!("SSTIterator init task failed: {err}")))
        })?
    }

    /// Get next record from Data Blocks (not `std::iter::Iterator` to avoid `Option<Result<Option<T>>>`)
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        if self.cursor >= self.data_end {
            return Ok(None);
        }

        // Parse record header
        if self.cursor + 1 > self.data_end {
            return Ok(None);
        }

        let flags = self.mmap[self.cursor];
        self.cursor += 1;

        // Parse version
        if self.cursor + 8 > self.data_end {
            return Ok(None);
        }
        let version = u64::from_le_bytes(self.mmap[self.cursor..self.cursor + 8].try_into()?);
        self.cursor += 8;

        // Parse TTL if present
        let ttl = if flags & FLAG_HAS_TTL != 0 {
            if self.cursor + 8 > self.data_end {
                return Ok(None);
            }
            let expire_at = u64::from_le_bytes(self.mmap[self.cursor..self.cursor + 8].try_into()?);
            self.cursor += 8;
            Some(expire_at)
        } else {
            None
        };

        // Parse key and value lengths
        if self.cursor + 8 > self.data_end {
            return Ok(None);
        }

        let key_len =
            u32::from_le_bytes(self.mmap[self.cursor..self.cursor + 4].try_into()?) as usize;
        self.cursor += 4;

        let val_len =
            u32::from_le_bytes(self.mmap[self.cursor..self.cursor + 4].try_into()?) as usize;
        self.cursor += 4;

        // Parse key and value
        if self.cursor + key_len + val_len > self.data_end {
            return Ok(None);
        }

        let key: Arc<[u8]> = self.mmap[self.cursor..self.cursor + key_len].to_vec().into();
        self.cursor += key_len;

        let value: Arc<[u8]> = self.mmap[self.cursor..self.cursor + val_len].to_vec().into();
        self.cursor += val_len;

        Ok(Some(Record { key, value, version, ttl, is_tombstone: flags & FLAG_TOMBSTONE != 0 }))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_flush_memtable() {
        let db_dir = tempdir().unwrap().keep();

        let memtable = Memtable::default();
        memtable.insert(b"key1", b"value1", None);
        memtable.insert(b"key2", b"value2", None);
        memtable.insert(b"key3", b"value3", None);

        let sst = SSTable::flush(&db_dir, &memtable, 0, 1).await.unwrap();

        assert!(sst.path().exists());
        assert_eq!(sst.level(), 0);
        assert_eq!(sst.id(), 1);

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_sstable_get() {
        let db_dir = tempdir().unwrap().keep();
        let _ = tokio::fs::create_dir_all(&db_dir).await;

        let memtable = Memtable::default();
        memtable.insert(b"apple", b"red", None);
        memtable.insert(b"banana", b"yellow", None);
        memtable.insert(b"cherry", b"red", None);

        let sst = SSTable::flush(&db_dir, &memtable, 0, 1).await.unwrap();

        let result = sst.get(b"banana").await.unwrap();
        assert_eq!(result.unwrap().0.as_ref(), b"yellow");

        let result = sst.get(b"nonexistent").await.unwrap();
        assert!(result.is_none());

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_sstable_key_range() {
        let db_dir = tempdir().unwrap().keep();
        let _ = tokio::fs::create_dir_all(&db_dir).await;

        let memtable = Memtable::default();
        memtable.insert(b"key100", b"value100", None);
        memtable.insert(b"key200", b"value200", None);
        memtable.insert(b"key300", b"value300", None);

        let sst = SSTable::flush(&db_dir, &memtable, 0, 1).await.unwrap();

        assert!(sst.contains_key_range(b"key200"));
        assert!(!sst.contains_key_range(b"key050")); // Before range
        assert!(!sst.contains_key_range(b"key400")); // After range

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_bloom_filter_negative_lookup() {
        let db_dir = tempdir().unwrap().keep();
        let _ = tokio::fs::create_dir_all(&db_dir).await;

        let memtable = Memtable::default();
        for i in 0..100 {
            let key = format!("key_{i:03}");
            let value = format!("value_{i:03}");
            memtable.insert(key.as_bytes(), value.as_bytes(), None);
        }

        let sst = SSTable::flush(&db_dir, &memtable, 0, 1).await.unwrap();

        // Bloom filter should return false for definitely non-existent keys
        // (Note: false positives possible, but with 1% FPR should be rare)
        let mut false_negatives = 0;
        for i in 1000..1100 {
            let key = format!("nonexistent_{i}");
            let result = sst.get(key.as_bytes()).await.unwrap();
            if result.is_some() {
                false_negatives += 1;
            }
        }
        assert_eq!(false_negatives, 0, "Bloom filter must not have false negatives");

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_bloom_filter_positive_lookup() {
        let db_dir = tempdir().unwrap().keep();
        let _ = tokio::fs::create_dir_all(&db_dir).await;

        let memtable = Memtable::default();
        for i in 0..50 {
            let key = format!("existing_{i:03}");
            let value = format!("value_{i:03}");
            memtable.insert(key.as_bytes(), value.as_bytes(), None);
        }

        let sst = SSTable::flush(&db_dir, &memtable, 0, 1).await.unwrap();

        // All existing keys must be found (bloom filter must not have false negatives)
        for i in 0..50 {
            let key = format!("existing_{i:03}");
            let result = sst.get(key.as_bytes()).await.unwrap();
            assert!(result.is_some(), "Key {key} must be found");
        }

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }

    #[tokio::test]
    async fn test_sstable_open_with_bloom_filter() {
        let db_dir = tempdir().unwrap().keep();
        let _ = tokio::fs::create_dir_all(&db_dir).await;

        // Create SSTable
        let memtable = Memtable::default();
        memtable.insert(b"alpha", b"one", None);
        memtable.insert(b"beta", b"two", None);
        memtable.insert(b"gamma", b"three", None);

        let sst = SSTable::flush(&db_dir, &memtable, 0, 1).await.unwrap();
        let sst_path = sst.path().to_path_buf();
        drop(sst);

        // Reopen from disk
        let reopened = SSTable::open(sst_path).await.unwrap();

        // Verify bloom filter works after reopen
        let result = reopened.get(b"beta").await.unwrap();
        assert_eq!(result.unwrap().0.as_ref(), b"two");

        let result = reopened.get(b"nonexistent").await.unwrap();
        assert!(result.is_none());

        let _ = tokio::fs::remove_dir_all(&db_dir).await;
    }
}
