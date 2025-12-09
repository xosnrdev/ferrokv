use std::path::{Path, PathBuf};

use bytes::Bytes;
use crc32fast::Hasher;
use memmap2::Mmap;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::errors::{FerroError, Result};

/// Special TTL value to mark tombstones
const TOMBSTONE_MARKER: u64 = u64::MAX;

/// Type alias for batch entry: (key, value, ttl, `is_tombstone`)
pub type BatchWalEntry<'a> = (&'a [u8], &'a [u8], Option<u64>, bool);

/// WAL entry representation in memory
#[derive(Debug)]
pub struct WalEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub ttl: Option<u64>,
    pub is_tombstone: bool,
}

impl WalEntry {
    /// Deserialize entry from bytes with CRC verification
    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 20 {
            // Minimum: CRC(4) + KeyLen(4) + ValLen(4) + TTL(8)
            return Err(FerroError::InvalidData("Entry too short".into()));
        }

        // Read CRC
        let stored_crc = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(&data[4..]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(FerroError::Corruption(
                format!("CRC mismatch: expected {stored_crc}, got {computed_crc}").into(),
            ));
        }

        // Read lengths
        let key_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let val_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let ttl_val = u64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]);

        // Validate lengths
        let expected_len = 20 + key_len + val_len;
        if data.len() < expected_len {
            return Err(FerroError::InvalidData(
                format!("Entry incomplete: expected {} bytes, got {}", expected_len, data.len())
                    .into(),
            ));
        }

        // Extract key and value
        let key_start = 20;
        let val_start = key_start + key_len;
        let key = Bytes::copy_from_slice(&data[key_start..val_start]);
        let value = Bytes::copy_from_slice(&data[val_start..val_start + val_len]);

        let is_tombstone = ttl_val == TOMBSTONE_MARKER && val_len == 0;
        let ttl = if ttl_val == TOMBSTONE_MARKER || ttl_val == 0 { None } else { Some(ttl_val) };

        Ok(Self { key, value, ttl, is_tombstone })
    }
}

/// Write-Ahead Log (WAL) for durability
pub struct Wal {
    file: File,
    offset: u64,
    write_buf: Vec<u8>,
}

impl Wal {
    /// Create or open a WAL file
    pub async fn new(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().create(true).write(true).append(true).open(&path).await?;

        let metadata = file.metadata().await?;
        let offset = metadata.len();
        Ok(Self { file, offset, write_buf: Vec::new() })
    }

    /// Append an entry to the WAL with durability guarantee
    /// CRITICAL: Does not return until `sync_data()` completes
    pub async fn append(&mut self, key: &[u8], value: &[u8], ttl: Option<u64>) -> Result<()> {
        let key_len = key.len() as u32;
        let val_len = value.len() as u32;
        let ttl_val = ttl.unwrap_or_default();

        let total_len = 20 + key.len() + value.len();
        self.write_buf.clear();
        self.write_buf.resize(total_len, 0);

        self.write_buf[4..8].copy_from_slice(&key_len.to_le_bytes());
        self.write_buf[8..12].copy_from_slice(&val_len.to_le_bytes());
        self.write_buf[12..20].copy_from_slice(&ttl_val.to_le_bytes());
        self.write_buf[20..20 + key.len()].copy_from_slice(key);
        self.write_buf[20 + key.len()..].copy_from_slice(value);

        // Calculate CRC32 over payload
        let mut hasher = Hasher::new();
        hasher.update(&self.write_buf[4..]);
        self.write_buf[0..4].copy_from_slice(&hasher.finalize().to_le_bytes());

        self.file.write_all(&self.write_buf).await?;
        self.offset += total_len as u64;

        self.sync().await
    }

    /// Write multiple entries with single fsync
    pub async fn append_batch(&mut self, entries: &[BatchWalEntry<'_>]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Calculate total size for all entries
        let total_size = entries.iter().map(|(k, v, ..)| 20 + k.len() + v.len()).sum();

        self.write_buf.clear();
        self.write_buf.reserve(total_size);

        // Serialize all entries into buffer
        for (key, value, ttl, is_tombstone) in entries {
            let start_pos = self.write_buf.len();
            let entry_len = 20 + key.len() + value.len();

            // Extend buffer for this entry
            self.write_buf.resize(start_pos + entry_len, 0);
            let entry_slice = &mut self.write_buf[start_pos..];

            // Write entry header
            let key_len = key.len() as u32;
            let val_len = value.len() as u32;
            let ttl_val = if *is_tombstone { TOMBSTONE_MARKER } else { ttl.unwrap_or_default() };

            entry_slice[4..8].copy_from_slice(&key_len.to_le_bytes());
            entry_slice[8..12].copy_from_slice(&val_len.to_le_bytes());
            entry_slice[12..20].copy_from_slice(&ttl_val.to_le_bytes());
            entry_slice[20..20 + key.len()].copy_from_slice(key);
            entry_slice[20 + key.len()..entry_len].copy_from_slice(value);

            // Calculate CRC32 for this entry
            let mut hasher = Hasher::new();
            hasher.update(&entry_slice[4..entry_len]);
            entry_slice[0..4].copy_from_slice(&hasher.finalize().to_le_bytes());
        }

        // Single write + single sync (group commit)
        self.file.write_all(&self.write_buf).await?;
        self.offset += total_size as u64;

        self.sync().await
    }

    /// Append tombstone to WAL
    pub async fn append_tombstone(&mut self, key: &[u8]) -> Result<()> {
        self.append(key, &[], Some(TOMBSTONE_MARKER)).await
    }

    /// Sync WAL to disk (durability guarantee)
    pub async fn sync(&mut self) -> Result<()> {
        self.file.sync_data().await?;
        Ok(())
    }

    /// Recover all entries from WAL (startup replay)
    pub async fn recover(path: &Path) -> Result<Vec<WalEntry>> {
        let path = path.to_path_buf();

        // mmap operations are blocking, we delegate them to blocking thread pool to maintain non-blocking guarantee
        tokio::task::spawn_blocking(move || {
            let file_std = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(err) => return Err(err.into()),
            };
            let mmap = unsafe { Mmap::map(&file_std)? };

            let mut entries = Vec::new();
            let mut offset = 0;

            while offset + 20 <= mmap.len() {
                // Parse entry header (20 bytes minimum)
                let header = &mmap[offset..offset + 20];
                let key_len =
                    u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
                let val_len =
                    u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

                let total_len = 20 + key_len + val_len;

                // Check if complete entry is available
                if offset + total_len > mmap.len() {
                    break;
                }

                // Deserialize and verify CRC
                let entry_slice = &mmap[offset..offset + total_len];
                match WalEntry::deserialize(entry_slice) {
                    Ok(entry) => entries.push(entry),
                    Err(FerroError::Corruption(msg)) => {
                        // Skip corrupted entry (partial write from crash)
                        eprintln!("WAL corruption at offset {offset}: {msg}");
                        break;
                    }
                    Err(err) => return Err(err),
                }

                offset += total_len;
            }

            Ok(entries)
        })
        .await
        .map_err(|err| {
            FerroError::Io(std::io::Error::other(format!("WAL recovery task failed: {err}")))
        })?
    }

    /// Truncate WAL (to be called after Memtable flush to `SSTable`)
    pub async fn truncate(&mut self) -> Result<()> {
        // Truncate file in place without reopening to preserve inode
        // This keeps the same file descriptor, preventing background tasks
        // with Arc clones from losing the file reference
        self.file.set_len(0).await?;
        self.file.sync_all().await?;

        self.offset = 0;
        self.write_buf.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::helpers::get_now;

    fn wal_path() -> PathBuf {
        tempdir().unwrap().keep().join("wal.log")
    }

    #[tokio::test]
    async fn test_truncate_reclaims_space() {
        let wal_path = wal_path();
        let mut wal = Wal::new(wal_path.clone()).await.unwrap();
        wal.append(b"key1", b"value1", None).await.unwrap();
        wal.append(b"key2", b"value2", None).await.unwrap();

        // Get file size before truncate
        let metadata_before = tokio::fs::metadata(&wal_path).await.unwrap();
        assert!(metadata_before.len() > 0, "WAL must have data");

        // Truncate (simulates post-Memtable flush)
        wal.truncate().await.unwrap();

        // Verify WAL is empty and offset reset
        assert_eq!(wal.offset, 0, "Offset must be reset");
        let metadata_after = tokio::fs::metadata(&wal_path).await.unwrap();
        assert_eq!(metadata_after.len(), 0, "WAL file must be empty after truncate");

        // Verify recovery returns no entries
        let entries = Wal::recover(&wal_path).await.unwrap();
        assert_eq!(entries.len(), 0, "Truncated WAL must be empty");

        tokio::fs::remove_file(&wal_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_sequential_writes() {
        let wal_path = wal_path();
        let mut wal = Wal::new(wal_path.clone()).await.unwrap();
        let initial_offset = wal.offset;

        // Append multiple entries
        wal.append(b"k1", b"v1", None).await.unwrap();
        let offset_after_first = wal.offset;
        assert!(offset_after_first > initial_offset, "Offset must advance");

        wal.append(b"k2", b"v2", None).await.unwrap();
        let offset_after_second = wal.offset;
        assert!(offset_after_second > offset_after_first, "Offset must advance sequentially");

        // Verify data is actually on disk (file size matches offset)
        let metadata = tokio::fs::metadata(&wal_path).await.unwrap();
        assert_eq!(metadata.len(), wal.offset, "File size must match WAL offset");

        tokio::fs::remove_file(&wal_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_batch() {
        let wal_path = wal_path();
        let ttl_3600 = get_now() + 3600;

        // Write batch of entries
        {
            let mut wal = Wal::new(wal_path.clone()).await.unwrap();
            let initial_offset = wal.offset;

            // Group commit: 3 entries, 1 fsync
            wal.append_batch(&[
                (b"key1", b"value1", None, false),
                (b"key2", b"value2", Some(ttl_3600), false),
                (b"key3", b"value3", None, false),
            ])
            .await
            .unwrap();

            // Verify offset advanced by total size
            let expected_size = (20 + 4 + 6) + (20 + 4 + 6) + (20 + 4 + 6);
            assert_eq!(wal.offset, initial_offset + expected_size);
        }

        // Recovery: verify all batch entries are durable
        let entries = Wal::recover(&wal_path).await.unwrap();
        assert_eq!(entries.len(), 3, "All batch entries must survive restart");

        assert_eq!(entries[0].key, Bytes::from("key1"));
        assert_eq!(entries[0].value, Bytes::from("value1"));
        assert_eq!(entries[0].ttl, None);

        assert_eq!(entries[1].key, Bytes::from("key2"));
        assert_eq!(entries[1].value, Bytes::from("value2"));
        assert_eq!(entries[1].ttl, Some(ttl_3600), "Batch preserves per-entry TTL");

        assert_eq!(entries[2].key, Bytes::from("key3"));
        assert_eq!(entries[2].value, Bytes::from("value3"));
        assert_eq!(entries[2].ttl, None);

        tokio::fs::remove_file(&wal_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_with_tombstones() {
        let wal_path = wal_path();

        {
            let mut wal = Wal::new(wal_path.clone()).await.unwrap();

            wal.append_batch(&[
                (b"key1", b"value1", None, false),
                (b"key2", b"", None, true),
                (b"key3", b"value3", None, false),
            ])
            .await
            .unwrap();
        }

        let entries = Wal::recover(&wal_path).await.unwrap();
        assert_eq!(entries.len(), 3);

        assert!(!entries[0].is_tombstone);
        assert_eq!(entries[0].key, Bytes::from("key1"));

        assert!(entries[1].is_tombstone, "Tombstone flag must be preserved");
        assert_eq!(entries[1].key, Bytes::from("key2"));

        assert!(!entries[2].is_tombstone);
        assert_eq!(entries[2].key, Bytes::from("key3"));

        tokio::fs::remove_file(&wal_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_with_empty() {
        let wal_path = wal_path();
        let mut wal = Wal::new(wal_path.clone()).await.unwrap();
        let initial_offset = wal.offset;

        // Empty batch should be no-op
        wal.append_batch(&[]).await.unwrap();
        assert_eq!(wal.offset, initial_offset, "Empty batch must not change offset");

        let entries = Wal::recover(&wal_path).await.unwrap();
        assert_eq!(entries.len(), 0);

        tokio::fs::remove_file(&wal_path).await.unwrap();
    }
}
