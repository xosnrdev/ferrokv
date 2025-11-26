use std::path::PathBuf;

use crate::errors::{FerroError, Result};
use crate::helpers::{KIBI, MEBI};
use crate::storage::FerroKv;

/// Configuration for `FerroKv` database
#[derive(Debug)]
pub struct Config {
    /// Memtable flush threshold in bytes (default: 64MB)
    pub memtable_size: usize,

    /// L0 compaction trigger threshold (default: 4 files)
    pub l0_compaction_threshold: usize,

    /// Maximum `SSTable` file size in bytes (default: 4MB)
    pub sstable_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { memtable_size: 64 * MEBI, l0_compaction_threshold: 4, sstable_size: 4 * MEBI }
    }
}

impl Config {
    /// Validate configuration values
    fn validate(&self) -> Result<()> {
        if self.memtable_size < MEBI {
            return Err(FerroError::InvalidData("memtable_size must be at least 1MB".into()));
        }

        if self.l0_compaction_threshold == 0 {
            return Err(FerroError::InvalidData(
                "l0_compaction_threshold must be greater than 0".into(),
            ));
        }

        if self.sstable_size < 512 * KIBI {
            return Err(FerroError::InvalidData("sstable_size must be at least 512KB".into()));
        }

        if self.sstable_size > self.memtable_size {
            return Err(FerroError::InvalidData(
                "sstable_size should not exceed memtable_size".into(),
            ));
        }

        Ok(())
    }
}

/// Builder for configuring and opening a `FerroKv` database
#[derive(Default)]
pub struct FerroKvBuilder {
    path: Option<PathBuf>,
    config: Config,
}

impl FerroKvBuilder {
    /// Set the database directory path
    pub(crate) fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Set the memtable flush threshold in bytes
    ///
    /// When the memtable reaches this size, it will be flushed to disk as an `SSTable`.
    /// Larger values reduce write amplification but increase memory usage.
    ///
    /// Default: 64MB
    /// Minimum: 1MB
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.config.memtable_size = size;
        self
    }

    /// Set the L0 compaction threshold
    ///
    /// When the number of L0 `SSTables` exceeds this threshold, compaction is triggered.
    /// Lower values improve read performance but increase write amplification.
    ///
    /// Default: 4 files
    /// Minimum: 1 file
    pub fn l0_compaction_threshold(mut self, threshold: usize) -> Self {
        self.config.l0_compaction_threshold = threshold;
        self
    }

    /// Set the maximum `SSTable` file size in bytes
    ///
    /// Individual `SSTable` files will not exceed this size during compaction.
    /// Smaller files enable faster compaction but increase file count.
    ///
    /// Default: 4MB
    /// Minimum: 512KB
    pub fn sstable_size(mut self, size: usize) -> Self {
        self.config.sstable_size = size;
        self
    }

    /// Open the database with the configured settings
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The path was not set
    /// - The configuration is invalid
    /// - The database directory cannot be created
    /// - WAL recovery fails
    pub async fn open(self) -> Result<FerroKv> {
        let path =
            self.path.ok_or_else(|| FerroError::InvalidData("Database path not set".into()))?;

        self.config.validate()?;

        FerroKv::open_with_config(path, self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.memtable_size, 64 * MEBI);
        assert_eq!(config.l0_compaction_threshold, 4);
        assert_eq!(config.sstable_size, 4 * MEBI);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_memtable_too_small() {
        let config = Config {
            memtable_size: 512 * KIBI, // Less than 1MB
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_zero_l0_threshold() {
        let config = Config { l0_compaction_threshold: 0, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_sstable_too_small() {
        let config = Config {
            sstable_size: 256 * KIBI, // Less than 512KB
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_sstable_exceeds_memtable() {
        let config = Config {
            memtable_size: 4 * MEBI,
            sstable_size: 8 * MEBI, // Larger than memtable
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_builder_defaults() {
        let builder = FerroKvBuilder::default();
        assert_eq!(builder.config.memtable_size, 64 * MEBI);
        assert_eq!(builder.config.l0_compaction_threshold, 4);
        assert_eq!(builder.config.sstable_size, 4 * MEBI);
    }

    #[test]
    fn test_builder_custom_values() {
        let builder = FerroKvBuilder::default()
            .memtable_size(128 * MEBI)
            .l0_compaction_threshold(8)
            .sstable_size(16 * MEBI);

        assert_eq!(builder.config.memtable_size, 128 * MEBI);
        assert_eq!(builder.config.l0_compaction_threshold, 8);
        assert_eq!(builder.config.sstable_size, 16 * MEBI);
    }
}
