use std::path::PathBuf;

use crate::errors::{FerroError, Result};
use crate::helpers::{DB_PATH, KIBI, MEBI};
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

/// Builder for configuring a `FerroKv` database.
///
/// # Examples
///
///```no_run
/// use ferrokv::Builder;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let db = Builder::new()
///         .path("./data")
///         .memtable_size(128 * 1024 * 1024)
///         .l0_compaction_threshold(8)
///         .sstable_size(8 * 1024 * 1024)
///         .build()
///         .await?;
///
///     Ok(())
/// }
/// ```
pub struct Builder {
    path: Option<PathBuf>,
    config: Config,
}

impl Default for Builder {
    fn default() -> Self {
        Self { path: Some(DB_PATH.into()), config: Config::default() }
    }
}

impl Builder {
    /// Creates a new [`Builder`] instance with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the database directory path.
    #[must_use]
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the memtable flush threshold in bytes.
    ///
    /// When the memtable reaches this size, it will be flushed to disk as an `SSTable`.
    /// Larger values reduce write amplification but increase memory usage.
    ///
    /// **NOTE**: The least minimum value is 1MB and default is 64MB.
    #[must_use]
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.config.memtable_size = size;
        self
    }

    /// Set the L0 compaction threshold.
    ///
    /// When the number of L0 `SSTables` exceeds this threshold, compaction is triggered.
    /// Lower values improve read performance but increase write amplification.
    ///
    /// **NOTE**: The least minimum value is 1 file and default is 4 files.
    #[must_use]
    pub fn l0_compaction_threshold(mut self, threshold: usize) -> Self {
        self.config.l0_compaction_threshold = threshold;
        self
    }

    /// Set the maximum `SSTable` file size in bytes.
    ///
    /// Individual `SSTable` files will not exceed this size during compaction.
    /// Smaller files enable faster compaction but increase file count.
    ///
    /// **NOTE**: The least minimum value is 512KB and default is 4MB.
    #[must_use]
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
    #[deprecated(since = "1.0.2", note = "Use `build()` method instead")]
    pub async fn open(self) -> Result<FerroKv> {
        let path =
            self.path.ok_or_else(|| FerroError::InvalidData("Database path not set".into()))?;

        self.config.validate()?;

        FerroKv::with_config(path, self.config).await
    }

    /// Open the database with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The path was not set
    /// - The configuration is invalid
    /// - The database directory cannot be created
    /// - WAL recovery fails
    pub async fn build(self) -> Result<FerroKv> {
        let path =
            self.path.ok_or_else(|| FerroError::InvalidData("Database path not set".into()))?;

        self.config.validate()?;

        FerroKv::with_config(path, self.config).await
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
        let builder = Builder::default();
        assert_eq!(builder.config.memtable_size, 64 * MEBI);
        assert_eq!(builder.config.l0_compaction_threshold, 4);
        assert_eq!(builder.config.sstable_size, 4 * MEBI);
    }

    #[test]
    fn test_builder_custom_values() {
        let builder = Builder::default()
            .memtable_size(128 * MEBI)
            .l0_compaction_threshold(8)
            .sstable_size(16 * MEBI);

        assert_eq!(builder.config.memtable_size, 128 * MEBI);
        assert_eq!(builder.config.l0_compaction_threshold, 8);
        assert_eq!(builder.config.sstable_size, 16 * MEBI);
    }
}
