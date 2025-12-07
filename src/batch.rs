use std::time::Duration;

use crate::helpers::get_now;

/// A batch of write operations to be executed atomically with a single fsync.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
///
/// use ferrokv::{FerroKv, WriteBatch};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let db = FerroKv::with_path("./data").await?;
///     let mut batch = WriteBatch::new();
///
///     batch
///         .set(b"foo:1", b"bar")
///         .set(b"foo:2", b"baz")
///         .set_ex(b"foo:3", b"qux", Duration::from_secs(3600))
///         .del(b"foo:2");
///
///     // Execute all operations with a single fsync
///     db.write_batch(batch).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct WriteBatch {
    pub(crate) entries: Vec<BatchEntry>,
}

/// Internal representation of a batch operation.
pub(crate) enum BatchEntry {
    /// Set a key-value pair with optional TTL
    Set { key: Vec<u8>, value: Vec<u8>, ttl: Option<u64> },
    /// Delete a key (tombstone)
    Del { key: Vec<u8> },
}

impl WriteBatch {
    /// Create a new empty write batch.
    #[must_use]
    pub const fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Create a new write batch with pre-allocated capacity.
    ///
    /// Use this when you know approximately how many operations will be added.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self { entries: Vec::with_capacity(capacity) }
    }

    /// Add a SET operation to the batch.
    ///
    /// The key-value pair will be written when the batch is executed.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        #[cfg(debug_assertions)]
        println!("SET {} {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
        self.entries.push(BatchEntry::Set { key: key.to_vec(), value: value.to_vec(), ttl: None });
        self
    }

    /// Add a SET operation with TTL to the batch.
    ///
    /// The key-value pair will expire after the specified duration.
    pub fn set_ex(&mut self, key: &[u8], value: &[u8], ttl: Duration) -> &mut Self {
        let ttl = ttl.as_secs();

        #[cfg(debug_assertions)]
        println!(
            "SETEX {} {} {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
            ttl
        );

        let expire_at = get_now() + ttl;
        self.entries.push(BatchEntry::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            ttl: Some(expire_at),
        });
        self
    }

    /// Add a DELETE operation to the batch.
    ///
    /// A tombstone marker will be written for this key.
    pub fn del(&mut self, key: &[u8]) -> &mut Self {
        #[cfg(debug_assertions)]
        println!("DEL {}", String::from_utf8_lossy(key));

        self.entries.push(BatchEntry::Del { key: key.to_vec() });
        self
    }

    /// Returns the number of operations in the batch.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the batch contains no operations.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all operations from the batch.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_builder() {
        let mut batch = WriteBatch::new();

        batch.set(b"key1", b"value1").set(b"key2", b"value2").del(b"key3");

        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_batch_with_capacity() {
        let batch = WriteBatch::with_capacity(100);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_clear() {
        let mut batch = WriteBatch::new();
        batch.set(b"key1", b"value1");
        assert_eq!(batch.len(), 1);

        batch.clear();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_set_ex() {
        let mut batch = WriteBatch::new();
        batch.set_ex(b"key1", b"value1", Duration::from_secs(3600));

        assert_eq!(batch.len(), 1);
        match &batch.entries[0] {
            BatchEntry::Set { ttl, .. } => {
                assert!(ttl.is_some());
            }
            BatchEntry::Del { .. } => panic!("Expected Set entry"),
        }
    }
}
