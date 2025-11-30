use crc32fast::Hasher;

use crate::errors::{FerroError, Result};

/// Bloom filter for probabilistic set membership testing.
///
/// Provides O(1) "definitely not here" checks to avoid unnecessary disk reads.
pub struct BloomFilter {
    /// Bit array stored as bytes
    bits: Vec<u8>,
    /// Number of hash functions (k)
    num_hashes: u8,
    /// Number of bits in the filter (m)
    num_bits: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter optimized for the given parameters.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Minimum 1 item to avoid division by zero
        let n = expected_items.max(1) as f64;
        let p = false_positive_rate.clamp(0.0001, 0.5);

        // Calculate optimal number of bits (m)
        let ln2 = std::f64::consts::LN_2;
        let ln2_squared = ln2 * ln2;
        let num_bits = ((-n * p.ln()) / ln2_squared).ceil() as usize;
        let num_bits = num_bits.max(8); // Minimum 1 byte

        // Calculate optimal number of hash functions (k)
        let num_hashes = ((num_bits as f64 / n) * ln2).ceil() as u8;
        let num_hashes = num_hashes.clamp(1, 30);

        // Allocate bit array (round up to full bytes)
        let num_bytes = num_bits.div_ceil(8);
        let bits = vec![0u8; num_bytes];

        Self { bits, num_hashes, num_bits }
    }

    /// Insert a key into the Bloom filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hash_pair(key);

        for i in 0..u64::from(self.num_hashes) {
            let bit_index = self.get_bit_index(h1, h2, i);
            self.set_bit(bit_index);
        }
    }

    /// Check if a key may be in the set.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::hash_pair(key);

        for i in 0..u64::from(self.num_hashes) {
            let bit_index = self.get_bit_index(h1, h2, i);
            if !self.get_bit(bit_index) {
                return false;
            }
        }

        true
    }

    /// Serialize the Bloom filter for storage.
    ///
    /// Format: `[num_hashes(1B) | num_bits(4B) | bits...]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + self.bits.len());
        buf.push(self.num_hashes);
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Deserialize a Bloom filter from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 5 {
            return Err(FerroError::InvalidData("Bloom filter data too short".into()));
        }

        let num_hashes = data[0];
        let num_bits = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let expected_bytes = num_bits.div_ceil(8);

        if data.len() < 5 + expected_bytes {
            return Err(FerroError::InvalidData(
                format!(
                    "Bloom filter incomplete: expected {} bytes, got {}",
                    5 + expected_bytes,
                    data.len()
                )
                .into(),
            ));
        }

        let bits = data[5..5 + expected_bytes].to_vec();

        Ok(Self { bits, num_hashes, num_bits })
    }

    /// Compute two independent hash values using CRC32 with different seeds.
    ///
    /// Kirsch-Mitzenmacher: h(i) = h1 + i * h2
    fn hash_pair(key: &[u8]) -> (u64, u64) {
        // Hash 1: CRC32 of key directly
        let mut hasher1 = Hasher::new();
        hasher1.update(key);
        let h1 = u64::from(hasher1.finalize());

        // Hash 2: CRC32 of key with a different seed (append marker byte)
        let mut hasher2 = Hasher::new_with_initial(0xDEAD_BEEF);
        hasher2.update(key);
        let h2 = u64::from(hasher2.finalize());

        (h1, h2)
    }

    /// Calculate bit index using Kirsch-Mitzenmacher formula.
    fn get_bit_index(&self, h1: u64, h2: u64, i: u64) -> usize {
        let combined = h1.wrapping_add(i.wrapping_mul(h2));
        (combined % self.num_bits as u64) as usize
    }

    /// Set a bit in the bit array.
    fn set_bit(&mut self, bit_index: usize) {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        self.bits[byte_index] |= 1 << bit_offset;
    }

    /// Get a bit from the bit array.
    fn get_bit(&self, bit_index: usize) -> bool {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        (self.bits[byte_index] >> bit_offset) & 1 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_lookup() {
        let mut filter = BloomFilter::new(100, 0.01);

        // Insert keys
        filter.insert(b"key1");
        filter.insert(b"key2");
        filter.insert(b"key3");

        // Inserted keys must return true
        assert!(filter.may_contain(b"key1"));
        assert!(filter.may_contain(b"key2"));
        assert!(filter.may_contain(b"key3"));

        // Non-inserted keys should (usually) return false
        // Note: false positives are possible, but unlikely with low FPR
        let mut false_positives = 0;
        for i in 0..1000 {
            let key = format!("nonexistent_{i}");
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }
        // With 1% FPR and 1000 tests, expect ~10 false positives (allow some variance)
        assert!(false_positives < 50, "Too many false positives: {false_positives}");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut filter = BloomFilter::new(50, 0.01);

        filter.insert(b"apple");
        filter.insert(b"banana");
        filter.insert(b"cherry");

        // Serialize
        let serialized = filter.serialize();

        // Deserialize
        let restored = BloomFilter::deserialize(&serialized).unwrap();

        // Verify state preserved
        assert!(restored.may_contain(b"apple"));
        assert!(restored.may_contain(b"banana"));
        assert!(restored.may_contain(b"cherry"));
        assert_eq!(restored.num_hashes, filter.num_hashes);
        assert_eq!(restored.num_bits, filter.num_bits);
    }

    #[test]
    fn test_empty_filter() {
        let filter = BloomFilter::new(10, 0.01);

        // Empty filter should return false for all keys
        assert!(!filter.may_contain(b"anything"));
        assert!(!filter.may_contain(b"random"));
    }

    #[test]
    fn test_false_positive_rate() {
        let n = 10_000;
        let target_fpr = 0.01;
        let mut filter = BloomFilter::new(n, target_fpr);

        // Insert n keys
        for i in 0..n {
            let key = format!("inserted_{i}");
            filter.insert(key.as_bytes());
        }

        // Test n non-existent keys
        let mut false_positives = 0;
        for i in 0..n {
            let key = format!("not_inserted_{i}");
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fpr = f64::from(false_positives) / n as f64;

        // Allow 3x tolerance for statistical variance
        assert!(
            actual_fpr < target_fpr * 3.0,
            "FPR too high: {actual_fpr:.4} (expected ~{target_fpr})"
        );
    }

    #[test]
    fn test_deserialize_invalid_data() {
        // Too short
        let result = BloomFilter::deserialize(&[1, 2, 3]);
        assert!(result.is_err());

        // Incomplete bits
        let result = BloomFilter::deserialize(&[7, 64, 0, 0, 0]);
        assert!(result.is_err());
    }
}
