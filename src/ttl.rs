use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use tokio::sync::{Notify, RwLock};

/// Proactive TTL expiration tracker
#[derive(Default)]
pub struct ExpiryHeap {
    /// Min-heap ordered by `expire_at` timestamp
    heap: RwLock<BinaryHeap<ExpiryEntry>>,

    /// Wake background cleanup task when new entry added
    notifier: Notify,
}

/// Entry in expiry heap
#[derive(Eq, PartialEq)]
struct ExpiryEntry {
    expire_at: u64,
    key: Arc<[u8]>,
}

impl Ord for ExpiryEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (BinaryHeap is max-heap by default)
        other.expire_at.cmp(&self.expire_at).then_with(|| other.key.cmp(&self.key))
    }
}

impl PartialOrd for ExpiryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ExpiryHeap {
    /// Schedule key for expiration
    pub async fn schedule(&self, key: Arc<[u8]>, expire_at: u64) {
        let entry = ExpiryEntry { expire_at, key };
        self.heap.write().await.push(entry);
        self.notifier.notify_one();
    }

    /// Remove expiry (to be called when key updated/deleted before expiry)
    #[cfg(test)]
    pub async fn cancel(&self, key: &[u8]) {
        let mut heap = self.heap.write().await;

        // Rebuild heap without matching key
        let entries: Vec<_> = heap.drain().filter(|entry| entry.key.as_ref() != key).collect();

        for entry in entries {
            heap.push(entry);
        }
    }

    /// Get next expiry without removing (for background task sleep calculation)
    pub async fn peek_next(&self) -> Option<(u64, Arc<[u8]>)> {
        self.heap.read().await.peek().map(|entry| (entry.expire_at, Arc::clone(&entry.key)))
    }

    /// Remove and return expired entry if ready
    pub async fn pop_expired(&self, now: u64) -> Option<Arc<[u8]>> {
        let mut heap = self.heap.write().await;

        if let Some(entry) = heap.peek()
            && entry.expire_at <= now
        {
            return heap.pop().map(|e| e.key);
        }

        None
    }

    /// Get reference to notifier for background task
    pub fn notifier(&self) -> &Notify {
        &self.notifier
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_expiry_heap_schedule() {
        let heap = ExpiryHeap::default();
        let key: Arc<[u8]> = b"test_key".to_vec().into();
        let expire_at = 1000;

        heap.schedule(key.clone(), expire_at).await;

        // Verify entry scheduled
        let next = heap.peek_next().await;
        assert_eq!(next, Some((expire_at, key)));
    }

    #[tokio::test]
    async fn test_proactive_cold_data_cleanup() {
        let heap = ExpiryHeap::default();
        let key: Arc<[u8]> = b"cold_key".to_vec().into();
        let now = 1000;
        let expire_at = now + 10;

        heap.schedule(key.clone(), expire_at).await;

        // Before expiration - no deletion
        let popped = heap.pop_expired(now).await;
        assert_eq!(popped, None);

        // After expiration - proactive deletion triggered
        let popped = heap.pop_expired(expire_at + 1).await;
        assert_eq!(popped, Some(key));
    }

    #[tokio::test]
    async fn test_non_blocking_notification() {
        let heap: Arc<ExpiryHeap> = Arc::new(ExpiryHeap::default());
        let heap_clone = Arc::clone(&heap);

        // Spawn background listener
        let handle = tokio::spawn(async move {
            tokio::select! {
                () = heap_clone.notifier().notified() => {
                    true // Notified
                }
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    false // Timeout
                }
            }
        });

        // Schedule entry (should notify)
        let key: Arc<[u8]> = b"notify_key".to_vec().into();
        heap.schedule(key, 5000).await;

        // Verify notification received without blocking
        let notified = handle.await.unwrap();
        assert!(notified, "Background task must receive notification");
    }

    #[tokio::test]
    async fn test_expiry_cancellation() {
        let heap = ExpiryHeap::default();
        let key: Arc<[u8]> = b"cancel_key".to_vec().into();

        heap.schedule(key.clone(), 1000).await;
        heap.schedule(Arc::from(b"other_key".as_ref()), 2000).await;

        // Cancel first key
        heap.cancel(&key).await;

        // Verify only other_key remains
        let next: Option<(u64, Arc<[u8]>)> = heap.peek_next().await;
        let (expire, key) = next.unwrap();
        assert_eq!(expire, 2000);
        assert_eq!(key.as_ref(), b"other_key");
    }

    #[tokio::test]
    async fn test_min_heap_ordering() {
        let heap = ExpiryHeap::default();

        // Insert out of order
        heap.schedule(Arc::from(b"key3".as_ref()), 3000).await;
        heap.schedule(Arc::from(b"key1".as_ref()), 1000).await;
        heap.schedule(Arc::from(b"key2".as_ref()), 2000).await;

        // Earliest must be at top
        let next: Option<(u64, Arc<[u8]>)> = heap.peek_next().await;
        assert_eq!(next.unwrap().0, 1000);

        // Pop in order
        let k1: Option<Arc<[u8]>> = heap.pop_expired(1000).await;
        assert_eq!(k1.unwrap().as_ref(), b"key1");

        let k2: Option<Arc<[u8]>> = heap.pop_expired(2000).await;
        assert_eq!(k2.unwrap().as_ref(), b"key2");

        let k3: Option<Arc<[u8]>> = heap.pop_expired(3000).await;
        assert_eq!(k3.unwrap().as_ref(), b"key3");
    }

    #[tokio::test]
    async fn test_three_tier_strategy_integration() {
        let heap = ExpiryHeap::default();
        let now = 1000;

        // Simulate cold key (never accessed)
        let cold_key: Arc<[u8]> = b"cold_key".to_vec().into();
        heap.schedule(cold_key.clone(), now + 100).await;

        // should delete proactively
        let deleted: Option<Arc<[u8]>> = heap.pop_expired(now + 101).await;
        assert_eq!(deleted, Some(cold_key));

        // No entry left (proactive cleanup succeeded)
        assert_eq!(heap.peek_next().await, None);
    }
}
