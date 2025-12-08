use std::sync::Arc;
use std::time::Duration;

use ferrokv::{FerroError, FerroKv, WriteBatch};
use tempfile::tempdir;

#[tokio::test]
async fn test_crash_recovery() {
    let db_dir = tempdir().unwrap().keep();

    {
        let db = FerroKv::with_path(&db_dir).await.unwrap();
        db.set(b"key1", b"value1").await.unwrap();
        db.set(b"key2", b"value2").await.unwrap();
    } // Drop simulates crash

    let db2 = FerroKv::with_path(&db_dir).await.unwrap();
    assert_eq!(db2.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));
    assert_eq!(db2.get(b"key2").await.unwrap().as_deref(), Some(&b"value2"[..]));

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_ttl_expiration() {
    let db_dir = tempdir().unwrap().keep();

    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // Set TTL to 1 second and wait 2 seconds to ensure expiration
    db.set_ex(b"key1", b"value1", Duration::from_secs(1)).await.unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(db.get(b"key1").await.unwrap(), None);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_concurrent_operations() {
    let db_dir = tempdir().unwrap().keep();

    let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());
    let mut handles = Vec::new();

    for i in 0..10 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            let key = format!("key{i}");
            let value = format!("value{i}");
            db.set(key.as_bytes(), value.as_bytes()).await.unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    for i in 0..10 {
        let key = format!("key{i}");
        let expected = format!("value{i}");
        let result = db.get(key.as_bytes()).await.unwrap();
        assert_eq!(result.as_deref(), Some(expected.as_bytes()));
    }

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_incr() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // Increment non-existent key (should start at 0)
    let val = db.incr(b"counter").await.unwrap();
    assert_eq!(val, 1);

    // Increment again
    let val = db.incr(b"counter").await.unwrap();
    assert_eq!(val, 2);

    // Increment multiple times
    for i in 3..=10 {
        let val = db.incr(b"counter").await.unwrap();
        assert_eq!(val, i);
    }

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_incr_durability() {
    let db_dir = tempdir().unwrap().keep();

    {
        let db = FerroKv::with_path(&db_dir).await.unwrap();
        db.incr(b"counter").await.unwrap();
        db.incr(b"counter").await.unwrap();
        db.incr(b"counter").await.unwrap();
    } // Drop (simulates crash)

    // Reopen and verify counter persisted
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // Read as string to verify value
    let val = db.get(b"counter").await.unwrap().unwrap();
    let val_str = std::str::from_utf8(&val).unwrap();
    assert_eq!(val_str, "3");

    // Continue incrementing
    let val = db.incr(b"counter").await.unwrap();
    assert_eq!(val, 4);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_incr_concurrent() {
    let db_dir = tempdir().unwrap().keep();

    let db = Arc::new(FerroKv::with_path(&db_dir).await.unwrap());

    // Spawn 10 concurrent incrementers
    let mut handles = Vec::new();
    for _ in 0..10 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                db.incr(b"shared_counter").await.unwrap();
            }
        }));
    }

    // Wait for all increments
    for handle in handles {
        handle.await.unwrap();
    }

    // Final value should be 100 (10 tasks × 10 increments)
    let val = db.get(b"shared_counter").await.unwrap().unwrap();
    let val_str = std::str::from_utf8(&val).unwrap();
    assert_eq!(val_str, "100", "All increments should be atomic");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_incr_invalid_value() {
    let db_dir = tempdir().unwrap().keep();

    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // Set non-numeric value
    db.set(b"bad_counter", b"not_a_number").await.unwrap();

    // Increment should default to 0 and return 1
    let val = db.incr(b"bad_counter").await.unwrap();
    assert_eq!(val, 1);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_del_durability() {
    let db_dir = tempdir().unwrap().keep();

    {
        let db = FerroKv::with_path(&db_dir).await.unwrap();
        db.set(b"key1", b"value1").await.unwrap();
        db.del(b"key1").await.unwrap();
    } // Drop simulates crash

    // Reopen and verify deletion persisted via WAL
    let db = FerroKv::with_path(&db_dir).await.unwrap();
    assert_eq!(db.get(b"key1").await.unwrap(), None, "Deletion should survive restart");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_del_idempotent() {
    let db_dir = tempdir().unwrap().keep();

    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key1", b"value1").await.unwrap();

    // First delete
    let existed1 = db.del(b"key1").await.unwrap();
    assert!(existed1, "First delete should return true");

    // Second delete (idempotent)
    let existed2 = db.del(b"key1").await.unwrap();
    assert!(!existed2, "Second delete should return false");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_del_set_del_sequence() {
    let db_dir = tempdir().unwrap().keep();

    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // SET -> DEL -> SET sequence
    db.set(b"key1", b"value1").await.unwrap();
    assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));

    db.del(b"key1").await.unwrap();
    assert_eq!(db.get(b"key1").await.unwrap(), None);

    db.set(b"key1", b"value2").await.unwrap();
    assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value2"[..]));

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_scan_range() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // Insert keys in sorted order
    db.set(b"key_a", b"value_a").await.unwrap();
    db.set(b"key_b", b"value_b").await.unwrap();
    db.set(b"key_c", b"value_c").await.unwrap();
    db.set(b"key_d", b"value_d").await.unwrap();
    db.set(b"key_e", b"value_e").await.unwrap();

    // Scan range [key_b, key_d]
    let results = db.scan(&b"key_b"[..]..=&b"key_d"[..]).await.unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0.as_ref(), b"key_b");
    assert_eq!(results[1].0.as_ref(), b"key_c");
    assert_eq!(results[2].0.as_ref(), b"key_d");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_scan_unbounded() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"a", b"1").await.unwrap();
    db.set(b"b", b"2").await.unwrap();
    db.set(b"c", b"3").await.unwrap();

    // Scan all keys (unbounded range)
    let results = db.scan(..).await.unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0.as_ref(), b"a");
    assert_eq!(results[1].0.as_ref(), b"b");
    assert_eq!(results[2].0.as_ref(), b"c");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_scan_empty_range() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key_a", b"value_a").await.unwrap();
    db.set(b"key_z", b"value_z").await.unwrap();

    // Scan range that has no keys
    let results = db.scan(&b"key_m"[..]..&b"key_n"[..]).await.unwrap();

    assert_eq!(results.len(), 0);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_scan_tombstone_filtering() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key_a", b"value_a").await.unwrap();
    db.set(b"key_b", b"value_b").await.unwrap();
    db.set(b"key_c", b"value_c").await.unwrap();

    // Delete key_b
    db.del(b"key_b").await.unwrap();

    // Scan should not include deleted key
    let results = db.scan(..).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"key_a");
    assert_eq!(results[1].0.as_ref(), b"key_c");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_scan_ttl_expiration() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key_a", b"value_a").await.unwrap();
    db.set_ex(b"key_b", b"value_b", Duration::from_millis(100)).await.unwrap();
    db.set(b"key_c", b"value_c").await.unwrap();

    // Wait for TTL expiration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Scan should not include expired key
    let results = db.scan(..).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"key_a");
    assert_eq!(results[1].0.as_ref(), b"key_c");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_scan_prefix() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    // Insert keys with different prefixes
    db.set(b"user:100", b"alice").await.unwrap();
    db.set(b"user:200", b"bob").await.unwrap();
    db.set(b"user:300", b"charlie").await.unwrap();
    db.set(b"session:100", b"active").await.unwrap();
    db.set(b"session:200", b"expired").await.unwrap();

    // Scan only user: prefix
    let results = db.scan(&b"user:"[..]..&b"user:\xff"[..]).await.unwrap();

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|(k, _)| k.starts_with(b"user:")));

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_write_batch() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    let mut batch = WriteBatch::new();
    batch.set(b"key1", b"value1").set(b"key2", b"value2").set(b"key3", b"value3");

    db.write_batch(batch).await.unwrap();

    assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));
    assert_eq!(db.get(b"key2").await.unwrap().as_deref(), Some(&b"value2"[..]));
    assert_eq!(db.get(b"key3").await.unwrap().as_deref(), Some(&b"value3"[..]));

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_write_batch_with_delete() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"to_delete", b"initial").await.unwrap();

    let mut batch = WriteBatch::new();
    batch.set(b"key1", b"value1").del(b"to_delete").set(b"key2", b"value2");

    db.write_batch(batch).await.unwrap();

    assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));
    assert_eq!(db.get(b"key2").await.unwrap().as_deref(), Some(&b"value2"[..]));
    assert_eq!(db.get(b"to_delete").await.unwrap(), None);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_write_batch_with_ttl() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    let mut batch = WriteBatch::new();
    batch.set(b"permanent", b"stays").set_ex(b"expires", b"goes", Duration::from_millis(100));

    db.write_batch(batch).await.unwrap();

    assert_eq!(db.get(b"permanent").await.unwrap().as_deref(), Some(&b"stays"[..]));
    assert_eq!(db.get(b"expires").await.unwrap().as_deref(), Some(&b"goes"[..]));

    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(db.get(b"permanent").await.unwrap().as_deref(), Some(&b"stays"[..]));
    assert_eq!(db.get(b"expires").await.unwrap(), None);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_write_batch_durability() {
    let db_dir = tempdir().unwrap().keep();

    {
        let db = FerroKv::with_path(&db_dir).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.set(b"key1", b"value1").set(b"key2", b"value2").del(b"key3");

        db.write_batch(batch).await.unwrap();
    } // Drop simulates crash

    let db = FerroKv::with_path(&db_dir).await.unwrap();
    assert_eq!(db.get(b"key1").await.unwrap().as_deref(), Some(&b"value1"[..]));
    assert_eq!(db.get(b"key2").await.unwrap().as_deref(), Some(&b"value2"[..]));

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_write_batch_scan_integration() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key1", b"value1").await.unwrap();
    db.set(b"key2", b"value2").await.unwrap();
    db.set(b"key3", b"value3").await.unwrap();

    let keys = db.keys().await.unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.iter().any(|k| k.as_ref() == b"key1"));
    assert!(keys.iter().any(|k| k.as_ref() == b"key2"));
    assert!(keys.iter().any(|k| k.as_ref() == b"key3"));

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_keys_excludes_tombstones() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key1", b"value1").await.unwrap();
    db.set(b"key2", b"value2").await.unwrap();
    db.del(b"key1").await.unwrap();

    let keys = db.keys().await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].as_ref(), b"key2");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_keys_excludes_expired() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    db.set(b"key1", b"value1").await.unwrap();
    db.set_ex(b"key2", b"value2", Duration::from_millis(100)).await.unwrap();

    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(150)).await;

    let keys = db.keys().await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].as_ref(), b"key1");

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_len_accurate_count() {
    let db_dir = tempdir().unwrap().keep();
    let db = FerroKv::with_path(&db_dir).await.unwrap();

    assert_eq!(db.len().await.unwrap(), 0);
    assert!(db.is_empty().await.unwrap());

    db.set(b"k1", b"v").await.unwrap();
    assert_eq!(db.len().await.unwrap(), 1);
    assert!(!db.is_empty().await.unwrap());

    db.set(b"k2", b"v").await.unwrap();
    assert_eq!(db.len().await.unwrap(), 2);

    db.del(b"k1").await.unwrap();
    assert_eq!(db.len().await.unwrap(), 1);

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}

#[tokio::test]
async fn test_exclusive_lock_prevents_multi_process() {
    let db_dir = tempdir().unwrap().keep();

    // First process opens database successfully
    let db1 = FerroKv::with_path(&db_dir).await.unwrap();

    // Second process attempt should fail with Locked error
    let db2 = FerroKv::with_path(&db_dir).await;
    assert!(db2.is_err_and(|err| matches!(err, FerroError::Locked(_))));

    // After first process closes, second can open
    drop(db1);
    let db3 = FerroKv::with_path(&db_dir).await;
    assert!(db3.is_ok());

    let _ = tokio::fs::remove_dir_all(&db_dir).await;
}
