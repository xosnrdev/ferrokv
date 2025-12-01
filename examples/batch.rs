use std::time::Duration;

use ferrokv::{FerroKv, WriteBatch};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    let db = FerroKv::open(&db_dir).await?;

    let mut batch = WriteBatch::new();
    batch
        .set(b"foo:1", b"bar")
        .set(b"foo:2", b"baz")
        .set_ex(b"foo:3", b"qux", Duration::from_secs(3600))
        .del(b"foo:2");

    // Single fsync for all operations
    db.write_batch(batch).await?;

    // Verify batch operations
    db.scan(..).await?.iter().for_each(|(k, v)| {
        println!("{} = {}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
    });

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
