use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    // Open database with custom configuration
    let db = FerroKv::builder(&db_dir)
        .memtable_size(128 * 1024 * 1024)
        .l0_compaction_threshold(8)
        .sstable_size(8 * 1024 * 1024)
        .open()
        .await?;

    db.set(b"foo", b"bar").await?;

    if let Some(value) = db.get(b"foo").await? {
        println!("foo = {}", String::from_utf8_lossy(&value));
    }

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
