use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    println!("Opening database at {}", db_dir.display());
    // Open database with custom configuration
    let db = FerroKv::builder(db_dir.clone())
        .memtable_size(128 * 1024 * 1024)
        .l0_compaction_threshold(8)
        .sstable_size(8 * 1024 * 1024)
        .open()
        .await?;

    db.set(b"foo", b"bar").await?;

    let value = db.get(b"foo").await?;
    println!("foo = {}", String::from_utf8_lossy(&value.unwrap()));

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
