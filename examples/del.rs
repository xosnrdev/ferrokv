use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    let db = FerroKv::open(&db_dir).await?;

    db.set(b"foo", b"bar").await?;

    if let Some(value) = db.get(b"foo").await? {
        println!("foo = {}", String::from_utf8_lossy(&value));
    }

    // Delete key
    db.del(b"foo").await?;

    // Try to read deleted key
    let value = db.get(b"foo").await?;
    println!("foo = {}", String::from_utf8_lossy(&value.unwrap_or_default()));

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
