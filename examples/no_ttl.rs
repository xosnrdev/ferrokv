use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    let db = FerroKv::open(&db_dir).await?;

    // Write KVs
    db.set(b"foo", b"bar").await?;

    if let Some(value) = db.get(b"foo").await? {
        println!("foo = {}", String::from_utf8_lossy(&value));
    }

    println!("\nRestarting database...");
    drop(db);

    // Read persisted data
    let db = FerroKv::open(&db_dir).await?;
    if let Some(value) = db.get(b"foo").await? {
        println!("foo = {}", String::from_utf8_lossy(&value));
    }

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
