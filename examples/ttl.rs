use std::time::Duration;

use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    // Open database with default configuration
    let db = FerroKv::open(&db_dir).await?;

    // Write with TTL
    db.set_ex(b"foo", b"bar", Duration::from_secs(5)).await?;

    if let Some(value) = db.get(b"foo").await? {
        println!("Before expiry: foo = {}", String::from_utf8_lossy(&value));
    }

    // Wait for expiration
    let duration = Duration::from_secs(6);
    println!("Expiring in {} seconds...", duration.as_secs());
    tokio::time::sleep(duration).await;

    let value = db.get(b"foo").await?;
    println!("After expiry: foo = {value:?}");

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
