use std::time::Duration;

use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    println!("Opening database at {}", db_dir.display());
    // Open database with default configuration
    let db = FerroKv::open(db_dir.clone()).await?;

    // Write with TTL
    println!("Writing key with 5 seconds TTL...");
    db.set_ex(b"foo", b"bar", Duration::from_secs(5)).await?;

    let value = db.get(b"foo").await?;
    println!("Before expiry: foo = {}", String::from_utf8_lossy(&value.unwrap()));

    // Wait for expiration
    println!("Waiting for key to expire...");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let value = db.get(b"foo").await?;
    println!("After expiry: foo = {value:?}");

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
