use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    println!("Opening database at {}", db_dir.display());
    // Open database with default configuration
    let db = FerroKv::open(db_dir.clone()).await?;

    // Write keys
    println!("Writing keys...");
    db.set(b"user:1", b"alice").await?;
    db.set(b"user:2", b"bob").await?;
    db.set(b"user:3", b"charlie").await?;

    // Read keys
    println!("\nReading keys before deletion...");
    let value = db.get(b"user:1").await?;
    println!("user:1 = {}", String::from_utf8_lossy(&value.unwrap()));
    let value = db.get(b"user:2").await?;
    println!("user:2 = {}", String::from_utf8_lossy(&value.unwrap()));
    let value = db.get(b"user:3").await?;
    println!("user:3 = {}", String::from_utf8_lossy(&value.unwrap()));

    // Delete a key
    println!("\nDeleting user:2...");
    let deleted = db.del(b"user:2").await?;
    println!("Deletion successful: {deleted}");

    // Try to read deleted key
    println!("\nReading keys after deletion...");
    let value = db.get(b"user:1").await?;
    println!("user:1 = {}", String::from_utf8_lossy(&value.unwrap()));
    let value = db.get(b"user:2").await?;
    println!("user:2 = {value:?}");
    let value = db.get(b"user:3").await?;
    println!("user:3 = {}", String::from_utf8_lossy(&value.unwrap()));

    // Delete non-existent key
    println!("\nDeleting non-existent key...");
    let deleted = db.del(b"user:99").await?;
    println!("Deletion successful: {deleted}");

    // Idempotent deletion
    println!("\nDeleting user:2 again (idempotent)...");
    let deleted = db.del(b"user:2").await?;
    println!("Deletion successful: {deleted}");

    println!("\nRestarting database...");
    // Drop DB to simulate restart
    drop(db);

    // Verify deletion persists across restarts
    let db = FerroKv::open(db_dir.clone()).await?;
    println!("After restart:");
    let value = db.get(b"user:1").await?;
    println!("user:1 = {}", String::from_utf8_lossy(&value.unwrap()));
    let value = db.get(b"user:2").await?;
    println!("user:2 = {value:?} (still deleted)");
    let value = db.get(b"user:3").await?;
    println!("user:3 = {}", String::from_utf8_lossy(&value.unwrap()));

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
