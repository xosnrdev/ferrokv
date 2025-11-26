use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    println!("Opening database at {}", db_dir.display());
    // Open database with default configuration
    let db = FerroKv::open(db_dir.clone()).await?;

    // Write KVs
    println!("Writing keys...");
    db.set(b"user:1", b"alice").await?;
    db.set(b"user:2", b"bob").await?;

    // Reading Keys
    println!("Reading keys...");
    let value = db.get(b"user:1").await?;
    println!("user:1 = {}", String::from_utf8_lossy(&value.unwrap()));
    let value = db.get(b"user:2").await?;
    println!("user:2 = {}", String::from_utf8_lossy(&value.unwrap()));

    println!("\nRestarting database...");
    // Drop DB to simulate restart
    drop(db);

    // Read persisted data
    let db = FerroKv::open(db_dir.clone()).await?;
    let value = db.get(b"user:1").await?;
    println!("persisted user:1 = {}", String::from_utf8_lossy(&value.unwrap()));
    let value = db.get(b"user:2").await?;
    println!("persisted user:2 = {}", String::from_utf8_lossy(&value.unwrap()));

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
