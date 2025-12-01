use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    println!("Opening database at {}", db_dir.display());
    // Open database with default configuration
    let db = FerroKv::open(&db_dir).await?;

    // Write some keys with different prefixes
    println!("Writing keys...");
    db.set(b"user:100", b"alice").await?;
    db.set(b"user:200", b"bob").await?;
    db.set(b"user:300", b"charlie").await?;
    db.set(b"session:100", b"active").await?;
    db.set(b"session:200", b"expired").await?;
    db.set(b"config:theme", b"dark").await?;

    // Scan all keys
    println!("\nScanning all keys:");
    let results = db.scan(..).await?;
    for (key, value) in &results {
        println!("  {} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }

    // Scan with prefix (user:)
    println!("\nScanning keys with prefix 'user:':");
    let results = db.scan(&b"user:"[..]..&b"user:\xff"[..]).await?;
    for (key, value) in &results {
        println!("  {} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }

    // Scan with bounded range
    println!("\nScanning keys in range [user:100, user:200]:");
    let results = db.scan(&b"user:100"[..]..=&b"user:200"[..]).await?;
    for (key, value) in &results {
        println!("  {} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
