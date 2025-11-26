use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    println!("Opening database at {}", db_dir.display());
    // Open database with default configuration
    let db = FerroKv::open(db_dir.clone()).await?;

    // Atomic increment operations
    println!("Incrementing counter...");
    let count = db.incr(b"visits").await?;
    println!("visits = {count}");

    let count = db.incr(b"visits").await?;
    println!("visits = {count}");

    let count = db.incr(b"visits").await?;
    println!("visits = {count}");

    println!("\nRestarting database...");
    // Drop DB to simulate restart
    drop(db);

    // Counter persists across restarts
    let db = FerroKv::open(db_dir.clone()).await?;
    let count = db.incr(b"visits").await?;
    println!("persisted visits = {count}");

    // Multiple counters
    println!("\nUsing multiple counters...");
    let page_views = db.incr(b"stats:page_views").await?;
    println!("stats:page_views = {page_views}");

    let api_calls = db.incr(b"stats:api_calls").await?;
    println!("stats:api_calls = {api_calls}");

    let page_views = db.incr(b"stats:page_views").await?;
    println!("stats:page_views = {page_views}");

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
