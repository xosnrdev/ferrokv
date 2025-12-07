use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    let db = FerroKv::with_path(&db_dir).await?;

    let count = db.incr(b"foo").await?;
    println!("foo = {count}");

    let count = db.incr(b"foo").await?;
    println!("foo = {count}");

    let count = db.incr(b"foo").await?;
    println!("foo = {count}");

    println!("\nRestarting database...");
    // Drop DB to simulate restart
    drop(db);

    let db = FerroKv::with_path(&db_dir).await?;
    let count = db.incr(b"foo").await?;
    println!("foo = {count}");

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
