use ferrokv::FerroKv;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: Using a temporary directory for demonstration.
    let db_dir = tempdir()?.keep();

    let db = FerroKv::open(&db_dir).await?;

    db.set(b"foo:1", b"bar").await?;
    db.set(b"foo:2", b"baz").await?;
    db.set(b"foo:3", b"qux").await?;

    // Scan all keys
    db.scan(..).await?.iter().for_each(|(key, value)| {
        println!("{} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    });

    // Scan range of keys
    db.scan(&b"foo:1"[..]..=&b"foo:2"[..]).await?.iter().for_each(|(key, value)| {
        println!("{} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    });

    // Cleanup
    drop(db);
    let _ = tokio::fs::remove_dir_all(&db_dir).await;

    Ok(())
}
