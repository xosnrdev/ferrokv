use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use clap_cargo::style::CLAP_STYLING;
use comfy_table::presets::ASCII_MARKDOWN;
use comfy_table::{Cell, Color, Table};
use ferrokv::{Builder, FerroKv};

#[derive(Parser)]
#[command(about, version, styles = CLAP_STYLING)]
struct Cli {
    /// Path to the database directory
    #[arg(long, default_value = "./ferrokv", value_name = "PATH")]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan key-value pairs within a range
    Scan {
        /// Inclusive start key
        #[arg(long, value_name = "KEY")]
        start: Option<String>,

        /// Exclusive end key
        #[arg(long, value_name = "KEY")]
        end: Option<String>,

        /// Maximum number of pairs to return
        #[arg(short, long, default_value = "100", value_name = "N")]
        limit: usize,

        /// Show count of matching keys
        #[arg(long, conflicts_with = "limit")]
        count: bool,

        /// Show only keys excluding values
        #[arg(long, conflicts_with = "count")]
        keys_only: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let db = Builder::new()
        .path(&cli.db)
        .read_only()
        .build()
        .await
        .with_context(|| format!("Failed to open database at `{}`", cli.db.display()))?;

    match cli.command {
        Commands::Scan { start, end, limit, count, keys_only } => {
            handle_scan(&db, start, end, limit, count, keys_only).await
        }
    }
}

async fn handle_scan(
    db: &FerroKv,
    start: Option<String>,
    end: Option<String>,
    limit: usize,
    count: bool,
    keys_only: bool,
) -> Result<()> {
    let pairs = match (&start, &end) {
        (None, None) => db.scan(..).await,
        (Some(s), None) => db.scan(s.as_bytes()..).await,
        (None, Some(e)) => db.scan(..e.as_bytes()).await,
        (Some(s), Some(e)) => db.scan(s.as_bytes()..e.as_bytes()).await,
    }
    .context("Failed to execute scan operation")?;

    if count {
        println!("{}", pairs.len());
        return Ok(());
    }

    let limited_pairs: Vec<_> = pairs.into_iter().take(limit).collect();
    if limited_pairs.is_empty() {
        println!("No keys found in the specified range.");
        return Ok(());
    }

    display_pairs(keys_only, &limited_pairs);

    Ok(())
}

type Pairs<'a> = &'a [(Arc<[u8]>, Arc<[u8]>)];

fn display_pairs(keys_only: bool, pairs: Pairs) {
    let mut table = Table::new();
    table.load_preset(ASCII_MARKDOWN);

    if keys_only {
        table.set_header(vec![Cell::new("Key").fg(Color::Green)]);

        for (key, _) in pairs {
            table.add_row(vec![format_bytes(key)]);
        }
    } else {
        table.set_header(vec![
            Cell::new("Key").fg(Color::Green),
            Cell::new("Value").fg(Color::Cyan),
        ]);

        for (key, value) in pairs {
            table.add_row(vec![format_bytes(key), format_bytes(value)]);
        }
    }

    println!("{table}");
}

fn format_bytes(v: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(v)
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert();
}
