use std::{
    fs,
    io::{self, Read},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use openplotva_updates::{DEFAULT_UPDATE_QUEUE_KEY, RedisUpdateQueue};
use redis::Client as RedisClient;

#[derive(Debug, Parser)]
#[command(about = "Inject Rust-native OpenPlotva Telegram updates into Redis")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Encode one carapax-compatible Telegram update JSON document and RPUSH it.
    Enqueue {
        /// Redis URL, for example redis://127.0.0.1:6379/0.
        #[arg(long)]
        redis_url: String,
        /// Redis update queue key.
        #[arg(long, default_value = DEFAULT_UPDATE_QUEUE_KEY)]
        key: String,
        /// JSON file to read; omit to read stdin.
        #[arg(long)]
        json_file: Option<PathBuf>,
        #[arg(long)]
        allowed_only: bool,
    },
    /// Print the current update queue length.
    Len {
        /// Redis URL, for example redis://127.0.0.1:6379/0.
        #[arg(long)]
        redis_url: String,
        /// Redis update queue key.
        #[arg(long, default_value = DEFAULT_UPDATE_QUEUE_KEY)]
        key: String,
    },
    /// Wait until the update queue reaches the expected length.
    WaitLen {
        /// Redis URL, for example redis://127.0.0.1:6379/0.
        #[arg(long)]
        redis_url: String,
        /// Redis update queue key.
        #[arg(long, default_value = DEFAULT_UPDATE_QUEUE_KEY)]
        key: String,
        /// Expected queue length.
        #[arg(long)]
        expected: i64,
        /// Timeout in seconds.
        #[arg(long, default_value_t = 120)]
        timeout_seconds: u64,
        /// Poll interval in milliseconds.
        #[arg(long, default_value_t = 500)]
        poll_millis: u64,
    },
    /// Count Redis keys matching a pattern using SCAN.
    ScanCount {
        /// Redis URL, for example redis://127.0.0.1:6379/0.
        #[arg(long)]
        redis_url: String,
        /// Redis SCAN MATCH pattern.
        #[arg(long)]
        pattern: String,
    },
    /// Wait until Redis SCAN MATCH returns at least N keys.
    WaitScanCount {
        /// Redis URL, for example redis://127.0.0.1:6379/0.
        #[arg(long)]
        redis_url: String,
        /// Redis SCAN MATCH pattern.
        #[arg(long)]
        pattern: String,
        /// Minimum matching key count.
        #[arg(long)]
        at_least: usize,
        /// Timeout in seconds.
        #[arg(long, default_value_t = 300)]
        timeout_seconds: u64,
        /// Poll interval in milliseconds.
        #[arg(long, default_value_t = 1000)]
        poll_millis: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Enqueue {
            redis_url,
            key,
            json_file,
            allowed_only,
        } => enqueue_update(&redis_url, &key, json_file, allowed_only).await,
        Command::Len { redis_url, key } => {
            let queue = queue(&redis_url, &key)?;
            println!("{}", queue.len().await?);
            Ok(())
        }
        Command::WaitLen {
            redis_url,
            key,
            expected,
            timeout_seconds,
            poll_millis,
        } => wait_len(&redis_url, &key, expected, timeout_seconds, poll_millis).await,
        Command::ScanCount { redis_url, pattern } => {
            println!("{}", scan_count(&redis_url, &pattern).await?);
            Ok(())
        }
        Command::WaitScanCount {
            redis_url,
            pattern,
            at_least,
            timeout_seconds,
            poll_millis,
        } => wait_scan_count(&redis_url, &pattern, at_least, timeout_seconds, poll_millis).await,
    }
}

async fn enqueue_update(
    redis_url: &str,
    key: &str,
    json_file: Option<PathBuf>,
    allowed_only: bool,
) -> anyhow::Result<()> {
    let body = read_json(json_file)?;
    let update = openplotva_updates::decode_telegram_update_json_slice(&body)
        .context("decode Telegram update JSON")?;
    let queue = queue(redis_url, key)?;
    let queued = if allowed_only {
        queue.enqueue_allowed_update(&update).await?
    } else {
        queue.enqueue_update(&update).await?;
        true
    };
    println!("{}", if queued { "queued" } else { "skipped" });
    Ok(())
}

fn read_json(json_file: Option<PathBuf>) -> anyhow::Result<Vec<u8>> {
    match json_file {
        Some(path) => fs::read(&path).with_context(|| format!("read {}", path.display())),
        None => {
            let mut body = Vec::new();
            io::stdin()
                .read_to_end(&mut body)
                .context("read update JSON from stdin")?;
            Ok(body)
        }
    }
}

async fn wait_len(
    redis_url: &str,
    key: &str,
    expected: i64,
    timeout_seconds: u64,
    poll_millis: u64,
) -> anyhow::Result<()> {
    let queue = queue(redis_url, key)?;
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let poll = Duration::from_millis(poll_millis.max(1));
    loop {
        let len = queue.len().await?;
        if len == expected {
            println!("{len}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("queue {key} length is {len}, expected {expected}");
        }
        tokio::time::sleep(poll).await;
    }
}

async fn wait_scan_count(
    redis_url: &str,
    pattern: &str,
    at_least: usize,
    timeout_seconds: u64,
    poll_millis: u64,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let poll = Duration::from_millis(poll_millis.max(1));
    loop {
        let count = scan_count(redis_url, pattern).await?;
        if count >= at_least {
            println!("{count}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("Redis pattern {pattern:?} matched {count}, expected at least {at_least}");
        }
        tokio::time::sleep(poll).await;
    }
}

async fn scan_count(redis_url: &str, pattern: &str) -> anyhow::Result<usize> {
    let client = RedisClient::open(redis_url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let mut cursor = 0_u64;
    let mut total = 0_usize;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut connection)
            .await?;
        total += keys.len();
        cursor = next;
        if cursor == 0 {
            return Ok(total);
        }
    }
}

fn queue(redis_url: &str, key: &str) -> anyhow::Result<RedisUpdateQueue> {
    Ok(RedisUpdateQueue::with_key(
        RedisClient::open(redis_url)?,
        key,
    ))
}
