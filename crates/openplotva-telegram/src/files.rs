//! Bounded Bot API file lookups and downloads.
//!
//! The shared HTTP client keeps a long request timeout for the `getUpdates`
//! long poll. File work runs inside queue workers that hold a slot while they
//! wait, so it gets its own short bounds here.

use std::time::Duration;

use carapax::{
    api::{DownloadFileError, ExecuteError},
    types::{File, GetFile},
};
use futures_util::StreamExt;
use thiserror::Error;

use crate::TelegramClient;

/// Bound for one `getFile` call, including the client's own retries.
pub const TELEGRAM_GET_FILE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound for one file download, from the request to the last body chunk.
pub const TELEGRAM_FILE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Failure of a bounded Bot API file lookup or download.
#[derive(Debug, Error)]
pub enum TelegramFileError {
    #[error("get telegram file: timed out after {}s", .0.as_secs())]
    GetFileTimedOut(Duration),
    #[error("get telegram file: {0}")]
    GetFile(#[source] ExecuteError),
    #[error("telegram file_path is empty")]
    MissingFilePath,
    #[error("download telegram file: timed out after {}s", .0.as_secs())]
    DownloadTimedOut(Duration),
    #[error("download telegram file: {0}")]
    Download(#[source] DownloadFileError),
    #[error("download telegram file chunk: {0}")]
    DownloadChunk(#[source] reqwest::Error),
    #[error("telegram file exceeds {limit} bytes")]
    TooLarge { received: usize, limit: usize },
}

/// Run `getFile` within [`TELEGRAM_GET_FILE_TIMEOUT`].
pub async fn get_telegram_file(
    client: &TelegramClient,
    file_id: &str,
) -> Result<File, TelegramFileError> {
    get_telegram_file_within(client, file_id, TELEGRAM_GET_FILE_TIMEOUT).await
}

/// Run `getFile` within [`TELEGRAM_GET_FILE_TIMEOUT`] and return the download path.
pub async fn get_telegram_file_path(
    client: &TelegramClient,
    file_id: &str,
) -> Result<String, TelegramFileError> {
    let file = get_telegram_file(client, file_id).await?;
    telegram_file_path(&file)
        .map(str::to_owned)
        .ok_or(TelegramFileError::MissingFilePath)
}

/// Download path of a `getFile` result, when Telegram returned a usable one.
#[must_use]
pub fn telegram_file_path(file: &File) -> Option<&str> {
    file.file_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
}

/// Download a file within [`TELEGRAM_FILE_DOWNLOAD_TIMEOUT`], refusing bodies above `max_bytes`.
pub async fn download_telegram_file(
    client: &TelegramClient,
    file_path: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, TelegramFileError> {
    download_telegram_file_within(client, file_path, max_bytes, TELEGRAM_FILE_DOWNLOAD_TIMEOUT)
        .await
}

async fn get_telegram_file_within(
    client: &TelegramClient,
    file_id: &str,
    timeout: Duration,
) -> Result<File, TelegramFileError> {
    tokio::time::timeout(timeout, client.execute(GetFile::new(file_id)))
        .await
        .map_err(|_| TelegramFileError::GetFileTimedOut(timeout))?
        .map_err(TelegramFileError::GetFile)
}

async fn download_telegram_file_within(
    client: &TelegramClient,
    file_path: &str,
    max_bytes: usize,
    timeout: Duration,
) -> Result<Vec<u8>, TelegramFileError> {
    let download = async {
        let mut stream = client
            .download_file(file_path)
            .await
            .map_err(TelegramFileError::Download)?;
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            // The download URL carries the bot token.
            let chunk =
                chunk.map_err(|error| TelegramFileError::DownloadChunk(error.without_url()))?;
            let received = data.len().saturating_add(chunk.len());
            if received > max_bytes {
                return Err(TelegramFileError::TooLarge {
                    received,
                    limit: max_bytes,
                });
            }
            data.extend_from_slice(&chunk);
        }
        Ok(data)
    };
    tokio::time::timeout(timeout, download)
        .await
        .map_err(|_| TelegramFileError::DownloadTimedOut(timeout))?
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener},
        sync::mpsc,
        time::{Duration, Instant},
    };

    use super::{
        TELEGRAM_FILE_DOWNLOAD_TIMEOUT, TELEGRAM_GET_FILE_TIMEOUT, TelegramFileError,
        download_telegram_file_within, get_telegram_file_within,
    };
    use crate::TelegramClient;

    const TEST_TIMEOUT: Duration = Duration::from_millis(300);

    fn client(address: SocketAddr) -> TelegramClient {
        TelegramClient::new("fixture")
            .expect("fixture client")
            .with_host(format!("http://{address}"))
            .with_max_retries(0)
    }

    /// Answer one request with `head` + `body`, then keep the socket open until released.
    fn serve_once(head: String, body: Vec<u8>) -> (SocketAddr, mpsc::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        let address = listener.local_addr().expect("fixture server address");
        let (release, released) = mpsc::channel::<()>();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => return,
                    Ok(read) => request.extend_from_slice(&buffer[..read]),
                }
            }
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&body);
            let _ = stream.flush();
            let _ = released.recv_timeout(Duration::from_secs(5));
        });
        (address, release)
    }

    #[test]
    fn get_file_bound_stays_within_ten_seconds() {
        assert!(TELEGRAM_GET_FILE_TIMEOUT <= Duration::from_secs(10));
        assert!(TELEGRAM_FILE_DOWNLOAD_TIMEOUT <= Duration::from_secs(30));
    }

    #[tokio::test]
    async fn get_file_stops_waiting_for_a_silent_server() {
        // Never accepted: the kernel backlog completes the handshake and nothing answers.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent server");
        let address = listener.local_addr().expect("silent server address");
        let started = Instant::now();

        let error = get_telegram_file_within(&client(address), "file-id", TEST_TIMEOUT)
            .await
            .expect_err("a silent server must time out");

        assert!(
            matches!(error, TelegramFileError::GetFileTimedOut(timeout) if timeout == TEST_TIMEOUT)
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(
            TelegramFileError::GetFileTimedOut(Duration::from_secs(10)).to_string(),
            "get telegram file: timed out after 10s"
        );
    }

    #[tokio::test]
    async fn download_stops_waiting_for_a_stalled_body() {
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n".to_owned();
        let (address, _release) = serve_once(head, b"partial".to_vec());

        let error =
            download_telegram_file_within(&client(address), "voice/file.oga", 4096, TEST_TIMEOUT)
                .await
                .expect_err("a stalled body must time out");

        assert!(
            matches!(error, TelegramFileError::DownloadTimedOut(timeout) if timeout == TEST_TIMEOUT)
        );
    }

    #[tokio::test]
    async fn download_returns_the_body_and_enforces_the_size_limit() {
        let body = b"0123456789".to_vec();
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );

        let (address, release) = serve_once(head.clone(), body.clone());
        let data =
            download_telegram_file_within(&client(address), "photos/file.jpg", 10, TEST_TIMEOUT)
                .await
                .expect("a body at the limit downloads");
        let _ = release.send(());
        assert_eq!(data, body);

        let (address, release) = serve_once(head, body);
        let error =
            download_telegram_file_within(&client(address), "photos/file.jpg", 9, TEST_TIMEOUT)
                .await
                .expect_err("a body above the limit is refused");
        let _ = release.send(());
        assert!(matches!(
            error,
            TelegramFileError::TooLarge {
                received: 10,
                limit: 9
            }
        ));
    }
}
