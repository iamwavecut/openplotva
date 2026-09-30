use carapax::api::{Client, Method};
use serde_json::{Map, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};

/// Capture SDK wire payloads when multipart-backed types cannot be serialized downstream.
pub(crate) async fn bot_api_payload<M: Method>(
    method: M,
) -> Result<Value, Box<dyn std::error::Error>>
where
    M::Response: serde::de::DeserializeOwned + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = std::thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(offset) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..offset]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                if bytes.len() >= offset + 4 + length {
                    break;
                }
            }
        }
        let body = r#"{"ok":false,"error_code":400,"description":"fixture capture complete"}"#;
        write!(
            stream,
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )?;
        Ok(bytes)
    });
    let client = Client::new("fixture")?
        .with_host(format!("http://{address}"))
        .with_max_retries(0);
    let _ = client.execute(method).await;
    let bytes = server.join().map_err(|_| "fixture server panicked")??;
    let offset = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("missing headers")?;
    let headers = String::from_utf8_lossy(&bytes[..offset]);
    let body = &bytes[offset + 4..];
    if headers.to_ascii_lowercase().contains("application/json") {
        return Ok(serde_json::from_slice(body)?);
    }
    let mut fields = Map::new();
    let body = String::from_utf8(body.to_vec())?;
    for part in body.split("\r\n--") {
        if let Some((headers, value)) = part.split_once("\r\n\r\n") {
            let name = headers
                .split("name=\"")
                .nth(1)
                .and_then(|v| v.split('"').next())
                .ok_or("missing field name")?;
            let value = value.trim_end_matches("\r\n");
            fields.insert(
                name.to_owned(),
                serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned())),
            );
        }
    }
    Ok(Value::Object(fields))
}
