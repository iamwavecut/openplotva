//! Alibaba ModelScope image generation client.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://api-inference.modelscope.cn";
pub const DEFAULT_MODEL: &str = "Tongyi-MAI/Z-Image-Turbo";
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// ModelScope client configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelScopeConfig {
    /// API key used as a bearer token.
    pub api_key: String,
    /// Base API URL.
    pub base_url: String,
    /// Default task poll interval.
    pub poll_interval: Duration,
    /// HTTP request timeout.
    pub request_timeout: Duration,
}

impl Default for ModelScopeConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            base_url: String::new(),
            poll_interval: Duration::ZERO,
            request_timeout: Duration::ZERO,
        }
    }
}

impl ModelScopeConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.api_key = self.api_key.trim().to_owned();
        self.base_url = if self.base_url.trim().is_empty() {
            DEFAULT_BASE_URL.to_owned()
        } else {
            self.base_url.trim().trim_end_matches('/').to_owned()
        };
        if self.poll_interval == Duration::ZERO {
            self.poll_interval = DEFAULT_POLL_INTERVAL;
        }
        if self.request_timeout == Duration::ZERO {
            self.request_timeout = DEFAULT_REQUEST_TIMEOUT;
        }
        self
    }

    #[must_use]
    pub fn configured(&self) -> bool {
        !self.api_key.trim().is_empty() && !self.base_url.trim().is_empty()
    }
}

/// ModelScope generation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelScopeGenerateRequest {
    pub model: String,
    /// Image prompt.
    pub prompt: String,
    /// Per-request poll interval. Defaults to config when zero.
    pub poll_interval: Duration,
}

/// ModelScope image generation result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelScopeResult {
    /// Downloaded image bytes.
    pub images: Vec<Vec<u8>>,
}

/// HTTP ModelScope client.
#[derive(Clone, Debug)]
pub struct ModelScopeClient {
    cfg: ModelScopeConfig,
    http: reqwest::Client,
}

impl ModelScopeClient {
    /// Build a reqwest-backed ModelScope client.
    pub fn new(cfg: ModelScopeConfig) -> Result<Self, ModelScopeError> {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .map_err(ModelScopeError::BuildHttpClient)?;
        Ok(Self { cfg, http })
    }

    pub async fn generate(
        &self,
        req: ModelScopeGenerateRequest,
    ) -> Result<ModelScopeResult, ModelScopeError> {
        if !self.cfg.configured() {
            return Err(ModelScopeError::NotConfigured);
        }
        let prompt = req.prompt.trim();
        if prompt.is_empty() {
            return Err(ModelScopeError::EmptyPrompt);
        }
        let model = if req.model.trim().is_empty() {
            DEFAULT_MODEL
        } else {
            req.model.trim()
        };
        let poll_interval = if req.poll_interval == Duration::ZERO {
            self.cfg.poll_interval
        } else {
            req.poll_interval
        };

        let task_id = self.start_generation(model, prompt).await?;
        let images = self.wait_for_images(&task_id, poll_interval).await?;
        Ok(ModelScopeResult { images })
    }

    async fn start_generation(&self, model: &str, prompt: &str) -> Result<String, ModelScopeError> {
        let response = self
            .http
            .post(format!("{}/v1/images/generations", self.cfg.base_url))
            .header("Authorization", format!("Bearer {}", self.cfg.api_key))
            .header("Content-Type", "application/json")
            .header("X-ModelScope-Async-Mode", "true")
            .json(&GenerationPayload { model, prompt })
            .send()
            .await
            .map_err(ModelScopeError::Http)?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(ModelScopeError::Http)?
            .to_vec();
        if status >= 300 {
            return Err(ModelScopeError::GenerationStatus {
                status,
                body: body_text(&body),
            });
        }
        let decoded: GenerationResponse =
            serde_json::from_slice(&body).map_err(ModelScopeError::GenerationJson)?;
        let task_id = decoded.task_id.trim();
        if task_id.is_empty() {
            return Err(ModelScopeError::EmptyTaskId);
        }
        Ok(task_id.to_owned())
    }

    async fn wait_for_images(
        &self,
        task_id: &str,
        poll_interval: Duration,
    ) -> Result<Vec<Vec<u8>>, ModelScopeError> {
        loop {
            let status = self.get_task_status(task_id).await?;
            if status.task_status.trim().is_empty() {
                return Err(ModelScopeError::EmptyTaskStatus {
                    task_id: task_id.to_owned(),
                });
            }
            if status.task_status.eq_ignore_ascii_case("SUCCEED") {
                let urls = non_empty_output_images(&status.output_images);
                if urls.is_empty() {
                    return Err(ModelScopeError::NoImages {
                        task_id: task_id.to_owned(),
                    });
                }
                return self.download_images(urls).await;
            }
            if status.task_status.eq_ignore_ascii_case("FAILED") {
                return Err(ModelScopeError::TaskFailed {
                    task_id: task_id.to_owned(),
                    message: task_failure_message(&status),
                });
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn get_task_status(&self, task_id: &str) -> Result<TaskStatusResponse, ModelScopeError> {
        let response = self
            .http
            .get(format!("{}/v1/tasks/{}", self.cfg.base_url, task_id))
            .header("Authorization", format!("Bearer {}", self.cfg.api_key))
            .header("X-ModelScope-Task-Type", "image_generation")
            .send()
            .await
            .map_err(ModelScopeError::Http)?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(ModelScopeError::Http)?
            .to_vec();
        if status >= 300 {
            return Err(ModelScopeError::StatusStatus {
                status,
                body: body_text(&body),
            });
        }
        serde_json::from_slice(&body).map_err(ModelScopeError::StatusJson)
    }

    async fn download_images(&self, urls: Vec<String>) -> Result<Vec<Vec<u8>>, ModelScopeError> {
        let mut images = Vec::new();
        for url in urls {
            let image = self.download_image(&url).await?;
            if !image.is_empty() {
                images.push(image);
            }
        }
        if images.is_empty() {
            return Err(ModelScopeError::NoImagesDownloaded);
        }
        Ok(images)
    }

    async fn download_image(&self, url: &str) -> Result<Vec<u8>, ModelScopeError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(ModelScopeError::Http)?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(ModelScopeError::Http)?
            .to_vec();
        if status >= 300 {
            return Err(ModelScopeError::DownloadStatus {
                status,
                body: body_text(&body),
            });
        }
        Ok(body)
    }
}

/// ModelScope client errors.
#[derive(Debug, Error)]
pub enum ModelScopeError {
    /// HTTP client construction failed.
    #[error("build modelscope HTTP client: {0}")]
    BuildHttpClient(reqwest::Error),
    /// Missing API key.
    #[error("modelscope api key is empty")]
    NotConfigured,
    /// Missing prompt.
    #[error("prompt is empty")]
    EmptyPrompt,
    /// HTTP request failed.
    #[error("send modelscope request: {0}")]
    Http(reqwest::Error),
    /// Submit endpoint returned a non-success status.
    #[error("modelscope generation failed with status {status}: {body}")]
    GenerationStatus {
        /// HTTP status.
        status: u16,
        /// Response body.
        body: String,
    },
    /// Submit endpoint JSON failed to decode.
    #[error("failed to decode modelscope response: {0}")]
    GenerationJson(serde_json::Error),
    /// Submit endpoint omitted `task_id`.
    #[error("modelscope returned empty task_id")]
    EmptyTaskId,
    /// Task status endpoint returned a non-success status.
    #[error("modelscope status failed with status {status}: {body}")]
    StatusStatus {
        /// HTTP status.
        status: u16,
        /// Response body.
        body: String,
    },
    /// Task status JSON failed to decode.
    #[error("failed to decode modelscope status: {0}")]
    StatusJson(serde_json::Error),
    /// Task status omitted `task_status`.
    #[error("empty task status for {task_id}")]
    EmptyTaskStatus {
        /// ModelScope task ID.
        task_id: String,
    },
    /// Successful task omitted image URLs.
    #[error("task {task_id} succeeded but returned no images")]
    NoImages {
        /// ModelScope task ID.
        task_id: String,
    },
    /// Task failed upstream.
    #[error("modelscope task {task_id} failed: {message}")]
    TaskFailed {
        /// ModelScope task ID.
        task_id: String,
        /// Failure message.
        message: String,
    },
    /// Image download returned a non-success status.
    #[error("image download failed with status {status}: {body}")]
    DownloadStatus {
        /// HTTP status.
        status: u16,
        /// Response body.
        body: String,
    },
    /// All image URLs were empty or returned empty bodies.
    #[error("no images downloaded from modelscope")]
    NoImagesDownloaded,
}

#[derive(Debug, Serialize)]
struct GenerationPayload<'a> {
    model: &'a str,
    prompt: &'a str,
}

#[derive(Debug, Deserialize)]
struct GenerationResponse {
    #[serde(default)]
    task_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
struct TaskStatusResponse {
    #[serde(default)]
    task_status: String,
    #[serde(default)]
    output_images: Vec<String>,
    #[serde(default)]
    message: String,
    #[serde(default)]
    code: String,
}

fn non_empty_output_images(images: &[String]) -> Vec<String> {
    images
        .iter()
        .filter_map(|url| {
            let trimmed = url.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
        .collect()
}

fn task_failure_message(status: &TaskStatusResponse) -> String {
    let message = status.message.trim();
    if !message.is_empty() {
        return message.to_owned();
    }
    let code = status.code.trim();
    if !code.is_empty() {
        return code.to_owned();
    }
    "unknown error".to_owned()
}

fn body_text(body: &[u8]) -> String {
    String::from_utf8_lossy(body).trim().to_owned()
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
        time::Duration as StdDuration,
    };

    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn modelscope_generate_polls_and_downloads_image_like_go() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
        let addr = listener.local_addr().expect("local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            let image_url = format!("http://{addr}/image.webp");
            for step in 0..3 {
                let (mut stream, _) = listener.accept().expect("accept request");
                let request = read_request(&mut stream);
                captured.lock().expect("requests").push(request.clone());
                match step {
                    0 => write_json(
                        &mut stream,
                        &format!(r#"{{"task_id":"task-1","image_url":"{image_url}"}}"#),
                    ),
                    1 => write_json(
                        &mut stream,
                        &format!(r#"{{"task_status":"SUCCEED","output_images":["{image_url}"]}}"#),
                    ),
                    _ => write_response(&mut stream, "image/webp", b"image-bytes"),
                }
            }
        });

        let client = ModelScopeClient::new(ModelScopeConfig {
            api_key: "test-key".to_owned(),
            base_url: format!("http://{addr}"),
            poll_interval: StdDuration::from_millis(1),
            request_timeout: StdDuration::from_secs(5),
        })
        .expect("client");

        let result = client
            .generate(ModelScopeGenerateRequest {
                prompt: " quiet lake ".to_owned(),
                ..ModelScopeGenerateRequest::default()
            })
            .await
            .expect("generate");

        handle.join().expect("server thread");
        assert_eq!(result.images, vec![b"image-bytes".to_vec()]);

        let requests = requests.lock().expect("requests");
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /v1/images/generations HTTP/1.1"));
        assert!(contains_header(
            &requests[0],
            "authorization",
            "Bearer test-key"
        ));
        assert!(contains_header(
            &requests[0],
            "x-modelscope-async-mode",
            "true"
        ));
        assert!(contains_header(
            &requests[0],
            "content-type",
            "application/json"
        ));
        let body = requests[0].split("\r\n\r\n").nth(1).expect("body");
        let payload: Value = serde_json::from_str(body).expect("generation payload");
        assert_eq!(payload["model"], DEFAULT_MODEL);
        assert_eq!(payload["prompt"], "quiet lake");

        assert!(requests[1].starts_with("GET /v1/tasks/task-1 HTTP/1.1"));
        assert!(contains_header(
            &requests[1],
            "authorization",
            "Bearer test-key"
        ));
        assert!(contains_header(
            &requests[1],
            "x-modelscope-task-type",
            "image_generation"
        ));
        assert!(requests[2].starts_with("GET /image.webp HTTP/1.1"));
    }

    #[tokio::test]
    async fn modelscope_failed_task_prefers_message_then_code_then_unknown() {
        let status = TaskStatusResponse {
            task_status: "FAILED".to_owned(),
            message: " quota exceeded ".to_owned(),
            code: "ERR_QUOTA".to_owned(),
            output_images: Vec::new(),
        };
        assert_eq!(task_failure_message(&status), "quota exceeded");

        let status = TaskStatusResponse {
            message: String::new(),
            ..status
        };
        assert_eq!(task_failure_message(&status), "ERR_QUOTA");

        let status = TaskStatusResponse {
            code: String::new(),
            ..status
        };
        assert_eq!(task_failure_message(&status), "unknown error");
    }

    fn read_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(StdDuration::from_secs(2)))
            .expect("read timeout");
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if request_complete(&buffer) {
                break;
            }
        }
        String::from_utf8(buffer).expect("utf8 request")
    }

    fn request_complete(buffer: &[u8]) -> bool {
        let Some(header_end) = find_header_end(buffer) else {
            return false;
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        buffer.len() >= header_end + 4 + content_length
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn write_json(stream: &mut TcpStream, body: &str) {
        write_response(stream, "application/json", body.as_bytes());
    }

    fn write_response(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n",
            body.len()
        )
        .expect("write headers");
        stream.write_all(body).expect("write body");
        stream.flush().expect("flush");
    }

    fn contains_header(request: &str, name: &str, expected: &str) -> bool {
        request.lines().any(|line| {
            let Some((header, value)) = line.split_once(':') else {
                return false;
            };
            header.eq_ignore_ascii_case(name) && value.trim() == expected
        })
    }
}
