//! The `run_python` dialog tool.
//!
//! The model's script runs in the sandbox supervisor next to the bot. Chat
//! files are copied into the session home before the run, and the files the
//! model names in `send_files` are uploaded to the chat after a clean exit.

use std::{collections::BTreeSet, future::Future, path::Path, pin::Pin, sync::Arc};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use openplotva_dialog::{PythonRunRequest, TOOL_RESULT_STATUS_OK, ToolContext, ToolResult};
use openplotva_sandbox::api::{InputFile, RunRequest, RunResponse, Termination};
use openplotva_sandbox::client::{SandboxClient, SandboxClientError};
use openplotva_storage::{PostgresTelegramFileStore, TelegramFileRecord};
use openplotva_telegram::{
    AudioMessageRequest, AudioSource, ChatRef, MediaGroupMessageRequest, MediaGroupPhotoItem,
    PhotoMessageRequest, PhotoSource, ReplyParametersPlan, TelegramClient, UploadMessageRequest,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::dialog_messages::DirectDrawApiHistoryStore;

/// Largest file Telegram lets a bot download.
const MAX_INPUT_FILE_BYTES: u64 = 20 * 1024 * 1024;
/// Decoded input bytes per call; base64 keeps the request under the
/// supervisor's body limit.
const MAX_INPUT_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_INPUT_FILES: usize = 10;
/// Largest image Telegram accepts as a photo; bigger ones go as documents.
const MAX_PHOTO_BYTES: usize = 10_000_000;
const MAX_ALBUM_ITEMS: usize = 10;
const MAX_FILE_NAME_BYTES: usize = 120;
const CAPTION_MAX_CHARS: usize = 1024;
const STDOUT_MAX_CHARS: usize = 4000;
const STDERR_MAX_CHARS: usize = 3000;
const LISTED_FILES_MAX: usize = 30;
const ERROR_LINE_MAX_CHARS: usize = 300;

/// Boxed future returned by the sandbox tool boundaries.
pub type SandboxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The supervisor API the tool needs.
pub trait SandboxBackend: Send + Sync {
    fn run<'a>(
        &'a self,
        session: &'a str,
        request: &'a RunRequest,
    ) -> SandboxFuture<'a, Result<RunResponse, SandboxClientError>>;

    fn read_file<'a>(
        &'a self,
        session: &'a str,
        path: &'a str,
    ) -> SandboxFuture<'a, Result<Vec<u8>, SandboxClientError>>;
}

impl SandboxBackend for SandboxClient {
    fn run<'a>(
        &'a self,
        session: &'a str,
        request: &'a RunRequest,
    ) -> SandboxFuture<'a, Result<RunResponse, SandboxClientError>> {
        Box::pin(SandboxClient::run(self, session, request))
    }

    fn read_file<'a>(
        &'a self,
        session: &'a str,
        path: &'a str,
    ) -> SandboxFuture<'a, Result<Vec<u8>, SandboxClientError>> {
        Box::pin(SandboxClient::read_file(self, session, path))
    }
}

/// A chat file downloaded for a run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatFile {
    /// Original file name, when the chat message carries one.
    pub file_name: Option<String>,
    /// Telegram media kind, such as `photo` or `document`.
    pub kind: String,
    /// Extension of Telegram's stored file, without the dot.
    pub extension: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum ChatFileError {
    #[error("the file is not available in this chat")]
    NotFound,
    #[error("the file has {size} bytes; Telegram lets the bot download at most {limit}")]
    TooLarge { size: u64, limit: u64 },
    #[error("{0}")]
    Failed(String),
}

/// Resolves and downloads files the dialog references.
pub trait SandboxChatFiles: Send + Sync {
    fn fetch<'a>(
        &'a self,
        context: &'a ToolContext,
        reference: &'a str,
    ) -> SandboxFuture<'a, Result<ChatFile, ChatFileError>>;
}

/// A file the script created, read back from the session home.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveryError {
    pub path: String,
    pub error: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeliveryReport {
    pub sent: Vec<String>,
    pub errors: Vec<DeliveryError>,
}

/// Uploads output files to the chat of the dialog turn.
pub trait SandboxDelivery: Send + Sync {
    fn deliver<'a>(
        &'a self,
        context: &'a ToolContext,
        caption: &'a str,
        files: Vec<OutputFile>,
    ) -> SandboxFuture<'a, DeliveryReport>;
}

/// The `run_python` tool: chat files in, script run, output files out.
#[derive(Clone)]
pub struct PythonSandboxTool {
    backend: Arc<dyn SandboxBackend>,
    chat_files: Arc<dyn SandboxChatFiles>,
    delivery: Arc<dyn SandboxDelivery>,
}

impl PythonSandboxTool {
    #[must_use]
    pub fn new(
        backend: Arc<dyn SandboxBackend>,
        chat_files: Arc<dyn SandboxChatFiles>,
        delivery: Arc<dyn SandboxDelivery>,
    ) -> Self {
        Self {
            backend,
            chat_files,
            delivery,
        }
    }

    pub async fn run(&self, request: PythonRunRequest) -> ToolResult {
        let context = &request.context;
        if context.chat_id == 0 {
            return ToolResult::failed(
                "run_python_unavailable",
                "This dialog has no chat to run code for.",
            );
        }
        let file_ids = unique_values(&request.file_ids);
        if file_ids.len() > MAX_INPUT_FILES {
            return ToolResult::failed(
                "run_python_too_many_files",
                format!("Pass at most {MAX_INPUT_FILES} file_ids."),
            );
        }
        let send_files = unique_values(
            &request
                .send_files
                .iter()
                .map(|path| home_relative(path).to_owned())
                .collect::<Vec<_>>(),
        );
        if send_files.len() > openplotva_dialog::MAX_SEND_FILES {
            return ToolResult::failed(
                "run_python_too_many_files",
                format!(
                    "Pass at most {} send_files.",
                    openplotva_dialog::MAX_SEND_FILES
                ),
            );
        }
        let inputs = match self.fetch_inputs(context, &file_ids).await {
            Ok(inputs) => inputs,
            Err(result) => return *result,
        };
        let input_names = inputs.iter().map(|file| file.path.clone()).collect();
        let session = session_key(context);
        let run_request = RunRequest {
            code: request.code.clone(),
            files: inputs,
        };
        let response = match self.backend.run(&session, &run_request).await {
            Ok(response) => response,
            Err(SandboxClientError::Busy(_)) => {
                return ToolResult::failed(
                    "sandbox_busy",
                    "Every sandbox slot is busy. Ask the user to try again in a minute.",
                );
            }
            Err(SandboxClientError::Rejected { code, message }) => {
                return ToolResult::failed("run_python_rejected", format!("{code}: {message}"));
            }
            Err(error) => {
                tracing::warn!(%error, chat_id = context.chat_id, "python sandbox call failed");
                return ToolResult::failed(
                    "sandbox_unavailable",
                    "The Python sandbox is unavailable right now.",
                );
            }
        };

        let succeeded =
            response.termination == Termination::Exited && response.exit_code == Some(0);
        let mut report = DeliveryReport::default();
        if succeeded && !send_files.is_empty() {
            let mut outputs = Vec::new();
            for path in &send_files {
                match self.backend.read_file(&session, path).await {
                    Ok(bytes) if bytes.is_empty() => report.errors.push(DeliveryError {
                        path: path.clone(),
                        error: "the file is empty".to_owned(),
                    }),
                    Ok(bytes) => outputs.push(OutputFile {
                        path: path.clone(),
                        bytes,
                    }),
                    Err(error) => report.errors.push(DeliveryError {
                        path: path.clone(),
                        error: error.to_string(),
                    }),
                }
            }
            if !outputs.is_empty() {
                let delivered = self
                    .delivery
                    .deliver(context, request.caption.trim(), outputs)
                    .await;
                report.sent = delivered.sent;
                report.errors.extend(delivered.errors);
            }
        }
        let skipped = if succeeded { Vec::new() } else { send_files };
        run_result(&response, input_names, report, skipped)
    }

    async fn fetch_inputs(
        &self,
        context: &ToolContext,
        file_ids: &[String],
    ) -> Result<Vec<InputFile>, Box<ToolResult>> {
        let mut used_names = BTreeSet::new();
        let mut total_bytes = 0_u64;
        let mut files = Vec::with_capacity(file_ids.len());
        for (index, reference) in file_ids.iter().enumerate() {
            let file = self
                .chat_files
                .fetch(context, reference)
                .await
                .map_err(|error| {
                    let code = match error {
                        ChatFileError::NotFound => "run_python_file_not_found",
                        ChatFileError::TooLarge { .. } => "run_python_file_too_large",
                        ChatFileError::Failed(_) => "run_python_file_unavailable",
                    };
                    Box::new(ToolResult::failed(code, format!("{reference}: {error}")))
                })?;
            total_bytes = total_bytes.saturating_add(file.bytes.len() as u64);
            if total_bytes > MAX_INPUT_TOTAL_BYTES {
                return Err(Box::new(ToolResult::failed(
                    "run_python_file_too_large",
                    format!(
                        "The input files exceed {} MiB together.",
                        MAX_INPUT_TOTAL_BYTES / 1024 / 1024
                    ),
                )));
            }
            let name = unique_name(input_file_name(&file, index), &mut used_names);
            files.push(InputFile {
                path: name,
                content_base64: BASE64_STANDARD.encode(&file.bytes),
            });
        }
        Ok(files)
    }
}

/// One sandbox session per chat thread, so follow-up calls see earlier files.
fn session_key(context: &ToolContext) -> String {
    match context.thread_id {
        Some(thread_id) if thread_id != 0 => format!("c{}_t{thread_id}", context.chat_id),
        _ => format!("c{}", context.chat_id),
    }
}

/// Scripts run in `/home`, so the model often writes absolute or `./` paths.
fn home_relative(path: &str) -> &str {
    let path = path.trim();
    let path = path.strip_prefix("/home/").unwrap_or(path);
    path.strip_prefix("./").unwrap_or(path)
}

fn unique_values(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty() && seen.insert(*value))
        .map(str::to_owned)
        .collect()
}

fn input_file_name(file: &ChatFile, index: usize) -> String {
    if let Some(name) = file.file_name.as_deref().and_then(sanitize_file_name) {
        return name;
    }
    let kind = sanitize_file_name(&file.kind).unwrap_or_else(|| "file".to_owned());
    let extension = file
        .extension
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>();
    let extension = if extension.is_empty() {
        "bin".to_owned()
    } else {
        extension.to_ascii_lowercase()
    };
    format!("{kind}_{}.{extension}", index + 1)
}

/// Keep the base name only, without control characters or a leading dot,
/// so the supervisor accepts it as a plain file in the session home.
fn sanitize_file_name(name: &str) -> Option<String> {
    let base = name.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned = base
        .chars()
        .map(|c| if c.is_control() || c == ':' { '_' } else { c })
        .collect::<String>();
    let cleaned = cleaned.trim().trim_start_matches('.').trim();
    if cleaned.is_empty() {
        return None;
    }
    if cleaned.len() <= MAX_FILE_NAME_BYTES {
        return Some(cleaned.to_owned());
    }
    let (stem, extension) = match cleaned.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() && extension.len() <= 10 => {
            (stem, Some(extension))
        }
        _ => (cleaned, None),
    };
    let budget = MAX_FILE_NAME_BYTES - extension.map_or(0, |extension| extension.len() + 1);
    let mut end = budget.min(stem.len());
    while !stem.is_char_boundary(end) {
        end -= 1;
    }
    Some(match extension {
        Some(extension) => format!("{}.{extension}", &stem[..end]),
        None => stem[..end].to_owned(),
    })
}

fn unique_name(name: String, used: &mut BTreeSet<String>) -> String {
    if used.insert(name.clone()) {
        return name;
    }
    let (stem, extension) = match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => (stem.to_owned(), format!(".{extension}")),
        _ => (name.clone(), String::new()),
    };
    (2..)
        .map(|suffix| format!("{stem}_{suffix}{extension}"))
        .find(|candidate| used.insert(candidate.clone()))
        .unwrap_or(name)
}

fn run_result(
    response: &RunResponse,
    inputs: Vec<String>,
    report: DeliveryReport,
    skipped: Vec<String>,
) -> ToolResult {
    let mut data = Map::new();
    data.insert(
        "session".to_owned(),
        json!(if response.session.created {
            "new"
        } else {
            "continued"
        }),
    );
    data.insert("termination".to_owned(), json!(response.termination));
    if let Some(exit_code) = response.exit_code {
        data.insert("exit_code".to_owned(), json!(exit_code));
    }
    data.insert("duration_ms".to_owned(), json!(response.duration_ms));
    if !response.stdout.is_empty() {
        let (stdout, clipped) = clip_head(&response.stdout, STDOUT_MAX_CHARS);
        data.insert("stdout".to_owned(), json!(stdout));
        if clipped || response.stdout_truncated {
            data.insert("stdout_truncated".to_owned(), json!(true));
        }
    }
    if !response.stderr.is_empty() {
        let (stderr, clipped) = clip_tail(&response.stderr, STDERR_MAX_CHARS);
        data.insert("stderr".to_owned(), json!(stderr));
        if clipped || response.stderr_truncated {
            data.insert("stderr_truncated".to_owned(), json!(true));
        }
    }
    if !response.files.is_empty() {
        data.insert(
            "files".to_owned(),
            json!(
                response
                    .files
                    .iter()
                    .take(LISTED_FILES_MAX)
                    .collect::<Vec<_>>()
            ),
        );
        if response.files.len() > LISTED_FILES_MAX {
            data.insert("files_truncated".to_owned(), json!(true));
        }
    }
    if !inputs.is_empty() {
        data.insert("inputs".to_owned(), json!(inputs));
    }
    if !report.sent.is_empty() {
        data.insert("sent".to_owned(), json!(report.sent));
    }
    if !report.errors.is_empty() {
        data.insert("send_errors".to_owned(), json!(report.errors));
    }
    if !skipped.is_empty() {
        data.insert("send_skipped".to_owned(), json!(skipped));
    }
    let data = Some(Value::Object(data));

    let (code, reason) = match (response.termination, response.exit_code) {
        (Termination::Exited, Some(0)) if report.errors.is_empty() => {
            let message = if report.sent.is_empty() {
                "The script finished.".to_owned()
            } else {
                format!(
                    "The script finished; the chat received {}.",
                    report.sent.join(", ")
                )
            };
            return ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message,
                data,
                ..ToolResult::default()
            };
        }
        (Termination::Exited, Some(0)) => (
            "run_python_files_not_sent",
            "The script finished, but some files were not sent; see send_errors.".to_owned(),
        ),
        (Termination::Exited, exit_code) => (
            "python_error",
            last_error_line(&response.stderr).unwrap_or_else(|| {
                format!(
                    "The script exited with code {}.",
                    exit_code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
                )
            }),
        ),
        (Termination::Timeout, _) => (
            "python_timeout",
            "The script hit the time limit and was stopped.".to_owned(),
        ),
        (Termination::Killed, _) => (
            "python_killed",
            "The script was killed, most likely for exceeding the memory limit.".to_owned(),
        ),
        (Termination::SandboxError, _) => (
            "sandbox_error",
            "The sandbox could not start the script.".to_owned(),
        ),
    };
    ToolResult {
        data,
        ..ToolResult::failed(code, reason)
    }
}

fn clip_head(text: &str, max_chars: usize) -> (String, bool) {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => (text[..end].to_owned(), true),
        None => (text.to_owned(), false),
    }
}

fn clip_tail(text: &str, max_chars: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= max_chars {
        return (text.to_owned(), false);
    }
    (text.chars().skip(count - max_chars).collect(), true)
}

fn last_error_line(stderr: &str) -> Option<String> {
    stderr
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| clip_head(line, ERROR_LINE_MAX_CHARS).0)
}

/// How one group of output files is sent.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Delivery {
    Photos(Vec<OutputFile>),
    Animation(OutputFile),
    Video(OutputFile),
    Audio(OutputFile),
    Document(OutputFile),
}

impl Delivery {
    fn files(&self) -> Vec<&OutputFile> {
        match self {
            Self::Photos(files) => files.iter().collect(),
            Self::Animation(file)
            | Self::Video(file)
            | Self::Audio(file)
            | Self::Document(file) => {
                vec![file]
            }
        }
    }
}

fn file_extension(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// Group consecutive photos into albums and pick a send method for the rest.
fn plan_deliveries(files: Vec<OutputFile>) -> Vec<Delivery> {
    let mut deliveries = Vec::new();
    let mut photos = Vec::new();
    for file in files {
        let extension = file_extension(&file.path);
        let is_photo = matches!(extension.as_str(), "png" | "jpg" | "jpeg" | "webp")
            && file.bytes.len() <= MAX_PHOTO_BYTES;
        if is_photo {
            photos.push(file);
            if photos.len() == MAX_ALBUM_ITEMS {
                deliveries.push(Delivery::Photos(std::mem::take(&mut photos)));
            }
            continue;
        }
        if !photos.is_empty() {
            deliveries.push(Delivery::Photos(std::mem::take(&mut photos)));
        }
        deliveries.push(match extension.as_str() {
            "gif" => Delivery::Animation(file),
            "mp4" => Delivery::Video(file),
            "mp3" | "m4a" => Delivery::Audio(file),
            _ => Delivery::Document(file),
        });
    }
    if !photos.is_empty() {
        deliveries.push(Delivery::Photos(photos));
    }
    deliveries
}

fn caption_text(caption: &str) -> String {
    clip_head(caption.trim(), CAPTION_MAX_CHARS).0
}

/// Telegram-backed chat file source and delivery for the tool.
#[derive(Clone)]
pub struct TelegramSandboxIo {
    telegram: TelegramClient,
    files: PostgresTelegramFileStore,
    history: Option<Arc<dyn DirectDrawApiHistoryStore>>,
    bot_id: i64,
}

impl TelegramSandboxIo {
    #[must_use]
    pub fn new(telegram: TelegramClient, files: PostgresTelegramFileStore) -> Self {
        Self {
            telegram,
            files,
            history: None,
            bot_id: 0,
        }
    }

    #[must_use]
    pub fn with_history_store(
        mut self,
        history: Arc<dyn DirectDrawApiHistoryStore>,
        bot_id: i64,
    ) -> Self {
        self.history = Some(history);
        self.bot_id = bot_id;
        self
    }

    async fn lookup(
        &self,
        reference: &str,
        chat_id: i64,
    ) -> Result<Option<TelegramFileRecord>, ChatFileError> {
        let storage = |error: openplotva_storage::StorageError| {
            ChatFileError::Failed(format!("file lookup failed: {error}"))
        };
        let record = match self.files.get_file(reference).await.map_err(storage)? {
            Some(record) => Some(record),
            None => self
                .files
                .get_file_by_latest_file_id(reference)
                .await
                .map_err(storage)?,
        };
        Ok(record.filter(|record| crate::vision::telegram_file_seen_in_chat(record, chat_id)))
    }

    async fn download(&self, file_id: &str) -> Result<(Vec<u8>, String), ChatFileError> {
        let file: carapax::types::File = self
            .telegram
            .execute(carapax::types::GetFile::new(file_id))
            .await
            .map_err(|error| ChatFileError::Failed(format!("getFile failed: {error}")))?;
        if let Some(size) = file.file_size.and_then(|size| u64::try_from(size).ok())
            && size > MAX_INPUT_FILE_BYTES
        {
            return Err(ChatFileError::TooLarge {
                size,
                limit: MAX_INPUT_FILE_BYTES,
            });
        }
        let path = file
            .file_path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| ChatFileError::Failed("Telegram returned no file path".to_owned()))?
            .to_owned();
        let mut stream = self
            .telegram
            .download_file(&path)
            .await
            .map_err(|error| ChatFileError::Failed(format!("download failed: {error}")))?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| ChatFileError::Failed(format!("download failed: {error}")))?;
            let size = (bytes.len() + chunk.len()) as u64;
            if size > MAX_INPUT_FILE_BYTES {
                return Err(ChatFileError::TooLarge {
                    size,
                    limit: MAX_INPUT_FILE_BYTES,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok((bytes, file_extension(&path)))
    }

    async fn send(
        &self,
        target: &UploadTarget,
        delivery: &Delivery,
        caption: &str,
    ) -> Result<Vec<carapax::types::Message>, String> {
        let error = |error: carapax::api::ExecuteError| error.to_string();
        let build = |error: openplotva_telegram::OutboundBuildError| error.to_string();
        match delivery {
            Delivery::Photos(files) if files.len() == 1 => {
                let request = PhotoMessageRequest {
                    chat: target.chat,
                    message_thread_id: target.message_thread_id,
                    disable_notification: false,
                    photo: photo_source(&files[0]),
                    caption: caption.to_owned(),
                    render_as: String::new(),
                    has_spoiler: false,
                    reply_parameters: target.reply_parameters,
                };
                let method =
                    openplotva_telegram::build_photo_message_method(&request).map_err(build)?;
                self.telegram
                    .execute(method)
                    .await
                    .map(|message| vec![message])
                    .map_err(error)
            }
            Delivery::Photos(files) => {
                let request = MediaGroupMessageRequest {
                    chat: target.chat,
                    message_thread_id: target.message_thread_id,
                    disable_notification: false,
                    items: files
                        .iter()
                        .enumerate()
                        .map(|(index, file)| MediaGroupPhotoItem {
                            photo: photo_source(file),
                            caption: if index == 0 {
                                caption.to_owned()
                            } else {
                                String::new()
                            },
                            render_as: String::new(),
                            has_spoiler: false,
                        })
                        .collect(),
                    reply_parameters: target.reply_parameters,
                };
                let method = openplotva_telegram::build_media_group_message_method(&request)
                    .map_err(build)?;
                self.telegram.execute(method).await.map_err(error)
            }
            Delivery::Audio(file) => {
                let request = AudioMessageRequest {
                    chat: target.chat,
                    message_thread_id: target.message_thread_id,
                    disable_notification: false,
                    audio: AudioSource::Bytes {
                        file_name: upload_file_name(file),
                        bytes: file.bytes.clone(),
                    },
                    caption: caption.to_owned(),
                    render_as: String::new(),
                    reply_parameters: target.reply_parameters,
                };
                let method =
                    openplotva_telegram::build_audio_message_method(&request).map_err(build)?;
                self.telegram
                    .execute(method)
                    .await
                    .map(|message| vec![message])
                    .map_err(error)
            }
            Delivery::Animation(file) => {
                let method = openplotva_telegram::build_animation_message_method(
                    &target.upload(file, caption),
                )
                .map_err(build)?;
                self.telegram
                    .execute(method)
                    .await
                    .map(|message| vec![message])
                    .map_err(error)
            }
            Delivery::Video(file) => {
                let method =
                    openplotva_telegram::build_video_message_method(&target.upload(file, caption))
                        .map_err(build)?;
                self.telegram
                    .execute(method)
                    .await
                    .map(|message| vec![message])
                    .map_err(error)
            }
            Delivery::Document(file) => {
                let method = openplotva_telegram::build_document_message_method(
                    &target.upload(file, caption),
                )
                .map_err(build)?;
                self.telegram
                    .execute(method)
                    .await
                    .map(|message| vec![message])
                    .map_err(error)
            }
        }
    }

    async fn record_sent(&self, messages: &[carapax::types::Message]) {
        for message in messages {
            for file_ref in openplotva_updates::telegram_message_file_metadata_refs(message) {
                let Some(params) =
                    crate::updates::telegram_file_metadata_upsert_from_ref(&file_ref)
                else {
                    continue;
                };
                if let Err(error) = self.files.upsert_metadata(&params).await {
                    tracing::debug!(%error, "failed to record sandbox output file metadata");
                }
            }
            let Some(history) = self.history.as_ref() else {
                continue;
            };
            let Some(entry) =
                crate::dialog_messages::sent_message_history_entry(message, self.bot_id)
            else {
                continue;
            };
            if let Err(error) = history.record_direct_draw_api_history(entry).await {
                tracing::debug!(%error, "failed to record sandbox output in history");
            }
        }
    }
}

impl SandboxChatFiles for TelegramSandboxIo {
    fn fetch<'a>(
        &'a self,
        context: &'a ToolContext,
        reference: &'a str,
    ) -> SandboxFuture<'a, Result<ChatFile, ChatFileError>> {
        Box::pin(async move {
            let attachment = context
                .message_meta
                .attachments
                .iter()
                .chain(&context.image_attachments)
                .find(|attachment| {
                    attachment.file_unique_id == reference || attachment.file_id == reference
                });
            let (file_id, kind, unique_id) = match self.lookup(reference, context.chat_id).await? {
                Some(record) => (
                    record.latest_file_id,
                    record.media_kind,
                    record.file_unique_id,
                ),
                None => match attachment.filter(|attachment| !attachment.file_id.is_empty()) {
                    Some(attachment) => (
                        attachment.file_id.clone(),
                        attachment.kind.clone(),
                        attachment.file_unique_id.clone(),
                    ),
                    None => return Err(ChatFileError::NotFound),
                },
            };
            let file_name = context
                .message_meta
                .attachments
                .iter()
                .chain(&context.image_attachments)
                .find(|attachment| attachment.file_unique_id == unique_id)
                .or(attachment)
                .map(|attachment| attachment.file_name.trim().to_owned())
                .filter(|name| !name.is_empty());
            let (bytes, extension) = self.download(&file_id).await?;
            Ok(ChatFile {
                file_name,
                kind,
                extension,
                bytes,
            })
        })
    }
}

impl SandboxDelivery for TelegramSandboxIo {
    fn deliver<'a>(
        &'a self,
        context: &'a ToolContext,
        caption: &'a str,
        files: Vec<OutputFile>,
    ) -> SandboxFuture<'a, DeliveryReport> {
        Box::pin(async move {
            let target = UploadTarget::new(context);
            let mut caption = caption_text(caption);
            let mut report = DeliveryReport::default();
            for delivery in plan_deliveries(files) {
                let result = match self.send(&target, &delivery, &caption).await {
                    Ok(messages) => Ok(messages),
                    Err(error) if matches!(delivery, Delivery::Photos(_)) => {
                        tracing::debug!(%error, "photo send failed; retrying as documents");
                        self.send_as_documents(&target, &delivery, &caption).await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(messages) => {
                        self.record_sent(&messages).await;
                        report
                            .sent
                            .extend(delivery.files().into_iter().map(|file| file.path.clone()));
                        caption.clear();
                    }
                    Err(error) => {
                        report
                            .errors
                            .extend(delivery.files().into_iter().map(|file| DeliveryError {
                                path: file.path.clone(),
                                error: error.clone(),
                            }));
                    }
                }
            }
            report
        })
    }
}

impl TelegramSandboxIo {
    async fn send_as_documents(
        &self,
        target: &UploadTarget,
        delivery: &Delivery,
        caption: &str,
    ) -> Result<Vec<carapax::types::Message>, String> {
        let mut messages = Vec::new();
        for (index, file) in delivery.files().into_iter().enumerate() {
            let caption = if index == 0 { caption } else { "" };
            messages.extend(
                self.send(target, &Delivery::Document(file.clone()), caption)
                    .await?,
            );
        }
        Ok(messages)
    }
}

/// Where uploads go: the dialog's chat and thread, as a reply to the trigger.
#[derive(Clone, Copy, Debug)]
struct UploadTarget {
    chat: ChatRef,
    message_thread_id: i64,
    reply_parameters: Option<ReplyParametersPlan>,
}

impl UploadTarget {
    fn new(context: &ToolContext) -> Self {
        Self {
            chat: ChatRef {
                direct_messages_topic_id: context.message_meta.direct_messages_topic_id,
                id: context.chat_id,
                is_forum: context.thread_id.is_some(),
            },
            message_thread_id: context.thread_id.map(i64::from).unwrap_or_default(),
            reply_parameters: (context.message_id > 0).then(|| ReplyParametersPlan {
                message_id: i64::from(context.message_id),
                chat_id: context.chat_id,
                allow_sending_without_reply: true,
            }),
        }
    }

    fn upload(&self, file: &OutputFile, caption: &str) -> UploadMessageRequest {
        UploadMessageRequest {
            chat: self.chat,
            message_thread_id: self.message_thread_id,
            disable_notification: false,
            file_name: upload_file_name(file),
            bytes: file.bytes.clone(),
            caption: caption.to_owned(),
            render_as: String::new(),
            reply_parameters: self.reply_parameters,
        }
    }
}

fn upload_file_name(file: &OutputFile) -> String {
    sanitize_file_name(&file.path).unwrap_or_else(|| "file".to_owned())
}

fn photo_source(file: &OutputFile) -> PhotoSource {
    PhotoSource::Bytes {
        file_name: upload_file_name(file),
        bytes: file.bytes.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use openplotva_sandbox::api::{FileEntry, SessionInfo};

    use super::*;

    fn output(path: &str, size: usize) -> OutputFile {
        OutputFile {
            path: path.to_owned(),
            bytes: vec![1; size],
        }
    }

    fn context() -> ToolContext {
        ToolContext {
            chat_id: -10_042,
            thread_id: Some(7),
            message_id: 11,
            ..ToolContext::default()
        }
    }

    #[derive(Default)]
    struct FakeBackend {
        response: RunResponse,
        busy: bool,
        files: Vec<(String, Vec<u8>)>,
        calls: Mutex<Vec<(String, RunRequest)>>,
    }

    impl SandboxBackend for FakeBackend {
        fn run<'a>(
            &'a self,
            session: &'a str,
            request: &'a RunRequest,
        ) -> SandboxFuture<'a, Result<RunResponse, SandboxClientError>> {
            self.calls
                .lock()
                .expect("calls")
                .push((session.to_owned(), request.clone()));
            let result = if self.busy {
                Err(SandboxClientError::Busy("full".to_owned()))
            } else {
                Ok(self.response.clone())
            };
            Box::pin(async move { result })
        }

        fn read_file<'a>(
            &'a self,
            _session: &'a str,
            path: &'a str,
        ) -> SandboxFuture<'a, Result<Vec<u8>, SandboxClientError>> {
            let result = self
                .files
                .iter()
                .find(|(name, _)| name == path)
                .map(|(_, bytes)| bytes.clone())
                .ok_or_else(|| SandboxClientError::NotFound {
                    code: "file_not_found".to_owned(),
                    message: path.to_owned(),
                });
            Box::pin(async move { result })
        }
    }

    struct FakeChatFiles;

    impl SandboxChatFiles for FakeChatFiles {
        fn fetch<'a>(
            &'a self,
            _context: &'a ToolContext,
            reference: &'a str,
        ) -> SandboxFuture<'a, Result<ChatFile, ChatFileError>> {
            let result = match reference {
                "csv" => Ok(ChatFile {
                    file_name: Some("продажи.csv".to_owned()),
                    kind: "document".to_owned(),
                    extension: "csv".to_owned(),
                    bytes: b"a,b\n1,2\n".to_vec(),
                }),
                "photo" => Ok(ChatFile {
                    kind: "photo".to_owned(),
                    extension: "jpg".to_owned(),
                    bytes: vec![0xff, 0xd8],
                    ..ChatFile::default()
                }),
                _ => Err(ChatFileError::NotFound),
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Default)]
    struct FakeDelivery {
        delivered: Mutex<Vec<(String, Vec<String>)>>,
    }

    impl SandboxDelivery for FakeDelivery {
        fn deliver<'a>(
            &'a self,
            _context: &'a ToolContext,
            caption: &'a str,
            files: Vec<OutputFile>,
        ) -> SandboxFuture<'a, DeliveryReport> {
            let paths = files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>();
            self.delivered
                .lock()
                .expect("delivered")
                .push((caption.to_owned(), paths.clone()));
            Box::pin(async move {
                DeliveryReport {
                    sent: paths,
                    errors: Vec::new(),
                }
            })
        }
    }

    fn exited(code: i32) -> RunResponse {
        RunResponse {
            termination: Termination::Exited,
            exit_code: Some(code),
            stdout: "42\n".to_owned(),
            files: vec![FileEntry {
                path: "chart.png".to_owned(),
                size: 3,
                changed: true,
            }],
            session: SessionInfo {
                created: true,
                ..SessionInfo::default()
            },
            ..RunResponse::default()
        }
    }

    fn sandbox_tool(
        backend: FakeBackend,
    ) -> (PythonSandboxTool, Arc<FakeBackend>, Arc<FakeDelivery>) {
        let backend = Arc::new(backend);
        let delivery = Arc::new(FakeDelivery::default());
        let tool =
            PythonSandboxTool::new(backend.clone(), Arc::new(FakeChatFiles), delivery.clone());
        (tool, backend, delivery)
    }

    fn request(file_ids: &[&str], send_files: &[&str]) -> PythonRunRequest {
        PythonRunRequest {
            context: context(),
            code: "print(42)".to_owned(),
            file_ids: file_ids.iter().map(|id| (*id).to_owned()).collect(),
            send_files: send_files.iter().map(|path| (*path).to_owned()).collect(),
            caption: " График ".to_owned(),
        }
    }

    #[tokio::test]
    async fn successful_run_uploads_inputs_and_delivers_requested_files() {
        let (tool, backend, delivery) = sandbox_tool(FakeBackend {
            response: exited(0),
            files: vec![("chart.png".to_owned(), vec![1, 2, 3])],
            ..FakeBackend::default()
        });

        let result = tool
            .run(request(
                &["csv", "photo", "csv"],
                &["/home/chart.png", "./chart.png"],
            ))
            .await;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        let calls = backend.calls.lock().expect("calls");
        assert_eq!(calls[0].0, "c-10042_t7");
        let names = calls[0]
            .1
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["продажи.csv", "photo_2.jpg"]);
        assert_eq!(
            *delivery.delivered.lock().expect("delivered"),
            vec![("График".to_owned(), vec!["chart.png".to_owned()])]
        );
        let data = result.data.expect("data");
        assert_eq!(data["session"], "new");
        assert_eq!(data["stdout"], "42\n");
        assert_eq!(data["sent"], json!(["chart.png"]));
        assert_eq!(data["inputs"], json!(["продажи.csv", "photo_2.jpg"]));
    }

    #[tokio::test]
    async fn failed_script_reports_the_error_and_sends_nothing() {
        let mut response = exited(1);
        response.stderr =
            "Traceback (most recent call last):\n  File \"/sandbox/main.py\"\nNameError: name 'x' is not defined\n"
                .to_owned();
        let (tool, _, delivery) = sandbox_tool(FakeBackend {
            response,
            ..FakeBackend::default()
        });

        let result = tool.run(request(&[], &["chart.png"])).await;

        let error = result.error.expect("error");
        assert_eq!(error.code, "python_error");
        assert_eq!(error.reason, "NameError: name 'x' is not defined");
        assert!(delivery.delivered.lock().expect("delivered").is_empty());
        let data = result.data.expect("data");
        assert_eq!(data["send_skipped"], json!(["chart.png"]));
        assert_eq!(data["exit_code"], 1);
    }

    #[tokio::test]
    async fn missing_output_and_unknown_inputs_are_reported() {
        let (tool, backend, _) = sandbox_tool(FakeBackend {
            response: exited(0),
            ..FakeBackend::default()
        });
        let result = tool.run(request(&["nope"], &[])).await;
        assert_eq!(
            result.error.expect("error").code,
            "run_python_file_not_found"
        );
        assert!(backend.calls.lock().expect("calls").is_empty());

        let result = tool.run(request(&[], &["missing.png"])).await;
        assert_eq!(
            result.error.expect("error").code,
            "run_python_files_not_sent"
        );
        assert_eq!(
            result.data.expect("data")["send_errors"][0]["path"],
            "missing.png"
        );
    }

    #[tokio::test]
    async fn busy_and_limit_terminations_map_to_tool_errors() {
        let (tool, _, _) = sandbox_tool(FakeBackend {
            busy: true,
            ..FakeBackend::default()
        });
        assert_eq!(
            tool.run(request(&[], &[])).await.error.expect("error").code,
            "sandbox_busy"
        );

        for (termination, code) in [
            (Termination::Timeout, "python_timeout"),
            (Termination::Killed, "python_killed"),
            (Termination::SandboxError, "sandbox_error"),
        ] {
            let (tool, _, _) = sandbox_tool(FakeBackend {
                response: RunResponse {
                    termination,
                    ..RunResponse::default()
                },
                ..FakeBackend::default()
            });
            assert_eq!(
                tool.run(request(&[], &[])).await.error.expect("error").code,
                code
            );
        }
    }

    #[test]
    fn deliveries_group_photos_into_albums_and_route_media_by_extension() {
        let mut files = (0..12)
            .map(|index| output(&format!("p{index}.png"), 10))
            .collect::<Vec<_>>();
        files.push(output("big.jpg", MAX_PHOTO_BYTES + 1));
        files.push(output("a.gif", 1));
        files.push(output("v.mp4", 1));
        files.push(output("s.mp3", 1));
        files.push(output("report.xlsx", 1));
        files.push(output("last.webp", 1));

        let kinds = plan_deliveries(files)
            .iter()
            .map(|delivery| match delivery {
                Delivery::Photos(files) => format!("photos:{}", files.len()),
                Delivery::Animation(file) => format!("animation:{}", file.path),
                Delivery::Video(file) => format!("video:{}", file.path),
                Delivery::Audio(file) => format!("audio:{}", file.path),
                Delivery::Document(file) => format!("document:{}", file.path),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            vec![
                "photos:10",
                "photos:2",
                "document:big.jpg",
                "animation:a.gif",
                "video:v.mp4",
                "audio:s.mp3",
                "document:report.xlsx",
                "photos:1",
            ]
        );
    }

    #[test]
    fn file_names_are_plain_unique_and_bounded() {
        assert_eq!(
            sanitize_file_name("../../etc/.passwd").as_deref(),
            Some("passwd")
        );
        assert_eq!(
            sanitize_file_name("C:\\tmp\\a.csv").as_deref(),
            Some("a.csv")
        );
        assert_eq!(sanitize_file_name(" .. ").as_deref(), None);
        let long = format!("{}.xlsx", "я".repeat(100));
        let bounded = sanitize_file_name(&long).expect("name");
        assert!(bounded.len() <= MAX_FILE_NAME_BYTES);
        assert!(bounded.ends_with(".xlsx"));

        let mut used = BTreeSet::new();
        assert_eq!(unique_name("a.csv".to_owned(), &mut used), "a.csv");
        assert_eq!(unique_name("a.csv".to_owned(), &mut used), "a_2.csv");
        assert_eq!(unique_name("a.csv".to_owned(), &mut used), "a_3.csv");

        let unnamed = ChatFile {
            kind: "voice".to_owned(),
            extension: "oga".to_owned(),
            ..ChatFile::default()
        };
        assert_eq!(input_file_name(&unnamed, 0), "voice_1.oga");
    }

    #[test]
    fn output_is_clipped_for_the_model() {
        let response = RunResponse {
            termination: Termination::Exited,
            exit_code: Some(0),
            stdout: "x".repeat(STDOUT_MAX_CHARS + 5),
            stderr: format!("{}tail", "y".repeat(STDERR_MAX_CHARS)),
            ..RunResponse::default()
        };
        let data = run_result(&response, Vec::new(), DeliveryReport::default(), Vec::new())
            .data
            .expect("data");
        assert_eq!(
            data["stdout"].as_str().expect("stdout").len(),
            STDOUT_MAX_CHARS
        );
        assert_eq!(data["stdout_truncated"], true);
        assert!(data["stderr"].as_str().expect("stderr").ends_with("tail"));
        assert_eq!(data["stderr_truncated"], true);
    }

    #[test]
    fn session_keys_follow_chat_threads() {
        let mut context = context();
        assert_eq!(session_key(&context), "c-10042_t7");
        context.thread_id = None;
        assert_eq!(session_key(&context), "c-10042");
        assert!(openplotva_sandbox::api::is_valid_session_key(&session_key(
            &context
        )));
    }
}
