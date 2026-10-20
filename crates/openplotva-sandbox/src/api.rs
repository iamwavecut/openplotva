//! Wire types shared by the sandbox supervisor and its HTTP client.

use serde::{Deserialize, Serialize};

/// Longest accepted session key.
pub const MAX_SESSION_KEY_LEN: usize = 64;
/// Longest accepted path of a file inside a session home.
pub const MAX_FILE_PATH_LEN: usize = 255;

/// One file written into the session home before the code runs.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct InputFile {
    /// Path relative to the session home.
    pub path: String,
    /// File content, standard base64.
    pub content_base64: String,
}

/// Run one Python script inside a session.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunRequest {
    /// Full Python source.
    pub code: String,
    /// Files to write into the session home before the run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<InputFile>,
}

/// How the script ended.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Termination {
    /// The interpreter exited on its own; see `exit_code`.
    #[default]
    Exited,
    /// The per-call wall-clock limit killed the script.
    Timeout,
    /// A signal killed the script; with no timeout this is usually the memory limit.
    Killed,
    /// The sandbox could not start the script.
    SandboxError,
}

/// A regular file in the session home after the run.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileEntry {
    /// Path relative to the session home.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// True when this run created or modified the file.
    pub changed: bool,
}

/// Session state after the call.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionInfo {
    /// True when this call started a new, empty session.
    pub created: bool,
    /// Seconds until the session closes if no call arrives.
    pub idle_expires_in_seconds: u64,
    /// Seconds until the session closes regardless of activity.
    pub expires_in_seconds: u64,
}

/// Result of one run.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunResponse {
    pub termination: Termination,
    /// Process exit code when the interpreter exited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub stdout: String,
    #[serde(default)]
    pub stdout_truncated: bool,
    pub stderr: String,
    #[serde(default)]
    pub stderr_truncated: bool,
    pub duration_ms: u64,
    /// Files in the session home, changed ones first.
    #[serde(default)]
    pub files: Vec<FileEntry>,
    pub session: SessionInfo,
}

/// Error body of every non-2xx response.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorBody {
    /// Stable machine code, see the `ERROR_*` constants.
    pub code: String,
    pub message: String,
}

/// Every session slot is taken; retry later.
pub const ERROR_BUSY: &str = "busy";
/// The request is malformed or violates a limit.
pub const ERROR_INVALID_REQUEST: &str = "invalid_request";
/// The session does not exist or has expired.
pub const ERROR_SESSION_NOT_FOUND: &str = "session_not_found";
/// The file does not exist in the session home.
pub const ERROR_FILE_NOT_FOUND: &str = "file_not_found";
/// The file exceeds the download limit.
pub const ERROR_FILE_TOO_LARGE: &str = "file_too_large";
/// The supervisor failed.
pub const ERROR_INTERNAL: &str = "internal";

/// Whether `key` is a valid session key: 1–64 ASCII letters, digits, `-` or `_`.
#[must_use]
pub fn is_valid_session_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_SESSION_KEY_LEN
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Split a relative file path into its components, rejecting anything that
/// could leave the session home or name a hidden file.
#[must_use]
pub fn relative_path_components(path: &str) -> Option<Vec<&str>> {
    if path.is_empty() || path.len() > MAX_FILE_PATH_LEN || path.contains('\0') {
        return None;
    }
    if path.starts_with('/') || path.contains('\\') {
        return None;
    }
    let components = path.split('/').collect::<Vec<_>>();
    components
        .iter()
        .all(|component| !component.is_empty() && !component.starts_with('.'))
        .then_some(components)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_keys_allow_only_safe_ascii() {
        assert!(is_valid_session_key("chat-100123_t0"));
        assert!(!is_valid_session_key(""));
        assert!(!is_valid_session_key("chat/1"));
        assert!(!is_valid_session_key("чат"));
        assert!(!is_valid_session_key(&"a".repeat(MAX_SESSION_KEY_LEN + 1)));
    }

    #[test]
    fn relative_paths_reject_escapes_and_hidden_names() {
        assert_eq!(
            relative_path_components("out/chart.png"),
            Some(vec!["out", "chart.png"])
        );
        assert_eq!(
            relative_path_components("данные.csv"),
            Some(vec!["данные.csv"])
        );
        for bad in [
            "",
            "/etc/passwd",
            "../x",
            "a/../b",
            "./a",
            ".config/x",
            "a//b",
            "a/",
            "a\\b",
            "a\0b",
        ] {
            assert_eq!(relative_path_components(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn run_response_round_trips_with_snake_case_termination() {
        let response = RunResponse {
            termination: Termination::Timeout,
            stdout: "ok".to_owned(),
            ..RunResponse::default()
        };
        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["termination"], "timeout");
        assert!(json.get("exit_code").is_none());
        let back: RunResponse = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, response);
    }
}
