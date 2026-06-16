//! Logging and tracing setup for OpenPlotva.

pub mod secrets;

use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use openplotva_config::{DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE, ObservabilityConfig};
use serde_json::Value;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    EnvFilter, Layer,
    layer::Context,
    prelude::*,
    reload::{self, Handle},
};

static LOG_FILTER_RELOAD: OnceLock<Handle<EnvFilter, tracing_subscriber::Registry>> =
    OnceLock::new();

/// Runtime log entry stored for the diagnostic API.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeLogEntry {
    pub seq: u64,
    pub time: Option<String>,
    pub level: String,
    pub message: String,
    pub attrs: Option<Value>,
}

#[derive(Debug)]
pub struct RuntimeLogBuffer {
    inner: Mutex<RuntimeLogBufferInner>,
    next_seq: AtomicU64,
}

#[derive(Debug)]
struct RuntimeLogBufferInner {
    ring: Vec<Option<RuntimeLogEntry>>,
    write: usize,
    count: usize,
}

impl RuntimeLogBuffer {
    /// Create a bounded runtime log buffer.
    #[must_use]
    pub fn new(capacity: i32) -> Self {
        let capacity = if capacity > 0 {
            capacity as usize
        } else {
            DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE as usize
        };
        Self {
            inner: Mutex::new(RuntimeLogBufferInner {
                ring: vec![None; capacity],
                write: 0,
                count: 0,
            }),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Record one entry, assigning the next sequence number.
    pub fn record(&self, mut entry: RuntimeLogEntry) {
        entry.seq = self.next_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.ring.is_empty() {
            return;
        }
        let write = inner.write;
        inner.ring[write] = Some(entry);
        inner.write = (write + 1) % inner.ring.len();
        inner.count = inner.count.saturating_add(1).min(inner.ring.len());
    }

    #[must_use]
    pub fn logs(
        &self,
        after_seq: u64,
        limit: i32,
        level: &str,
        search: &str,
    ) -> Vec<RuntimeLogEntry> {
        let limit = if limit <= 0 { 50 } else { limit } as usize;
        let level = level.trim();
        let search = search.trim();
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.count == 0 || inner.ring.is_empty() {
            return Vec::new();
        }

        let start = if inner.write >= inner.count {
            inner.write - inner.count
        } else {
            inner.write + inner.ring.len() - inner.count
        };
        let mut out = Vec::with_capacity(limit.min(inner.count));
        for offset in 0..inner.count {
            let Some(entry) = inner.ring[(start + offset) % inner.ring.len()].as_ref() else {
                continue;
            };
            if accepts_log_entry(entry, after_seq, level, search) {
                out.push(entry.clone());
                if out.len() >= limit {
                    break;
                }
            }
        }
        out
    }

    /// Latest assigned sequence number.
    #[must_use]
    pub fn latest_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Relaxed)
    }
}

pub fn init(config: &ObservabilityConfig) -> Arc<RuntimeLogBuffer> {
    init_with_log_buffer_capacity(config, DEFAULT_RUNTIME_API_LOG_BUFFER_SIZE)
}

/// Initialize process-wide tracing with a caller-provided runtime log capacity.
pub fn init_with_log_buffer_capacity(
    config: &ObservabilityConfig,
    log_buffer_capacity: i32,
) -> Arc<RuntimeLogBuffer> {
    let filter = EnvFilter::try_new(&config.log_filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    let buffer = Arc::new(RuntimeLogBuffer::new(log_buffer_capacity));
    let capture_layer = RuntimeLogLayer {
        buffer: Arc::clone(&buffer),
    };

    let _ = tracing_subscriber::registry()
        .with(filter_layer)
        .with(capture_layer)
        .with(tracing_subscriber::fmt::layer().with_writer(RedactingMakeWriter))
        .try_init();
    let _ = LOG_FILTER_RELOAD.set(reload_handle);
    buffer
}

/// Update the process tracing filter for the admin log-level API.
pub fn set_log_level(level: &str) -> Result<(), String> {
    let filter = match level {
        "debug" => "openplotva=debug,tower_http=debug",
        "info" => "openplotva=info,tower_http=info",
        "warn" | "warning" => "openplotva=warn,tower_http=warn",
        "error" => "openplotva=error,tower_http=error",
        _ => return Err("invalid level".to_owned()),
    };
    let filter = EnvFilter::try_new(filter).map_err(|error| error.to_string())?;
    let handle = LOG_FILTER_RELOAD
        .get()
        .ok_or_else(|| "log filter reload handle is not configured".to_owned())?;
    handle.reload(filter).map_err(|error| error.to_string())
}

fn accepts_log_entry(entry: &RuntimeLogEntry, after_seq: u64, level: &str, search: &str) -> bool {
    if entry.seq <= after_seq {
        return false;
    }
    if !level.is_empty() && !entry.level.eq_ignore_ascii_case(level) {
        return false;
    }
    search.is_empty()
        || contains_fold(&entry.message, search)
        || contains_fold(&entry.level, search)
        || entry
            .attrs
            .as_ref()
            .and_then(Value::as_object)
            .is_some_and(|attrs| {
                attrs.iter().any(|(key, value)| {
                    contains_fold(key, search) || contains_fold(&value.to_string(), search)
                })
            })
}

fn contains_fold(value: &str, needle: &str) -> bool {
    value.to_lowercase().contains(&needle.to_lowercase())
}

#[derive(Clone)]
struct RuntimeLogLayer {
    buffer: Arc<RuntimeLogBuffer>,
}

impl<S> Layer<S> for RuntimeLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = RuntimeLogVisitor::default();
        event.record(&mut visitor);
        redact_json_strings(&mut visitor.attrs);
        let message = visitor
            .message
            .map(|message| secrets::redact(&message).into_owned())
            .unwrap_or_else(|| event.metadata().target().to_owned());
        self.buffer.record(RuntimeLogEntry {
            seq: 0,
            time: Some(system_time_millis_string()),
            level: event.metadata().level().as_str().to_ascii_lowercase(),
            message,
            attrs: if visitor.attrs.is_empty() {
                None
            } else {
                Some(Value::Object(visitor.attrs))
            },
        });
    }
}

#[derive(Default)]
struct RuntimeLogVisitor {
    message: Option<String>,
    attrs: serde_json::Map<String, Value>,
}

impl Visit for RuntimeLogVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, Value::String(value.to_owned()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, Value::Bool(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_value(field, Value::String(format!("{value:?}")));
    }
}

impl RuntimeLogVisitor {
    fn record_value(&mut self, field: &Field, value: Value) {
        if field.name() == "message" {
            self.message = Some(match value {
                Value::String(value) => value.trim_matches('"').to_owned(),
                other => other.to_string(),
            });
        } else {
            self.attrs.insert(field.name().to_owned(), value);
        }
    }
}

fn system_time_millis_string() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    millis.to_string()
}

/// `MakeWriter` that scrubs secrets from formatted log output before it reaches
/// the underlying sink (stdout).
struct RedactingMakeWriter;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RedactingMakeWriter {
    type Writer = RedactingWriter<std::io::Stdout>;

    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter::new(std::io::stdout())
    }
}

struct RedactingWriter<W> {
    inner: W,
}

impl<W: std::io::Write> RedactingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: std::io::Write> std::io::Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match std::str::from_utf8(buf) {
            Ok(text) => self.inner.write_all(secrets::redact(text).as_bytes())?,
            Err(_) => self.inner.write_all(buf)?,
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Redact secrets from string-valued entries of a runtime-log attribute map.
fn redact_json_strings(attrs: &mut serde_json::Map<String, Value>) {
    for value in attrs.values_mut() {
        if let Value::String(text) = value
            && let std::borrow::Cow::Owned(redacted) = secrets::redact(text)
        {
            *text = redacted;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RedactingWriter, RuntimeLogBuffer, RuntimeLogEntry, redact_json_strings};
    use serde_json::{Value, json};
    use std::io::Write;

    const SECRET_TAIL: &str = "AAFfake_value_1234567890abcdefghij";

    #[test]
    fn redacting_writer_masks_bot_token_and_reports_full_input() {
        let mut sink: Vec<u8> = Vec::new();
        let line =
            format!("GET https://api.telegram.org/file/bot345381228:{SECRET_TAIL}/photos/x.jpg\n");
        let written = {
            let mut writer = RedactingWriter::new(&mut sink);
            writer
                .write(line.as_bytes())
                .expect("writing to a vec never fails")
        };
        assert_eq!(written, line.len(), "reports full input consumed");
        let out = String::from_utf8(sink).expect("redacted output stays valid utf-8");
        assert!(!out.contains(SECRET_TAIL), "token tail masked: {out}");
        assert!(out.contains("bot345381228:"), "bot id kept: {out}");
    }

    #[test]
    fn runtime_log_layer_redacts_secret_from_captured_event() {
        use super::RuntimeLogLayer;
        use std::sync::Arc;
        use tracing_subscriber::prelude::*;

        let buffer = Arc::new(RuntimeLogBuffer::new(8));
        let layer = RuntimeLogLayer {
            buffer: Arc::clone(&buffer),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let url = format!("https://api.telegram.org/bot345381228:{SECRET_TAIL}/getMe");
            tracing::warn!(url = %url, "telegram send failed");
        });

        let captured = format!("{:?}", buffer.logs(0, 10, "", ""));
        assert!(
            !captured.contains(SECRET_TAIL),
            "captured runtime event must be redacted: {captured}"
        );
        assert!(
            captured.contains("bot345381228:"),
            "bot id kept in captured event: {captured}"
        );
    }

    #[test]
    fn redact_json_strings_masks_only_string_values() {
        let mut attrs = serde_json::Map::new();
        attrs.insert(
            "url".to_owned(),
            Value::String(format!(
                "https://api.telegram.org/bot345381228:{SECRET_TAIL}/getMe"
            )),
        );
        attrs.insert("count".to_owned(), json!(42));
        redact_json_strings(&mut attrs);
        assert!(
            !attrs["url"]
                .as_str()
                .expect("url attr is a string")
                .contains(SECRET_TAIL),
            "string attr masked: {:?}",
            attrs["url"]
        );
        assert_eq!(attrs["count"], json!(42), "numeric attr untouched");
    }

    #[test]
    fn runtime_log_buffer_matches_go_bounded_after_seq_and_filters() {
        let buffer = RuntimeLogBuffer::new(3);
        for (level, message) in [
            ("info", "one"),
            ("warn", "two"),
            ("error", "database failed"),
            ("info", "runtime ready"),
        ] {
            buffer.record(RuntimeLogEntry {
                seq: 0,
                time: None,
                level: level.to_owned(),
                message: message.to_owned(),
                attrs: None,
            });
        }

        let logs = buffer.logs(0, 10, "", "");
        assert_eq!(
            logs.iter()
                .map(|entry| entry.message.as_str())
                .collect::<Vec<_>>(),
            vec!["two", "database failed", "runtime ready"]
        );
        let after = logs[1].seq;
        assert_eq!(buffer.logs(after, 10, "", "")[0].message, "runtime ready");
        assert_eq!(
            buffer.logs(0, 10, "error", "")[0].message,
            "database failed"
        );
        assert_eq!(
            buffer.logs(0, 10, "", "RUNTIME")[0].message,
            "runtime ready"
        );
    }
}
