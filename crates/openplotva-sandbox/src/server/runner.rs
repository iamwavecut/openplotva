//! One script execution under nsjail.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::api::Termination;

/// Path of the script inside the jail.
pub const SCRIPT_PATH: &str = "/sandbox/main.py";

/// What one run produced.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunOutcome {
    pub termination: Termination,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stdout_truncated: bool,
    pub stderr: String,
    pub stderr_truncated: bool,
    pub duration: Duration,
}

/// Boxed future returned by [`CodeRunner::run`].
pub type RunFuture<'a> = Pin<Box<dyn Future<Output = RunOutcome> + Send + 'a>>;

/// Runs one script with `home` mounted as the working directory.
pub trait CodeRunner: Send + Sync + 'static {
    fn run<'a>(&'a self, home: &'a Path, code: &'a str) -> RunFuture<'a>;
}

/// Runs scripts through the nsjail binary and the sandbox config.
#[derive(Debug)]
pub struct NsjailRunner {
    nsjail: PathBuf,
    config: PathBuf,
    python: PathBuf,
    scratch: PathBuf,
    backstop: Duration,
    max_output_bytes: usize,
    counter: AtomicU64,
}

impl NsjailRunner {
    #[must_use]
    pub fn new(
        nsjail: PathBuf,
        config: PathBuf,
        python: PathBuf,
        scratch: PathBuf,
        backstop: Duration,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            nsjail,
            config,
            python,
            scratch,
            backstop,
            max_output_bytes,
            counter: AtomicU64::new(0),
        }
    }

    async fn run_inner(&self, home: &Path, code: &str) -> RunOutcome {
        let started = Instant::now();
        let call = self.counter.fetch_add(1, Ordering::Relaxed);
        let script = self.scratch.join(format!("{call}.py"));
        let log = self.scratch.join(format!("{call}.log"));
        if let Err(error) = tokio::fs::write(&script, code).await {
            return sandbox_error(started, format!("write script: {error}"));
        }
        let outcome = self.spawn(home, &script, &log, started).await;
        for path in [&script, &log] {
            if let Err(error) = tokio::fs::remove_file(path).await {
                tracing::debug!(path = %path.display(), %error, "failed to remove scratch file");
            }
        }
        outcome
    }

    async fn spawn(&self, home: &Path, script: &Path, log: &Path, started: Instant) -> RunOutcome {
        let mut command = Command::new(&self.nsjail);
        command
            .arg("--config")
            .arg(&self.config)
            .arg("--log")
            .arg(log)
            .arg("--bindmount")
            .arg(format!("{}:/home", home.display()))
            .arg("--bindmount_ro")
            .arg(format!("{}:{SCRIPT_PATH}", script.display()))
            .arg("--")
            .arg(&self.python)
            .arg(SCRIPT_PATH)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => return sandbox_error(started, format!("spawn nsjail: {error}")),
        };
        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            return sandbox_error(started, "nsjail pipes are missing".to_owned());
        };
        let limit = self.max_output_bytes;
        let stdout_task = tokio::spawn(read_capped(stdout, limit));
        let stderr_task = tokio::spawn(read_capped(stderr, limit));
        let status = tokio::time::timeout(self.backstop, child.wait()).await;
        let backstop_hit = status.is_err();
        if backstop_hit && let Err(error) = child.kill().await {
            tracing::warn!(%error, "failed to kill nsjail after the backstop timeout");
        }
        let (stdout, stdout_truncated) = stdout_task.await.unwrap_or_default();
        let (stderr, stderr_truncated) = stderr_task.await.unwrap_or_default();
        let log_text = tokio::fs::read_to_string(log).await.unwrap_or_default();
        let nsjail_code = match status {
            Ok(Ok(status)) => status.code(),
            Ok(Err(error)) => {
                return sandbox_error(started, format!("wait for nsjail: {error}"));
            }
            Err(_) => None,
        };
        let (termination, exit_code) = if backstop_hit {
            (Termination::Timeout, None)
        } else {
            classify(&log_text, nsjail_code)
        };
        if termination == Termination::SandboxError {
            tracing::error!(log = %error_lines(&log_text), "nsjail failed to run the script");
        }
        RunOutcome {
            termination,
            exit_code,
            stdout,
            stdout_truncated,
            stderr,
            stderr_truncated,
            duration: started.elapsed(),
        }
    }
}

impl CodeRunner for NsjailRunner {
    fn run<'a>(&'a self, home: &'a Path, code: &'a str) -> RunFuture<'a> {
        Box::pin(self.run_inner(home, code))
    }
}

fn sandbox_error(started: Instant, message: String) -> RunOutcome {
    tracing::error!(%message, "sandbox run failed");
    RunOutcome {
        termination: Termination::SandboxError,
        stderr: "The sandbox could not start the script.".to_owned(),
        duration: started.elapsed(),
        ..RunOutcome::default()
    }
}

/// Map the nsjail log and exit code to a termination.
///
/// The jailed interpreter runs as PID 1 of its namespace, so nsjail reports
/// its fate in the log: an exit status, a killing signal, or the time limit.
fn classify(log: &str, nsjail_code: Option<i32>) -> (Termination, Option<i32>) {
    if log.contains("run time >= time limit") {
        return (Termination::Timeout, None);
    }
    if log.contains("terminated with signal") {
        return (Termination::Killed, None);
    }
    if let Some(code) = log
        .lines()
        .rev()
        .find_map(|line| line.split("exited with status: ").nth(1))
        .and_then(|rest| rest.split(',').next())
        .and_then(|code| code.trim().parse::<i32>().ok())
    {
        return (Termination::Exited, Some(code));
    }
    match nsjail_code {
        Some(code) if code < 128 => (Termination::Exited, Some(code)),
        _ => (Termination::SandboxError, None),
    }
}

fn error_lines(log: &str) -> String {
    log.lines()
        .filter(|line| line.starts_with("[E]") || line.starts_with("[F]"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn read_capped(mut reader: impl AsyncRead + Unpin, limit: usize) -> (String, bool) {
    let mut kept = Vec::new();
    let mut truncated = false;
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let room = limit.saturating_sub(kept.len());
                if read > room {
                    truncated = true;
                }
                kept.extend_from_slice(&buffer[..read.min(room)]);
            }
        }
    }
    (String::from_utf8_lossy(&kept).into_owned(), truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXIT_3: &str = "[I][t] Executing '/py' for '[STANDALONE MODE]'\n\
        [I][t] pid=10 ([STANDALONE MODE]) exited with status: 3, (PIDs left: 0)\n";
    const TIMEOUT: &str = "[I][t] pid=16 run time >= time limit (30 >= 30) ([STANDALONE MODE]). Killing it\n\
        [I][t] pid=16 ([STANDALONE MODE]) terminated with signal: SIGKILL (9), (PIDs left: 0)\n";
    const OOM: &str =
        "[I][t] pid=22 ([STANDALONE MODE]) terminated with signal: SIGKILL (9), (PIDs left: 0)\n";
    const FATAL: &str = "[F][t] main():359 Couldn't prepare sandboxing policy\n";

    #[test]
    fn classifies_nsjail_outcomes_from_its_log() {
        assert_eq!(classify(EXIT_3, Some(3)), (Termination::Exited, Some(3)));
        assert_eq!(classify(TIMEOUT, Some(137)), (Termination::Timeout, None));
        assert_eq!(classify(OOM, Some(137)), (Termination::Killed, None));
        assert_eq!(
            classify(FATAL, Some(255)),
            (Termination::SandboxError, None)
        );
        assert_eq!(classify("", None), (Termination::SandboxError, None));
        assert_eq!(classify("", Some(0)), (Termination::Exited, Some(0)));
    }

    #[tokio::test]
    async fn output_is_capped_without_blocking_the_writer() {
        let data = vec![b'x'; 20_000];
        let (text, truncated) = read_capped(&data[..], 100).await;
        assert_eq!(text.len(), 100);
        assert!(truncated);
        let (text, truncated) = read_capped(&b"short"[..], 100).await;
        assert_eq!(text, "short");
        assert!(!truncated);
    }
}
