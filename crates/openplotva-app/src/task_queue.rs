//! App-owned durable task queue runtime for non-control taskman work.

use std::{
    collections::BTreeMap,
    fmt, fs,
    future::Future,
    io::{self, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use openplotva_config::PersistentQueueConfig;
use openplotva_taskman::{
    InMemoryTaskQueue, TaskQueueIdAllocator, TaskQueueSnapshot, TaskQueueSnapshotError,
    TaskQueueWalRecord, TaskQueueWalSink, decode_task_queue_snapshot, decode_task_queue_wal_record,
    empty_task_queue_snapshot, encode_task_queue_snapshot, encode_task_queue_wal_record,
    replay_task_queue_wal_records,
};
use openplotva_telegram::{
    DeleteMessageRequest, TelegramOutboundMethod, build_delete_message_method,
};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};

pub const DEFAULT_SHARED_TASK_QUEUE_SNAPSHOT_FILE: &str = "openplotva-task-queue.snap";

/// Periodic snapshot interval for the runtime shared queue.
pub const SHARED_TASK_QUEUE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);

/// Shutdown snapshot timeout for the runtime shared queue.
pub const SHARED_TASK_QUEUE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const SHARED_TASK_QUEUE_STUCK_SCAN_INTERVAL: Duration = Duration::from_secs(2 * 60);
pub const SHARED_TASK_QUEUE_STUCK_DURATION: Duration = Duration::from_secs(4 * 60 * 60);
/// Number of compacted shared task queue WAL archives retained for crash/audit inspection.
pub const SHARED_TASK_QUEUE_WAL_ARCHIVE_KEEP: usize = 3;

/// File-backed store for Rust-native shared task queue snapshots.
#[derive(Clone, Debug)]
pub struct SharedTaskQueueSnapshotFileStore {
    path: PathBuf,
}

/// Error returned by the shared task queue snapshot file store.
#[derive(Debug, Error)]
pub enum SharedTaskQueueSnapshotFileStoreError {
    /// Reading the snapshot file failed.
    #[error("read shared task queue snapshot {path}: {source}")]
    Read {
        /// Snapshot file path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Decoding a read snapshot failed.
    #[error("decode shared task queue snapshot {path}: {source}")]
    Decode {
        /// Snapshot file path.
        path: PathBuf,
        /// Underlying snapshot decode error.
        #[source]
        source: TaskQueueSnapshotError,
    },
    /// Creating the snapshot directory failed.
    #[error("create shared task queue snapshot directory {path}: {source}")]
    CreateDir {
        /// Snapshot directory path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Encoding the snapshot failed.
    #[error("encode shared task queue snapshot {path}: {source}")]
    Encode {
        /// Snapshot file path.
        path: PathBuf,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// Writing the temporary snapshot file failed.
    #[error("write shared task queue snapshot {path}: {source}")]
    Write {
        /// Temporary snapshot file path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Syncing the temporary snapshot file failed.
    #[error("sync shared task queue snapshot {path}: {source}")]
    Sync {
        /// Temporary snapshot file path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Installing the temporary snapshot file failed.
    #[error("install shared task queue snapshot {from} -> {to}: {source}")]
    Rename {
        /// Temporary snapshot path.
        from: PathBuf,
        /// Final snapshot path.
        to: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Reading or writing the Rust-native WAL failed during startup or journaling.
    #[error("shared task queue WAL {path}: {source}")]
    Wal {
        /// WAL file path.
        path: PathBuf,
        /// Underlying WAL error.
        #[source]
        source: SharedTaskQueueWalFileStoreError,
    },
}

/// File-backed append-only Rust-native WAL store for shared task queue mutations.
#[derive(Clone, Debug)]
pub struct SharedTaskQueueWalFileStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

/// Error returned by the shared task queue WAL file store.
#[derive(Debug, Error)]
pub enum SharedTaskQueueWalFileStoreError {
    /// Reading the WAL failed.
    #[error("read {path}: {source}")]
    Read {
        /// WAL path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Creating the WAL parent directory failed.
    #[error("create WAL directory {path}: {source}")]
    CreateDir {
        /// WAL parent path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Opening the WAL for append failed.
    #[error("open {path}: {source}")]
    Open {
        /// WAL path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Encoding a WAL line failed.
    #[error("encode {path}: {source}")]
    Encode {
        /// WAL path.
        path: PathBuf,
        /// Underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// Writing a WAL line failed.
    #[error("write {path}: {source}")]
    Write {
        /// WAL path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Flushing a WAL line failed.
    #[error("flush {path}: {source}")]
    Flush {
        /// WAL path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Syncing a compacted WAL failed.
    #[error("sync compacted WAL {path}: {source}")]
    Sync {
        /// WAL path.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Archiving a compacted WAL failed.
    #[error("archive compacted WAL {from} -> {to}: {source}")]
    Archive {
        /// WAL path before archive rotation.
        from: PathBuf,
        /// Archive path.
        to: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
}

impl SharedTaskQueueSnapshotFileStore {
    /// Build a snapshot store for a concrete file path.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Return the configured snapshot file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load the Rust-native snapshot from disk, returning `None` when it does not exist yet.
    pub fn load_snapshot(
        &self,
    ) -> Result<Option<TaskQueueSnapshot>, SharedTaskQueueSnapshotFileStoreError> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(SharedTaskQueueSnapshotFileStoreError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };

        decode_task_queue_snapshot(&bytes)
            .map(Some)
            .map_err(|source| SharedTaskQueueSnapshotFileStoreError::Decode {
                path: self.path.clone(),
                source,
            })
    }

    /// Save the Rust-native snapshot atomically through a sibling temporary file.
    pub fn save_snapshot(
        &self,
        snapshot: &TaskQueueSnapshot,
    ) -> Result<(), SharedTaskQueueSnapshotFileStoreError> {
        let bytes = encode_task_queue_snapshot(snapshot).map_err(|source| {
            SharedTaskQueueSnapshotFileStoreError::Encode {
                path: self.path.clone(),
                source,
            }
        })?;
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| {
                SharedTaskQueueSnapshotFileStoreError::CreateDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }

        let tmp_path = shared_task_queue_snapshot_tmp_path(&self.path);
        let mut file = fs::File::create(&tmp_path).map_err(|source| {
            SharedTaskQueueSnapshotFileStoreError::Write {
                path: tmp_path.clone(),
                source,
            }
        })?;
        file.write_all(&bytes)
            .map_err(|source| SharedTaskQueueSnapshotFileStoreError::Write {
                path: tmp_path.clone(),
                source,
            })?;
        file.sync_all()
            .map_err(|source| SharedTaskQueueSnapshotFileStoreError::Sync {
                path: tmp_path.clone(),
                source,
            })?;
        drop(file);

        fs::rename(&tmp_path, &self.path).map_err(|source| {
            SharedTaskQueueSnapshotFileStoreError::Rename {
                from: tmp_path,
                to: self.path.clone(),
                source,
            }
        })
    }
}

impl SharedTaskQueueWalFileStore {
    /// Build a WAL store next to the configured snapshot file.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Return the configured WAL path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load_records(
        &self,
    ) -> Result<Vec<TaskQueueWalRecord>, SharedTaskQueueWalFileStoreError> {
        let text = match fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(SharedTaskQueueWalFileStoreError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };

        let mut records = Vec::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let Ok(record) = decode_task_queue_wal_record(line.as_bytes()) else {
                break;
            };
            records.push(record);
        }
        Ok(records)
    }

    /// Append one WAL line and flush it so crash recovery is not tied to snapshot ticks.
    pub fn append_record(
        &self,
        record: &TaskQueueWalRecord,
    ) -> Result<(), SharedTaskQueueWalFileStoreError> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| {
                SharedTaskQueueWalFileStoreError::CreateDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        let bytes = encode_task_queue_wal_record(record).map_err(|source| {
            SharedTaskQueueWalFileStoreError::Encode {
                path: self.path.clone(),
                source,
            }
        })?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| SharedTaskQueueWalFileStoreError::Open {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .map_err(|source| SharedTaskQueueWalFileStoreError::Write {
                path: self.path.clone(),
                source,
            })?;
        file.flush()
            .map_err(|source| SharedTaskQueueWalFileStoreError::Flush {
                path: self.path.clone(),
                source,
            })
    }

    /// Truncate WAL after its mutations have been durably captured in the snapshot.
    pub fn truncate_after_snapshot(&self) -> Result<(), SharedTaskQueueWalFileStoreError> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| {
                SharedTaskQueueWalFileStoreError::CreateDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        let archive_path = match fs::metadata(&self.path) {
            Ok(metadata) if metadata.len() > 0 => {
                Some(shared_task_queue_wal_archive_path(&self.path))
            }
            Ok(_) => None,
            Err(source) if source.kind() == io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(SharedTaskQueueWalFileStoreError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        if let Some(archive_path) = archive_path {
            fs::rename(&self.path, &archive_path).map_err(|source| {
                SharedTaskQueueWalFileStoreError::Archive {
                    from: self.path.clone(),
                    to: archive_path.clone(),
                    source,
                }
            })?;
            prune_shared_task_queue_wal_archives(&self.path, SHARED_TASK_QUEUE_WAL_ARCHIVE_KEEP);
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|source| SharedTaskQueueWalFileStoreError::Open {
                path: self.path.clone(),
                source,
            })?;
        file.sync_all()
            .map_err(|source| SharedTaskQueueWalFileStoreError::Sync {
                path: self.path.clone(),
                source,
            })
    }
}

impl TaskQueueWalSink for SharedTaskQueueWalFileStore {
    fn append_task_queue_wal_record(&self, record: &TaskQueueWalRecord) {
        if let Err(error) = self.append_record(record) {
            tracing::warn!(%error, path = %self.path.display(), "failed to append shared task queue WAL");
        }
    }
}

/// Runtime-owned shared task queue plus its durable snapshot store.
#[derive(Clone, Debug)]
pub struct SharedTaskQueueRuntime {
    queue: Arc<InMemoryTaskQueue>,
    snapshots: SharedTaskQueueSnapshotFileStore,
    wal: SharedTaskQueueWalFileStore,
    persist_lock: Arc<Mutex<()>>,
}

/// Startup restore report for the shared task queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueRestoreReport {
    /// Number of records loaded from the snapshot.
    pub restored: usize,
    /// Number of Rust-native WAL lines replayed after the snapshot.
    pub wal_replayed: usize,
    /// Number of processing jobs reset to pending during startup.
    pub requeued: usize,
}

/// Periodic snapshot worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueSnapshotWorkerReport {
    /// Number of snapshot ticks observed.
    pub ticks: usize,
    /// Number of successful snapshot writes.
    pub saved: usize,
    /// Number of failed snapshot writes.
    pub errors: usize,
    /// Last snapshot write error, if any.
    pub last_error: Option<String>,
}

/// Periodic stale-processing recovery worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueRecoveryWorkerReport {
    /// Number of recovery ticks observed.
    pub ticks: usize,
    /// Number of processing jobs moved back to pending.
    pub requeued: usize,
    /// Number of successful snapshot writes after recovery.
    pub saved: usize,
    /// Number of failed snapshot writes after recovery.
    pub errors: usize,
    /// Last snapshot write error, if any.
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueTerminalCleanupWorkerReport {
    /// Number of cleanup ticks observed.
    pub ticks: usize,
    /// Number of terminal jobs deleted.
    pub deleted: usize,
    /// Number of successful snapshot writes after cleanup.
    pub saved: usize,
    /// Number of failed snapshot writes after cleanup.
    pub errors: usize,
    /// Last snapshot write error, if any.
    pub last_error: Option<String>,
}

/// Periodic worker-heartbeat report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueHeartbeatWorkerReport {
    /// Number of heartbeat ticks observed.
    pub ticks: usize,
    /// Number of worker heartbeat writes.
    pub heartbeats: usize,
}

/// Periodic stuck-processing cleanup worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueStuckCleanupWorkerReport {
    /// Number of cleanup ticks observed.
    pub ticks: usize,
    /// Number of processing jobs marked failed.
    pub failed: usize,
    /// Number of successful snapshot writes after cleanup.
    pub saved: usize,
    /// Number of failed snapshot writes after cleanup.
    pub errors: usize,
    /// Last snapshot write error, if any.
    pub last_error: Option<String>,
}

/// One placeholder cleanup pass report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueuePlaceholderCleanupReport {
    /// Number of stale placeholder rows found.
    pub found: usize,
    /// Number of Telegram delete requests attempted.
    pub attempted: usize,
    /// Number of taskman message rows removed from the snapshot queue.
    pub cleaned: usize,
    /// Number of Telegram delete requests that failed.
    pub delete_errors: usize,
    /// Number of successful snapshot writes after cleanup.
    pub saved: usize,
    /// Number of failed snapshot writes after cleanup.
    pub errors: usize,
    /// Last Telegram or snapshot write error, if any.
    pub last_error: Option<String>,
}

/// Periodic placeholder cleanup worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueuePlaceholderCleanupWorkerReport {
    /// Number of cleanup ticks observed.
    pub ticks: usize,
    /// Number of stale placeholder rows found.
    pub found: usize,
    /// Number of Telegram delete requests attempted.
    pub attempted: usize,
    /// Number of taskman message rows removed from the snapshot queue.
    pub cleaned: usize,
    /// Number of Telegram delete requests that failed.
    pub delete_errors: usize,
    /// Number of successful snapshot writes after cleanup.
    pub saved: usize,
    /// Number of failed snapshot writes after cleanup.
    pub errors: usize,
    /// Last Telegram or snapshot write error, if any.
    pub last_error: Option<String>,
}

/// Future returned by placeholder cleanup delete effects.
pub type PlaceholderDeleteFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Telegram boundary used by the shared taskman placeholder cleanup worker.
pub trait SharedTaskQueuePlaceholderDeleteEffects {
    /// Effect error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Delete one placeholder message from Telegram.
    fn delete_placeholder_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
    ) -> PlaceholderDeleteFuture<'a, Self::Error>;
}

impl SharedTaskQueuePlaceholderDeleteEffects for openplotva_telegram::TelegramClient {
    type Error = String;

    fn delete_placeholder_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
    ) -> PlaceholderDeleteFuture<'a, Self::Error> {
        Box::pin(async move {
            let method = build_delete_message_method(&DeleteMessageRequest {
                chat_id,
                message_id,
            })
            .map_err(|error| error.to_string())?;
            TelegramOutboundMethod::from(method)
                .execute_with(self)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }
}

impl SharedTaskQueueRuntime {
    /// Load a queue from the snapshot file, or start empty when no snapshot exists yet.
    pub fn load_or_new(
        snapshots: SharedTaskQueueSnapshotFileStore,
    ) -> Result<(Self, SharedTaskQueueRestoreReport), SharedTaskQueueSnapshotFileStoreError> {
        Self::load_or_new_inner(snapshots, None)
    }

    /// Load a queue using the shared runtime taskman ID allocator.
    pub fn load_or_new_with_id_allocator(
        snapshots: SharedTaskQueueSnapshotFileStore,
        ids: TaskQueueIdAllocator,
    ) -> Result<(Self, SharedTaskQueueRestoreReport), SharedTaskQueueSnapshotFileStoreError> {
        Self::load_or_new_inner(snapshots, Some(ids))
    }

    fn load_or_new_inner(
        snapshots: SharedTaskQueueSnapshotFileStore,
        ids: Option<TaskQueueIdAllocator>,
    ) -> Result<(Self, SharedTaskQueueRestoreReport), SharedTaskQueueSnapshotFileStoreError> {
        let wal = SharedTaskQueueWalFileStore::new(shared_task_queue_wal_path(snapshots.path()));
        let wal_records =
            wal.load_records()
                .map_err(|source| SharedTaskQueueSnapshotFileStoreError::Wal {
                    path: wal.path().to_path_buf(),
                    source,
                })?;
        let wal_replayed = wal_records.len();
        let snapshot = snapshots
            .load_snapshot()?
            .unwrap_or_else(empty_task_queue_snapshot);
        let snapshot = replay_task_queue_wal_records(snapshot, wal_records);
        let restored_before_requeue = snapshot.records.len();
        let journal: Arc<dyn TaskQueueWalSink> = Arc::new(wal.clone());
        let queue = Arc::new(match ids {
            Some(ids) => InMemoryTaskQueue::from_snapshot_with_id_allocator_and_journal(
                snapshot, ids, journal,
            ),
            None => InMemoryTaskQueue::from_snapshot_with_journal(snapshot, journal),
        });
        let requeued = queue.requeue_processing_for_startup();
        let mut report = SharedTaskQueueRestoreReport {
            restored: restored_before_requeue,
            wal_replayed,
            requeued,
        };
        let runtime = Self {
            queue,
            snapshots,
            wal,
            persist_lock: Arc::new(Mutex::new(())),
        };
        if report.requeued > 0 {
            runtime.persist_snapshot()?;
        }
        report.restored = runtime.queue.records().len();
        Ok((runtime, report))
    }

    /// Build an empty queue with the provided snapshot store.
    #[must_use]
    pub fn new_empty(snapshots: SharedTaskQueueSnapshotFileStore) -> Self {
        let wal = SharedTaskQueueWalFileStore::new(shared_task_queue_wal_path(snapshots.path()));
        let journal: Arc<dyn TaskQueueWalSink> = Arc::new(wal.clone());
        Self {
            queue: Arc::new(InMemoryTaskQueue::new_with_journal(journal)),
            snapshots,
            wal,
            persist_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Build an empty queue using the shared runtime taskman ID allocator.
    #[must_use]
    pub fn new_empty_with_id_allocator(
        snapshots: SharedTaskQueueSnapshotFileStore,
        ids: TaskQueueIdAllocator,
    ) -> Self {
        let wal = SharedTaskQueueWalFileStore::new(shared_task_queue_wal_path(snapshots.path()));
        let journal: Arc<dyn TaskQueueWalSink> = Arc::new(wal.clone());
        Self {
            queue: Arc::new(InMemoryTaskQueue::new_with_id_allocator_and_journal(
                ids, journal,
            )),
            snapshots,
            wal,
            persist_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Return the shared task queue.
    #[must_use]
    pub fn queue(&self) -> Arc<InMemoryTaskQueue> {
        Arc::clone(&self.queue)
    }

    /// Return the snapshot path.
    #[must_use]
    pub fn snapshot_path(&self) -> &Path {
        self.snapshots.path()
    }

    /// Return the Rust-native WAL path.
    #[must_use]
    pub fn wal_path(&self) -> &Path {
        self.wal.path()
    }

    /// Persist a point-in-time queue snapshot.
    pub fn persist_snapshot(&self) -> Result<(), SharedTaskQueueSnapshotFileStoreError> {
        let _guard = self
            .persist_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.queue.with_locked_snapshot(|snapshot| {
            self.snapshots.save_snapshot(snapshot)?;
            self.wal.truncate_after_snapshot().map_err(|source| {
                SharedTaskQueueSnapshotFileStoreError::Wal {
                    path: self.wal.path().to_path_buf(),
                    source,
                }
            })
        })
    }

    pub fn requeue_expired_processing(&self, now: OffsetDateTime) -> Vec<i64> {
        self.queue.requeue_expired_processing(now)
    }

    /// Update one worker heartbeat timestamp.
    pub fn update_worker_heartbeat(&self, worker_id: &str, at: OffsetDateTime) {
        self.queue.update_worker_heartbeat(worker_id, at);
    }

    pub fn fail_stuck_processing(&self, now: OffsetDateTime, stuck_duration: Duration) -> Vec<i64> {
        self.queue.fail_stuck_processing(now, stuck_duration)
    }

    pub fn prune_terminal_before(&self, cutoff: OffsetDateTime) -> Vec<i64> {
        self.queue.prune_terminal_before(cutoff)
    }
}

/// Default Rust-native snapshot path for the shared task queue.
#[must_use]
pub fn default_shared_task_queue_snapshot_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".plotva")
        .join(DEFAULT_SHARED_TASK_QUEUE_SNAPSHOT_FILE)
}

#[must_use]
pub fn shared_task_queue_snapshot_path_from_config(config: &PersistentQueueConfig) -> PathBuf {
    let configured = config.snapshot_path.trim();
    if configured.is_empty() {
        default_shared_task_queue_snapshot_path()
    } else {
        PathBuf::from(configured)
    }
}

#[must_use]
pub fn shared_task_queue_snapshot_interval_from_config(config: &PersistentQueueConfig) -> Duration {
    Duration::from_secs(config.snapshot_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_recovery_interval_from_config(config: &PersistentQueueConfig) -> Duration {
    Duration::from_secs(config.recovery_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_cleanup_interval_from_config(config: &PersistentQueueConfig) -> Duration {
    Duration::from_secs(config.cleanup_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_completed_retention_from_config(
    config: &PersistentQueueConfig,
) -> TimeDuration {
    TimeDuration::days(i64::from(config.completed_job_retention_days.max(0)))
}

#[must_use]
pub fn shared_task_queue_heartbeat_interval_from_config(
    config: &PersistentQueueConfig,
) -> Duration {
    Duration::from_secs(config.heartbeat_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_placeholder_cleanup_interval_from_config(
    config: &PersistentQueueConfig,
) -> Duration {
    Duration::from_secs(config.placeholder_cleanup_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_placeholder_max_age_from_config(
    config: &PersistentQueueConfig,
) -> Duration {
    Duration::from_secs(config.placeholder_max_age_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_worker_ids(worker_counts: &BTreeMap<String, i32>) -> Vec<String> {
    worker_counts
        .iter()
        .flat_map(|(queue_name, count)| {
            (0..(*count).max(0)).map(move |index| format!("{queue_name}-worker-{index}"))
        })
        .collect()
}

/// Persist the shared task queue periodically until shutdown is requested.
pub async fn run_shared_task_queue_snapshot_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueSnapshotWorkerReport {
    let mut report = SharedTaskQueueSnapshotWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                match runtime.persist_snapshot() {
                    Ok(()) => report.saved += 1,
                    Err(error) => {
                        report.errors += 1;
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, path = %runtime.snapshot_path().display(), "failed to persist shared task queue snapshot");
                    }
                }
            }
        }
    }

    report
}

/// Update shared taskman worker heartbeats periodically until shutdown is requested.
pub async fn run_shared_task_queue_heartbeat_worker_until(
    runtime: SharedTaskQueueRuntime,
    worker_ids: Vec<String>,
    interval: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueHeartbeatWorkerReport {
    let mut report = SharedTaskQueueHeartbeatWorkerReport::default();
    tokio::pin!(stop);
    if worker_ids.is_empty() {
        let _ = stop.await;
        return report;
    }

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                report.ticks += 1;
                let now = OffsetDateTime::now_utc();
                for worker_id in &worker_ids {
                    runtime.update_worker_heartbeat(worker_id, now);
                }
                report.heartbeats += worker_ids.len();
            }
        }
    }

    report
}

/// Requeue expired processing jobs periodically until shutdown is requested.
pub async fn run_shared_task_queue_recovery_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueRecoveryWorkerReport {
    let mut report = SharedTaskQueueRecoveryWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let requeued = runtime.requeue_expired_processing(OffsetDateTime::now_utc());
                if requeued.is_empty() {
                    continue;
                }
                report.requeued += requeued.len();
                tracing::warn!(?requeued, "requeued expired shared taskman processing jobs");
                match runtime.persist_snapshot() {
                    Ok(()) => report.saved += 1,
                    Err(error) => {
                        report.errors += 1;
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, path = %runtime.snapshot_path().display(), "failed to persist shared task queue snapshot after recovery");
                    }
                }
            }
        }
    }

    report
}

pub async fn run_shared_task_queue_terminal_cleanup_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    retention: TimeDuration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueTerminalCleanupWorkerReport {
    let mut report = SharedTaskQueueTerminalCleanupWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let cutoff = OffsetDateTime::now_utc() - retention;
                let deleted = runtime.prune_terminal_before(cutoff);
                if deleted.is_empty() {
                    continue;
                }
                report.deleted += deleted.len();
                tracing::warn!(deleted = deleted.len(), "deleted old shared taskman terminal jobs");
                match runtime.persist_snapshot() {
                    Ok(()) => report.saved += 1,
                    Err(error) => {
                        report.errors += 1;
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, path = %runtime.snapshot_path().display(), "failed to persist shared task queue snapshot after terminal cleanup");
                    }
                }
            }
        }
    }

    report
}

/// Mark stuck processing jobs failed periodically until shutdown is requested.
pub async fn run_shared_task_queue_stuck_cleanup_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    stuck_duration: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueStuckCleanupWorkerReport {
    let mut report = SharedTaskQueueStuckCleanupWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let failed = runtime.fail_stuck_processing(OffsetDateTime::now_utc(), stuck_duration);
                if failed.is_empty() {
                    continue;
                }
                report.failed += failed.len();
                tracing::warn!(?failed, "marked stuck shared taskman processing jobs failed");
                match runtime.persist_snapshot() {
                    Ok(()) => report.saved += 1,
                    Err(error) => {
                        report.errors += 1;
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, path = %runtime.snapshot_path().display(), "failed to persist shared task queue snapshot after stuck cleanup");
                    }
                }
            }
        }
    }

    report
}

/// Clean up stale placeholder rows once, deleting Telegram messages best-effort first.
pub async fn cleanup_shared_task_queue_placeholders_once<Effects>(
    runtime: &SharedTaskQueueRuntime,
    effects: &Effects,
    max_age: Duration,
    now: OffsetDateTime,
    per_delete_delay: Duration,
) -> SharedTaskQueuePlaceholderCleanupReport
where
    Effects: SharedTaskQueuePlaceholderDeleteEffects + Sync,
{
    let mut report = SharedTaskQueuePlaceholderCleanupReport::default();
    let stale = runtime.queue.stale_placeholder_messages(now, max_age);
    report.found = stale.len();

    for placeholder in stale {
        if !per_delete_delay.is_zero() {
            tokio::time::sleep(per_delete_delay).await;
        }
        report.attempted += 1;
        if let Err(error) = effects
            .delete_placeholder_message(placeholder.chat_id, i64::from(placeholder.message_id))
            .await
        {
            report.delete_errors += 1;
            report.last_error = Some(error.to_string());
            tracing::warn!(
                %error,
                job_id = placeholder.job_id,
                chat_id = placeholder.chat_id,
                message_id = placeholder.message_id,
                "failed to delete stale taskman placeholder from Telegram"
            );
        }
        if runtime.queue.delete_job_message(placeholder.id) {
            report.cleaned += 1;
        }
    }

    if report.cleaned > 0 {
        match runtime.persist_snapshot() {
            Ok(()) => report.saved += 1,
            Err(error) => {
                report.errors += 1;
                report.last_error = Some(error.to_string());
                tracing::warn!(%error, path = %runtime.snapshot_path().display(), "failed to persist shared task queue snapshot after placeholder cleanup");
            }
        }
    }

    report
}

/// Clean up stale placeholder rows periodically until shutdown is requested.
pub async fn run_shared_task_queue_placeholder_cleanup_worker_until<Effects>(
    runtime: SharedTaskQueueRuntime,
    effects: Effects,
    interval: Duration,
    max_age: Duration,
    per_delete_delay: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueuePlaceholderCleanupWorkerReport
where
    Effects: SharedTaskQueuePlaceholderDeleteEffects + Send + Sync,
{
    let mut report = SharedTaskQueuePlaceholderCleanupWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let pass = cleanup_shared_task_queue_placeholders_once(
                    &runtime,
                    &effects,
                    max_age,
                    OffsetDateTime::now_utc(),
                    per_delete_delay,
                ).await;
                report.found += pass.found;
                report.attempted += pass.attempted;
                report.cleaned += pass.cleaned;
                report.delete_errors += pass.delete_errors;
                report.saved += pass.saved;
                report.errors += pass.errors;
                if pass.last_error.is_some() {
                    report.last_error = pass.last_error;
                }
            }
        }
    }

    report
}

fn shared_task_queue_snapshot_tmp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| DEFAULT_SHARED_TASK_QUEUE_SNAPSHOT_FILE.into());
    path.with_file_name(format!("{file_name}.tmp"))
}

fn shared_task_queue_wal_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.wal", path.display()))
}

fn shared_task_queue_wal_archive_path(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let mut archive = PathBuf::from(format!("{}.archive.{nanos}", path.display()));
    for suffix in 1..100 {
        if !archive.exists() {
            return archive;
        }
        archive = PathBuf::from(format!("{}.archive.{nanos}.{suffix}", path.display()));
    }
    archive
}

fn prune_shared_task_queue_wal_archives(path: &Path, keep: usize) {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return;
    };
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let prefix = format!("{file_name}.archive.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    let mut archives: Vec<_> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|entry_path| {
            entry_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .collect();
    archives.sort();
    let remove_count = archives.len().saturating_sub(keep);
    for archive in archives.into_iter().take(remove_count) {
        if let Err(error) = fs::remove_file(&archive) {
            tracing::warn!(%error, path = %archive.display(), "failed to prune shared task queue WAL archive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_taskman::{
        DEFAULT_PRIORITY, DialogJobParams, MESSAGE_STATUS_COMPLETED, MESSAGE_STATUS_PLACEHOLDER,
        MESSAGE_TYPE_RESULT, TEXT_QUEUE_NAME, TaskQueueJobMessageParams, new_dialog_job_at,
    };
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    use time::OffsetDateTime;

    #[test]
    fn shared_task_queue_snapshot_path_uses_default_for_blank_go_config() {
        let config = persistent_queue_config("", 60);

        assert_eq!(
            shared_task_queue_snapshot_path_from_config(&config),
            default_shared_task_queue_snapshot_path()
        );
    }

    #[test]
    fn shared_task_queue_snapshot_path_uses_go_override_when_present() {
        let config = persistent_queue_config("/tmp/openplotva-custom-task-queue.snap", 60);

        assert_eq!(
            shared_task_queue_snapshot_path_from_config(&config),
            PathBuf::from("/tmp/openplotva-custom-task-queue.snap")
        );
    }

    #[test]
    fn shared_task_queue_snapshot_interval_uses_go_config_seconds() {
        let config = persistent_queue_config("", 12);

        assert_eq!(
            shared_task_queue_snapshot_interval_from_config(&config),
            Duration::from_secs(12)
        );
    }

    #[test]
    fn shared_task_queue_recovery_interval_uses_go_config_seconds() {
        let mut config = persistent_queue_config("", 60);
        config.recovery_interval_seconds = 9;

        assert_eq!(
            shared_task_queue_recovery_interval_from_config(&config),
            Duration::from_secs(9)
        );
    }

    #[test]
    fn shared_task_queue_heartbeat_config_and_worker_ids_match_go_shape() {
        let mut config = persistent_queue_config("", 60);
        config.heartbeat_interval_seconds = 17;
        let worker_counts = BTreeMap::from([
            ("image-regular".to_owned(), 1),
            ("image-vip".to_owned(), 2),
            ("music-vip".to_owned(), 0),
        ]);

        assert_eq!(
            shared_task_queue_heartbeat_interval_from_config(&config),
            Duration::from_secs(17)
        );
        assert_eq!(
            shared_task_queue_worker_ids(&worker_counts),
            vec![
                "image-regular-worker-0".to_owned(),
                "image-vip-worker-0".to_owned(),
                "image-vip-worker-1".to_owned(),
            ]
        );
    }

    #[test]
    fn shared_task_queue_runtime_updates_worker_heartbeat() {
        let path = unique_task_queue_snapshot_path("heartbeat");
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store);
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");

        runtime.update_worker_heartbeat("text-worker-0", now);

        assert_eq!(
            runtime.queue().worker_heartbeat_at("text-worker-0"),
            Some(now)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn shared_task_queue_placeholder_cleanup_config_uses_go_seconds() {
        let mut config = persistent_queue_config("", 60);
        config.placeholder_cleanup_interval_seconds = 11;
        config.placeholder_max_age_seconds = 22;

        assert_eq!(
            shared_task_queue_placeholder_cleanup_interval_from_config(&config),
            Duration::from_secs(11)
        );
        assert_eq!(
            shared_task_queue_placeholder_max_age_from_config(&config),
            Duration::from_secs(22)
        );
    }

    #[test]
    fn shared_task_queue_runtime_round_trips_snapshot_and_requeues_processing()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = unique_task_queue_snapshot_path("round-trip");
        let _ = std::fs::remove_file(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store.clone());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let first = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("first"), now),
        );
        let second = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("second"), now).with_priority(DEFAULT_PRIORITY + 1),
        );
        assert_eq!(first, 1);
        assert_eq!(second, 2);

        let work = runtime
            .queue()
            .dequeue(TEXT_QUEUE_NAME, "dialog", now)
            .expect("processing job");
        assert_eq!(work.id, second);
        runtime.persist_snapshot()?;

        let (restored, report) = SharedTaskQueueRuntime::load_or_new(store)?;
        assert_eq!(
            report,
            SharedTaskQueueRestoreReport {
                restored: 2,
                wal_replayed: 0,
                requeued: 1,
            }
        );
        let work = restored
            .queue()
            .dequeue(TEXT_QUEUE_NAME, "dialog", now)
            .expect("requeued processing job");
        assert_eq!(work.id, second);

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn shared_task_queue_runtime_recovers_expired_processing_and_persists()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = unique_task_queue_snapshot_path("recovery");
        let _ = std::fs::remove_file(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store.clone());
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let expired = start + time::Duration::seconds(61);
        let id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("stale"), start).with_processing_timeout_seconds(60),
        );
        runtime.queue().dequeue(TEXT_QUEUE_NAME, "dialog", start);

        assert_eq!(runtime.requeue_expired_processing(expired), vec![id]);
        runtime.persist_snapshot()?;
        let (restored, report) = SharedTaskQueueRuntime::load_or_new(store)?;

        assert_eq!(
            report,
            SharedTaskQueueRestoreReport {
                restored: 1,
                wal_replayed: 0,
                requeued: 0,
            }
        );
        assert_eq!(
            restored.queue().record(id).expect("job").status,
            openplotva_taskman::JobStatus::Pending
        );

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn shared_task_queue_runtime_fails_stuck_processing_and_persists()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = unique_task_queue_snapshot_path("stuck-cleanup");
        let _ = std::fs::remove_file(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store.clone());
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let stuck = start + time::Duration::hours(4) + time::Duration::seconds(1);
        let id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("stuck"), start),
        );
        runtime.queue().dequeue(TEXT_QUEUE_NAME, "dialog", start);

        assert_eq!(
            runtime.fail_stuck_processing(stuck, SHARED_TASK_QUEUE_STUCK_DURATION),
            vec![id]
        );
        runtime.persist_snapshot()?;
        let (restored, report) = SharedTaskQueueRuntime::load_or_new(store)?;

        assert_eq!(
            report,
            SharedTaskQueueRestoreReport {
                restored: 1,
                wal_replayed: 0,
                requeued: 0,
            }
        );
        let record = restored.queue().record(id).expect("job");
        assert_eq!(record.status, openplotva_taskman::JobStatus::Failed);
        assert_eq!(
            record.error.as_deref(),
            Some(openplotva_taskman::STUCK_JOB_ERROR_MESSAGE)
        );

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn shared_task_queue_snapshot_compacts_wal_after_save() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = unique_task_queue_snapshot_path("wal-compact");
        remove_task_queue_files(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store.clone());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job_id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("compact"), now),
        );
        runtime
            .queue()
            .dequeue(TEXT_QUEUE_NAME, "text-worker-0", now);
        assert!(runtime.wal_path().metadata()?.len() > 0);

        runtime.persist_snapshot()?;

        assert_eq!(runtime.wal_path().metadata()?.len(), 0);
        let archives = shared_task_queue_wal_archives(runtime.wal_path())?;
        assert_eq!(archives.len(), 1);
        assert!(archives[0].metadata()?.len() > 0);
        let (restored, report) = SharedTaskQueueRuntime::load_or_new(store)?;
        assert_eq!(
            report,
            SharedTaskQueueRestoreReport {
                restored: 1,
                wal_replayed: 0,
                requeued: 1,
            }
        );
        assert_eq!(
            restored.queue().record(job_id).expect("job").status,
            openplotva_taskman::JobStatus::Pending
        );

        remove_task_queue_files(&path);
        Ok(())
    }

    #[test]
    fn shared_task_queue_snapshot_prunes_old_wal_archives() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = unique_task_queue_snapshot_path("wal-archive-prune");
        remove_task_queue_files(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store);
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        for index in 0..(SHARED_TASK_QUEUE_WAL_ARCHIVE_KEEP + 2) {
            runtime.queue().assign(
                TEXT_QUEUE_NAME,
                new_dialog_job_at(dialog_params(&format!("archive-{index}")), now),
            );
            runtime.persist_snapshot()?;
        }

        let archives = shared_task_queue_wal_archives(runtime.wal_path())?;
        assert_eq!(archives.len(), SHARED_TASK_QUEUE_WAL_ARCHIVE_KEEP);
        assert_eq!(runtime.wal_path().metadata()?.len(), 0);

        remove_task_queue_files(&path);
        Ok(())
    }

    #[test]
    fn shared_task_queue_runtime_replays_wal_without_waiting_for_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = unique_task_queue_snapshot_path("wal-replay");
        remove_task_queue_files(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store.clone());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job_id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("wal"), now),
        );
        runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 100,
                message_id: 300,
                created_at: now,
                status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
            })?;
        runtime.queue().append_job_event(
            job_id,
            openplotva_taskman::TaskQueueJobEvent {
                stage: "provider".to_owned(),
                message: "started".to_owned(),
                ..openplotva_taskman::TaskQueueJobEvent::default()
            },
            now,
        )?;
        runtime
            .queue()
            .dequeue(TEXT_QUEUE_NAME, "text-worker-0", now);
        assert!(
            !path.exists(),
            "snapshot should not be needed for WAL recovery"
        );
        assert!(runtime.wal_path().exists());

        let (restored, report) = SharedTaskQueueRuntime::load_or_new(store)?;

        assert_eq!(
            report,
            SharedTaskQueueRestoreReport {
                restored: 1,
                wal_replayed: 4,
                requeued: 1,
            }
        );
        let record = restored.queue().record(job_id).expect("restored job");
        assert_eq!(record.status, openplotva_taskman::JobStatus::Pending);
        assert_eq!(record.messages.len(), 1);
        assert_eq!(record.events.len(), 1);
        assert_eq!(record.events[0].stage, "provider");

        remove_task_queue_files(&path);
        Ok(())
    }

    #[tokio::test]
    async fn shared_task_queue_placeholder_cleanup_deletes_and_persists_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = unique_task_queue_snapshot_path("placeholder-cleanup");
        let _ = std::fs::remove_file(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store.clone());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - time::Duration::seconds(121);
        let boundary = now - time::Duration::seconds(120);
        let job_id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("stale"), now),
        );
        let stale = runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 10,
                message_id: 103,
                created_at: old,
                status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
            })?;
        let boundary_placeholder =
            runtime
                .queue()
                .create_job_message(TaskQueueJobMessageParams {
                    job_id,
                    message_type: MESSAGE_TYPE_RESULT.to_owned(),
                    chat_id: 10,
                    message_id: 104,
                    created_at: boundary,
                    status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
                })?;
        let completed = runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 10,
                message_id: 105,
                created_at: old,
                status: MESSAGE_STATUS_COMPLETED.to_owned(),
            })?;
        let effects = PlaceholderEffectsStub::default();

        let report = cleanup_shared_task_queue_placeholders_once(
            &runtime,
            &effects,
            Duration::from_secs(120),
            now,
            Duration::ZERO,
        )
        .await;

        assert_eq!(
            report,
            SharedTaskQueuePlaceholderCleanupReport {
                found: 1,
                attempted: 1,
                cleaned: 1,
                saved: 1,
                ..SharedTaskQueuePlaceholderCleanupReport::default()
            }
        );
        assert_eq!(effects.deleted(), vec![(10, 103)]);
        let remaining = runtime
            .queue()
            .job_messages(job_id)
            .into_iter()
            .map(|message| message.id)
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![boundary_placeholder, completed]);
        assert!(!remaining.contains(&stale));

        let (restored, _report) = SharedTaskQueueRuntime::load_or_new(store)?;
        assert_eq!(restored.queue().job_messages(job_id).len(), 2);

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[tokio::test]
    async fn shared_task_queue_placeholder_cleanup_removes_row_after_delete_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = unique_task_queue_snapshot_path("placeholder-cleanup-error");
        let _ = std::fs::remove_file(&path);
        let store = SharedTaskQueueSnapshotFileStore::new(path.clone());
        let runtime = SharedTaskQueueRuntime::new_empty(store);
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - time::Duration::seconds(121);
        let job_id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("stale"), now),
        );
        runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 10,
                message_id: 103,
                created_at: old,
                status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
            })?;
        let effects = PlaceholderEffectsStub::failing("telegram unavailable");

        let report = cleanup_shared_task_queue_placeholders_once(
            &runtime,
            &effects,
            Duration::from_secs(120),
            now,
            Duration::ZERO,
        )
        .await;

        assert_eq!(report.found, 1);
        assert_eq!(report.attempted, 1);
        assert_eq!(report.cleaned, 1);
        assert_eq!(report.delete_errors, 1);
        assert!(runtime.queue().job_messages(job_id).is_empty());

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[derive(Clone, Default)]
    struct PlaceholderEffectsStub {
        deleted: Arc<Mutex<Vec<(i64, i64)>>>,
        error: Option<String>,
    }

    impl PlaceholderEffectsStub {
        fn failing(error: impl Into<String>) -> Self {
            Self {
                deleted: Arc::new(Mutex::new(Vec::new())),
                error: Some(error.into()),
            }
        }

        fn deleted(&self) -> Vec<(i64, i64)> {
            self.deleted
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl SharedTaskQueuePlaceholderDeleteEffects for PlaceholderEffectsStub {
        type Error = String;

        fn delete_placeholder_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i64,
        ) -> PlaceholderDeleteFuture<'a, Self::Error> {
            Box::pin(async move {
                self.deleted
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, message_id));
                match &self.error {
                    Some(error) => Err(error.clone()),
                    None => Ok(()),
                }
            })
        }
    }

    fn dialog_params(message_text: &str) -> DialogJobParams {
        DialogJobParams {
            chat_id: 100,
            message_id: 20,
            user_id: 7,
            user_full_name: "Wave Cut".to_owned(),
            message_text: message_text.to_owned(),
            original_text: message_text.to_owned(),
            meta: serde_json::json!({}),
            max_output_tokens: 0,
            thread_id: None,
        }
    }

    fn unique_task_queue_snapshot_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "openplotva-shared-task-queue-{label}-{}-{}.json",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    fn remove_task_queue_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let wal = shared_task_queue_wal_path(path);
        let _ = std::fs::remove_file(&wal);
        if let Ok(archives) = shared_task_queue_wal_archives(&wal) {
            for archive in archives {
                let _ = std::fs::remove_file(archive);
            }
        }
    }

    fn shared_task_queue_wal_archives(
        path: &Path,
    ) -> Result<Vec<std::path::PathBuf>, std::io::Error> {
        let Some(parent) = path.parent() else {
            return Ok(Vec::new());
        };
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return Ok(Vec::new());
        };
        let prefix = format!("{file_name}.archive.");
        let mut archives = Vec::new();
        for entry in std::fs::read_dir(parent)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
            {
                archives.push(path);
            }
        }
        archives.sort();
        Ok(archives)
    }

    fn persistent_queue_config(
        snapshot_path: impl Into<String>,
        snapshot_interval_seconds: i32,
    ) -> PersistentQueueConfig {
        PersistentQueueConfig {
            enabled: true,
            heartbeat_interval_seconds: 30,
            recovery_interval_seconds: 60,
            cleanup_interval_seconds: 300,
            default_processing_timeout_seconds: 300,
            max_retries: 3,
            completed_job_retention_days: 1,
            message_cleanup_interval_seconds: 300,
            job_message_cleanup_minutes: 30,
            control_workers: 2,
            text_workers: 4,
            dialog_aifarm_workers: 2,
            dialog_aifarm_fallback_workers: 1,
            dialog_aifarm_fallback_high_watermark: 30,
            dialog_aifarm_fallback_low_watermark: 20,
            dialog_aifarm_fallback_poll_interval_seconds: 1,
            image_regular_workers: 1,
            image_vip_workers: 1,
            music_vip_workers: 1,
            memory_consolidation_workers: 1,
            placeholder_cleanup_interval_seconds: 3600,
            placeholder_max_age_seconds: 7200,
            snapshot_path: snapshot_path.into(),
            snapshot_interval_seconds,
            llm_job_max_attempts: 5,
        }
    }
}
