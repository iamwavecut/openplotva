//! Session home directories and the file operations the supervisor performs in them.
//!
//! Sandboxed code controls the home tree, so every path is walked without
//! following symlinks. Callers hold the session run lock, so no jailed process
//! can change the tree during these operations.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::os::unix::fs::chown;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use thiserror::Error;
use tokio::process::Command;

use crate::api::{FileEntry, relative_path_components};

/// UID and GID the jailed process runs as.
pub const SANDBOX_UID: u32 = 65534;
const MAX_WALK_DEPTH: usize = 16;
const MAX_WALK_ENTRIES: usize = 5000;
const TMPFS_INODES: u32 = 4096;

/// How session homes are provisioned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HomeStorage {
    /// A dedicated tmpfs per session, owned by the sandbox user.
    Tmpfs { size_mib: u64 },
    /// A plain directory, for tests that run without privileges.
    PlainDir,
}

impl HomeStorage {
    fn owner(&self) -> Option<u32> {
        match self {
            Self::Tmpfs { .. } => Some(SANDBOX_UID),
            Self::PlainDir => None,
        }
    }

    /// Create an empty home at `dir` and seed it with the matplotlib font cache.
    pub async fn create(&self, dir: &Path, matplotlib_cache: Option<&Path>) -> io::Result<()> {
        tokio::fs::create_dir_all(dir).await?;
        if let Self::Tmpfs { size_mib } = self {
            let options = format!(
                "size={size_mib}m,nr_inodes={TMPFS_INODES},mode=0755,uid={SANDBOX_UID},gid={SANDBOX_UID},nosuid,nodev"
            );
            let status = Command::new("mount")
                .args(["-t", "tmpfs", "-o", &options, "tmpfs"])
                .arg(dir)
                .status()
                .await?;
            if !status.success() {
                return Err(io::Error::other(format!(
                    "mount tmpfs exited with {status}"
                )));
            }
        }
        if let Some(cache) = matplotlib_cache.filter(|cache| cache.is_dir()) {
            let owner = self.owner();
            let cache = cache.to_path_buf();
            let target = dir.join(".config").join("matplotlib");
            tokio::task::spawn_blocking(move || copy_tree(&cache, &target, owner))
                .await
                .map_err(io::Error::other)??;
        }
        Ok(())
    }

    /// Remove a home created by [`HomeStorage::create`].
    pub async fn destroy(&self, dir: &Path) {
        if matches!(self, Self::Tmpfs { .. }) {
            match Command::new("umount").arg("-l").arg(dir).status().await {
                Ok(status) if status.success() => {}
                Ok(status) => tracing::warn!(dir = %dir.display(), %status, "umount failed"),
                Err(error) => tracing::warn!(dir = %dir.display(), %error, "umount failed"),
            }
        }
        if let Err(error) = tokio::fs::remove_dir_all(dir).await
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %dir.display(), %error, "failed to remove session home");
        }
    }

    /// Write one input file, creating parent directories inside the home.
    pub fn write_file(&self, home: &Path, path: &str, content: &[u8]) -> Result<(), HomeFileError> {
        let components = relative_path_components(path).ok_or(HomeFileError::InvalidPath)?;
        let (name, parents) = components.split_last().ok_or(HomeFileError::InvalidPath)?;
        let mut current = home.to_path_buf();
        for component in parents {
            current.push(component);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => return Err(HomeFileError::NotADirectory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    fs::create_dir(&current)?;
                    self.set_owner(&current)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if !metadata.is_file() => return Err(HomeFileError::NotAFile),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::write(&current, content)?;
        self.set_owner(&current)
    }

    fn set_owner(&self, path: &Path) -> Result<(), HomeFileError> {
        if let Some(uid) = self.owner() {
            chown(path, Some(uid), Some(uid))?;
        }
        Ok(())
    }
}

/// Error from a file operation inside a session home.
#[derive(Debug, Error)]
pub enum HomeFileError {
    #[error("path must be relative, without '..' or hidden components")]
    InvalidPath,
    #[error("a path component is not a directory")]
    NotADirectory,
    #[error("the path is not a regular file")]
    NotAFile,
    #[error("file not found")]
    NotFound,
    #[error("file is {size} bytes, more than the {limit}-byte limit")]
    TooLarge { size: u64, limit: u64 },
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Read one regular file without following symlinks.
pub fn read_file(home: &Path, path: &str, limit: u64) -> Result<Vec<u8>, HomeFileError> {
    let components = relative_path_components(path).ok_or(HomeFileError::InvalidPath)?;
    let (name, parents) = components.split_last().ok_or(HomeFileError::InvalidPath)?;
    let mut current = home.to_path_buf();
    for component in parents {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(HomeFileError::NotFound),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(HomeFileError::NotFound);
            }
            Err(error) => return Err(error.into()),
        }
    }
    current.push(name);
    let metadata = match fs::symlink_metadata(&current) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return Err(HomeFileError::NotFound),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(HomeFileError::NotFound);
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.len() > limit {
        return Err(HomeFileError::TooLarge {
            size: metadata.len(),
            limit,
        });
    }
    Ok(fs::read(&current)?)
}

/// Size and modification time of every visible regular file, keyed by relative path.
pub type HomeSnapshot = BTreeMap<String, (u64, Option<SystemTime>)>;

/// Walk the visible part of a home: hidden names and symlinks are skipped.
#[must_use]
pub fn snapshot(home: &Path) -> HomeSnapshot {
    let mut files = HomeSnapshot::new();
    let mut stack = vec![(home.to_path_buf(), String::new(), 0_usize)];
    let mut visited = 0_usize;
    while let Some((dir, prefix, depth)) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            visited += 1;
            if visited > MAX_WALK_ENTRIES {
                return files;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
                continue;
            };
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if metadata.is_dir() && depth < MAX_WALK_DEPTH {
                stack.push((entry.path(), relative, depth + 1));
            } else if metadata.is_file() {
                files.insert(relative, (metadata.len(), metadata.modified().ok()));
            }
        }
    }
    files
}

/// List files after a run: changed files first, then the rest, each group by path.
#[must_use]
pub fn file_entries(before: &HomeSnapshot, after: &HomeSnapshot, limit: usize) -> Vec<FileEntry> {
    let (mut changed, mut unchanged): (Vec<_>, Vec<_>) = after
        .iter()
        .map(|(path, state)| FileEntry {
            path: path.clone(),
            size: state.0,
            changed: before.get(path) != Some(state),
        })
        .partition(|entry| entry.changed);
    changed.append(&mut unchanged);
    changed.truncate(limit);
    changed
}

fn copy_tree(source: &Path, target: &Path, owner: Option<u32>) -> io::Result<()> {
    let mut created = Vec::<PathBuf>::new();
    let mut stack = vec![(source.to_path_buf(), target.to_path_buf())];
    while let Some((from, to)) = stack.pop() {
        fs::create_dir_all(&to)?;
        created.push(to.clone());
        for entry in fs::read_dir(&from)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let destination = to.join(entry.file_name());
            if file_type.is_dir() {
                stack.push((entry.path(), destination));
            } else if file_type.is_file() {
                fs::copy(entry.path(), &destination)?;
                created.push(destination);
            }
        }
    }
    if let Some(uid) = owner {
        if let Some(parent) = target.parent() {
            chown(parent, Some(uid), Some(uid))?;
        }
        for path in created {
            chown(&path, Some(uid), Some(uid))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;

    fn temp_home(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "openplotva-sandbox-home-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create temp home");
        dir
    }

    #[test]
    fn writes_nested_files_and_reads_them_back() {
        let home = temp_home("write");
        let storage = HomeStorage::PlainDir;
        storage
            .write_file(&home, "data/табл.csv", b"a,b\n1,2\n")
            .expect("write");
        assert_eq!(
            read_file(&home, "data/табл.csv", 1024).expect("read"),
            b"a,b\n1,2\n"
        );
        assert!(matches!(
            read_file(&home, "data/табл.csv", 3),
            Err(HomeFileError::TooLarge { size: 8, limit: 3 })
        ));
        fs::remove_dir_all(home).expect("cleanup");
    }

    #[test]
    fn never_follows_symlinks_planted_by_sandboxed_code() {
        let home = temp_home("symlink");
        let outside = temp_home("outside");
        fs::write(outside.join("secret"), b"secret").expect("secret");
        symlink(outside.join("secret"), home.join("leak.txt")).expect("file link");
        symlink(&outside, home.join("dir")).expect("dir link");

        assert!(matches!(
            read_file(&home, "leak.txt", 1024),
            Err(HomeFileError::NotFound)
        ));
        assert!(matches!(
            read_file(&home, "dir/secret", 1024),
            Err(HomeFileError::NotFound)
        ));
        let storage = HomeStorage::PlainDir;
        assert!(matches!(
            storage.write_file(&home, "leak.txt", b"x"),
            Err(HomeFileError::NotAFile)
        ));
        assert!(matches!(
            storage.write_file(&home, "dir/new.txt", b"x"),
            Err(HomeFileError::NotADirectory)
        ));
        assert_eq!(fs::read(outside.join("secret")).expect("intact"), b"secret");
        assert!(!snapshot(&home).contains_key("leak.txt"));

        fs::remove_dir_all(home).expect("cleanup");
        fs::remove_dir_all(outside).expect("cleanup");
    }

    #[test]
    fn listing_puts_changed_files_first_and_hides_dotfiles() {
        let home = temp_home("listing");
        fs::write(home.join("old.txt"), b"old").expect("old");
        fs::create_dir_all(home.join(".config")).expect("hidden");
        fs::write(home.join(".config").join("x"), b"x").expect("hidden file");
        let before = snapshot(&home);
        fs::create_dir_all(home.join("out")).expect("out");
        fs::write(home.join("out").join("chart.png"), b"png").expect("chart");

        let entries = file_entries(&before, &snapshot(&home), 10);
        assert_eq!(
            entries,
            vec![
                FileEntry {
                    path: "out/chart.png".to_owned(),
                    size: 3,
                    changed: true,
                },
                FileEntry {
                    path: "old.txt".to_owned(),
                    size: 3,
                    changed: false,
                },
            ]
        );
        assert_eq!(file_entries(&before, &snapshot(&home), 1).len(), 1);
        fs::remove_dir_all(home).expect("cleanup");
    }

    #[tokio::test]
    async fn plain_homes_get_the_matplotlib_cache() {
        let root = temp_home("seed");
        let cache = root.join("cache");
        fs::create_dir_all(&cache).expect("cache");
        fs::write(cache.join("fontlist-v390.json"), b"{}").expect("fontlist");
        let home = root.join("home");
        let storage = HomeStorage::PlainDir;
        storage.create(&home, Some(&cache)).await.expect("create");
        assert!(home.join(".config/matplotlib/fontlist-v390.json").is_file());
        assert!(snapshot(&home).is_empty());
        storage.destroy(&home).await;
        assert!(!home.exists());
        fs::remove_dir_all(root).expect("cleanup");
    }
}
