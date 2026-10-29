//! Makes way for an update. launchd keeps the helper it started running when
//! the app's bundle is swapped for a new one, and starts it again only once it
//! exits, so the old build would otherwise go on serving for weeks. The helper
//! watches its own executable and, once another file is in its place, exits
//! when nothing a restart would cut is under way.

use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tracing::warn;

/// What is under way that a restart would cut: commands, forwards and a
/// pairing. Not SFTP, whose channel the backend keeps open for as long as it
/// keeps the connection.
#[derive(Clone, Default)]
pub struct Busy(Arc<AtomicUsize>);

/// One thing under way, until dropped.
pub struct Hold(Arc<AtomicUsize>);

impl Busy {
    pub fn hold(&self) -> Hold {
        self.0.fetch_add(1, Ordering::SeqCst);
        Hold(self.0.clone())
    }

    pub fn in_flight(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The path this process was started from, and the file that was there.
pub struct Executable {
    path: PathBuf,
    file: Stamp,
}

// What tells one file at a path from another put there in its place.
type Stamp = (u64, u64, i64, i64, u64);

fn stamp(path: &Path) -> io::Result<Stamp> {
    let meta = fs::metadata(path)?;
    Ok((
        meta.dev(),
        meta.ino(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.len(),
    ))
}

impl Executable {
    /// This process's, or none where it cannot be told, which only means an
    /// update goes unnoticed.
    pub fn current() -> Option<Self> {
        let found = std::env::current_exe().and_then(Self::at);
        found
            .inspect_err(|e| warn!("Updates to this helper will go unnoticed: {e}"))
            .ok()
    }

    fn at(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            file: stamp(&path)?,
            path,
        })
    }

    /// Whether another file is at the path now. Not while none is, or it
    /// cannot be looked at: a bundle swap leaves nothing there for a moment,
    /// and the person may have moved the app.
    fn replaced(&self) -> bool {
        stamp(&self.path).is_ok_and(|now| now != self.file)
    }
}

/// Until it is time to exit for launchd to start the build an update put in
/// this one's place: once the executable, looked at every `check`, was
/// replaced, as soon as nothing is under way, looked for every `poll`, or
/// `patience` later if something still is. Never, with no executable to watch.
pub async fn restart_due(
    executable: Option<Executable>,
    busy: &Busy,
    check: Duration,
    poll: Duration,
    patience: Duration,
) {
    let Some(executable) = executable else {
        return std::future::pending().await;
    };
    loop {
        tokio::time::sleep(check).await;
        if executable.replaced() {
            break;
        }
    }
    idle(busy, poll, patience).await;
}

/// Whether nothing was under way, looked for every `poll`, within `patience`.
pub async fn idle(busy: &Busy, poll: Duration, patience: Duration) -> bool {
    let idle = async {
        while busy.in_flight() > 0 {
            tokio::time::sleep(poll).await;
        }
    };
    tokio::time::timeout(patience, idle).await.is_ok()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{Busy, Executable, restart_due};

    const MS: Duration = Duration::from_millis(1);
    const LONG: Duration = Duration::from_secs(10);

    fn helper() -> (TempDir, Executable) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("langalpha-helper");
        fs::write(&path, "old").unwrap();
        let executable = Executable::at(path).unwrap();
        (dir, executable)
    }

    // As an update does: the new build written beside the old, then moved
    // over it. The same length, so only the file itself differs.
    fn update(dir: &TempDir) {
        let new = dir.path().join("langalpha-helper.new");
        fs::write(&new, "new").unwrap();
        fs::rename(&new, dir.path().join("langalpha-helper")).unwrap();
    }

    #[test]
    fn a_build_moved_over_it_replaces_the_executable() {
        let (dir, executable) = helper();
        update(&dir);
        assert!(executable.replaced());
    }

    #[test]
    fn an_executable_left_alone_is_not_replaced() {
        let (_dir, executable) = helper();
        assert!(!executable.replaced());
    }

    #[test]
    fn an_executable_gone_from_its_path_is_not_replaced() {
        let (dir, executable) = helper();
        fs::remove_file(dir.path().join("langalpha-helper")).unwrap();
        assert!(!executable.replaced());
    }

    #[tokio::test]
    async fn the_restart_waits_until_nothing_is_under_way() {
        let (dir, executable) = helper();
        update(&dir);
        let busy = Busy::default();
        let hold = busy.hold();
        let due = restart_due(Some(executable), &busy, MS, MS, LONG);
        tokio::pin!(due);
        let early = tokio::time::timeout(Duration::from_millis(200), &mut due).await;
        assert!(early.is_err(), "due while something was under way");
        drop(hold);
        let due = tokio::time::timeout(LONG, due).await;
        assert!(due.is_ok(), "not due once nothing was under way");
    }

    #[tokio::test]
    async fn the_restart_comes_at_the_limit_even_while_busy() {
        let (dir, executable) = helper();
        update(&dir);
        let busy = Busy::default();
        let _hold = busy.hold();
        let due = restart_due(Some(executable), &busy, MS, MS, Duration::from_millis(50));
        assert!(
            tokio::time::timeout(LONG, due).await.is_ok(),
            "not due at the limit"
        );
        assert_eq!(busy.in_flight(), 1);
    }
}
