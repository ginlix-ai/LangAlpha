//! The helper's log. With no terminal to read it, as when launchd starts it,
//! output goes to a file: launchd keeps no output of its own for an agent
//! unless its plist names a file, and a plist fixed at build time cannot name
//! one in the user's home. The app's plist names one beside its own logs,
//! outside the folder, which the app deletes when it undoes a pairing whose
//! failure points to the log.

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::IsTerminal;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::warn;
use tracing_subscriber::EnvFilter;

use crate::folder;

const LIMIT: u64 = 1 << 20;
const CHECK: Duration = Duration::from_secs(10);

pub fn init(dir: &Path, log: Option<PathBuf>) -> Result<()> {
    let terminal = std::io::stderr().is_terminal();
    if !terminal {
        let path = match log {
            Some(log) => folder::in_home(log)?,
            None => dir.join(folder::LOG),
        };
        if let Some(parent) = path.parent() {
            DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let file = redirect(&path)?;
        tokio::spawn(rotate(path, file));
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_ansi(terminal)
        .with_writer(std::io::stderr)
        .init();
    Ok(())
}

/// Sends this process's output to the end of the log.
fn redirect(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        if unsafe { libc::dup2(file.as_raw_fd(), fd) } < 0 {
            bail!("could not send output to {}", path.display());
        }
    }
    Ok(file)
}

/// Starts the log over once it passes its limit, keeping one old one. A
/// failure is only warned of: the log is no reason to stop serving.
async fn rotate(path: PathBuf, mut file: File) {
    loop {
        tokio::time::sleep(CHECK).await;
        if file.metadata().map_or(0, |m| m.len()) <= LIMIT {
            continue;
        }
        let rotated = fs::rename(&path, path.with_extension("log.1"))
            .map_err(anyhow::Error::from)
            .and_then(|()| redirect(&path));
        match rotated {
            Ok(new) => file = new,
            Err(e) => warn!("Could not start the log over: {e:#}"),
        }
    }
}
