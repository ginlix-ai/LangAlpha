//! Holds this machine's tunnel to a langalpha deployment and answers the
//! deployment's SSH logins over it itself, so the machine needs no sshd.
//!
//! To the deployment it is the same machine `tunnel.sh` sets up: the gateway
//! sees the tunnel's login and its `langalpha:22` forward, and the backend
//! logs in through it as it would to any SSH computer. Only the far end of
//! the forward differs, an SSH server in this process instead of the
//! machine's own.

mod account;
mod folder;
mod keys;
mod log;
mod login_env;
mod pair;
mod process;
mod server;
mod service;
#[cfg(test)]
mod tests;
mod tunnel;
mod update;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::folder::Request;

#[derive(Parser)]
#[command(version, about)]
enum Command {
    /// Asks to pair this machine with a deployment, then serves the folder
    /// as `run` does.
    Pair {
        /// The URL in the deployment's pairing command, ending in /script.
        url: String,
        #[arg(long)]
        dir: PathBuf,
    },
    /// Serves a folder: pairs it whenever a `pairing` file asks and holds its
    /// tunnel. What the login service runs.
    Run {
        /// The folder holding the pairing and this machine's own keys. A
        /// leading `~` is the account's home.
        #[arg(long)]
        dir: PathBuf,
        /// Where output goes with no terminal to show it, by default the
        /// folder's `helper.log`. A leading `~` is the account's home.
        #[arg(long)]
        log: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let (dir, url, log) = match Command::parse() {
        Command::Pair { url, dir } => (dir, Some(url), None),
        Command::Run { dir, log } => (dir, None, log),
    };
    let dir = folder::open(dir)?;
    log::init(&dir, log)?;
    let _lock = folder::lock(&dir)?;
    if let Some(url) = url {
        Request::write(&dir, &url)?;
    }
    service::run(&dir).await
}
