//! What one session channel runs, started and piped the way sshd does it for
//! a session with no pty: a shell with `-c`, in a session of its own, from a
//! clean environment, in the home folder.

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;

use bytes::Bytes;
use russh::server::{Handle, Msg};
use russh::{Channel, ChannelId, ChannelMsg, ChannelWriteHalf, Sig};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tracing::debug;

use crate::login_env;
use crate::server::Machine;

/// What a session channel asked to run.
pub enum Start {
    /// A command, through `sh`.
    Shell(Vec<u8>),
    /// The system's SFTP server, at this path.
    Sftp(PathBuf),
}

/// Starts what the channel asked for and pipes it through the channel.
/// `port` is the gateway's end of the dial, which the command sees as its
/// client's.
pub async fn run(
    channel: Channel<Msg>,
    handle: Handle,
    machine: Arc<Machine>,
    port: u32,
    start: Start,
) {
    let id = channel.id();
    let (rx, tx) = channel.split();
    // Before the command starts, as a login reads its shell files first.
    let vars = machine.login_env.vars().await;
    match spawn(start, &machine, &vars, port) {
        Ok(child) => pipe(child, rx, tx, handle, id).await,
        Err(e) => {
            // As a shell that cannot start its command: a message and 127.
            let _ = tx
                .extended_data_bytes(1, Bytes::from(format!("{e}\n")))
                .await;
            let _ = tx.exit_status(127).await;
            let _ = tx.eof().await;
            let _ = tx.close().await;
        }
    }
}

fn spawn(
    start: Start,
    machine: &Machine,
    vars: &[(OsString, OsString)],
    port: u32,
) -> io::Result<Child> {
    let account = &machine.account;
    let mut cmd = match start {
        // `sh`, where sshd would use the account's shell: zsh reads
        // `~/.zshenv` before every `-c` and fish its config, so what those
        // export would reach the agent past `KEPT`. The backend sends plain sh,
        // and `SHELL` still names the account's.
        Start::Shell(command) => {
            let mut cmd = Command::new("/bin/sh");
            cmd.arg0("sh").arg("-c").arg(OsStr::from_bytes(&command));
            cmd
        }
        Start::Sftp(server) => Command::new(server),
    };
    // The login arrives from this machine's own loopback, as it does when
    // `ssh -R` hands the gateway's dial to the local sshd, so a key line's
    // `from=` reads the same either way.
    cmd.env_clear()
        .env("USER", &account.name)
        .env("LOGNAME", &account.name)
        .env("HOME", &account.home)
        .env("SHELL", &account.shell)
        .env("MAIL", format!("/var/mail/{}", account.name))
        .env("SSH_CLIENT", format!("127.0.0.1 {port} 22"))
        .env("SSH_CONNECTION", format!("127.0.0.1 {port} 127.0.0.1 22"))
        // What the shell files set, which names none of the above.
        .envs(vars.iter().map(|(name, value)| (name, value)))
        .env("PATH", login_env::path(vars, &login_path()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(tz) = std::env::var_os("TZ") {
        cmd.env("TZ", tz);
    }
    // sshd falls back to / when the home folder is gone.
    cmd.current_dir(if account.home.is_dir() {
        account.home.as_path()
    } else {
        "/".as_ref()
    });
    // Its own session, so the command and what it starts are one group the
    // backend can signal, apart from this process.
    own_session(&mut cmd);
    cmd.spawn()
}

/// Has `cmd` lead a session of its own, with no controlling terminal.
pub(crate) fn own_session(cmd: &mut Command) {
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn login_path() -> String {
    mac_path(Path::new("/"))
}

// What sshd sets before the shell's own startup files run.
#[cfg(not(target_os = "macos"))]
pub(crate) fn login_path() -> String {
    "/usr/local/bin:/usr/bin:/bin:/usr/games".into()
}

/// The PATH Terminal has before the account's own startup files add to it:
/// Homebrew's folders first, where its installer has the shell put them,
/// then what `path_helper` reads from /etc/paths and /etc/paths.d, then
/// sshd's list. sshd's list alone leaves out Homebrew and `/usr/local/bin`,
/// so tools the user installed would not be found. Read for every command, so
/// a tool installed while the helper runs is found too.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn mac_path(root: &Path) -> String {
    let mut found: Vec<String> = Vec::new();
    if root.join("opt/homebrew/bin").is_dir() {
        found.extend(["/opt/homebrew/bin".into(), "/opt/homebrew/sbin".into()]);
    }
    let mut lists = vec![root.join("etc/paths")];
    if let Ok(entries) = std::fs::read_dir(root.join("etc/paths.d")) {
        let mut named: Vec<PathBuf> = entries.filter_map(|e| Some(e.ok()?.path())).collect();
        named.sort();
        lists.extend(named);
    }
    for list in lists {
        if let Ok(text) = std::fs::read_to_string(list) {
            found.extend(
                text.lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(Into::into),
            );
        }
    }
    found.extend(["/usr/bin", "/bin", "/usr/sbin", "/sbin"].map(Into::into));
    let mut seen = std::collections::HashSet::new();
    found.retain(|dir| seen.insert(dir.clone()));
    found.join(":")
}

async fn pipe(
    mut child: Child,
    mut rx: russh::ChannelReadHalf,
    tx: ChannelWriteHalf<Msg>,
    handle: Handle,
    id: ChannelId,
) {
    // Gone once the command is reaped, so a signal sent while its output
    // drains never reaches a process that reused the pid.
    let mut pid = child.id();
    let stdout = child.stdout.take().expect("piped");
    let stderr = child.stderr.take().expect("piped");
    // Input goes through a task of its own, so a command that is slow to
    // read never holds up a signal or the end of its output.
    let (feed, fed) = mpsc::unbounded_channel();
    tokio::spawn(feed_stdin(child.stdin.take().expect("piped"), fed));
    let mut feed = Some(feed);
    let output = async { tokio::join!(tx.data(stdout), tx.extended_data(1, stderr)) };
    tokio::pin!(output);
    let (mut drained, mut exited) = (false, false);
    while !(drained && exited) {
        tokio::select! {
            msg = rx.wait() => match msg {
                Some(ChannelMsg::Data { data }) => {
                    if let Some(feed) = &feed {
                        let _ = feed.send(data);
                    }
                }
                Some(ChannelMsg::Eof) => feed = None,
                Some(ChannelMsg::Signal { signal }) => signal_child(pid, &signal),
                // The client is gone. As sshd does without a pty, the command
                // is left to finish on its own; its output has nowhere to go.
                Some(ChannelMsg::Close) | None => return,
                Some(_) => {}
            },
            _ = &mut output, if !drained => {
                drained = true;
                let _ = tx.eof().await;
            }
            status = child.wait(), if !exited => {
                exited = true;
                pid = None;
                match status {
                    Ok(status) => report_exit(&tx, &handle, id, status).await,
                    Err(e) => debug!("waiting on the command failed: {e}"),
                }
            }
        }
    }
    let _ = tx.close().await;
}

async fn feed_stdin(mut stdin: ChildStdin, mut fed: mpsc::UnboundedReceiver<Bytes>) {
    while let Some(chunk) = fed.recv().await {
        if stdin.write_all(&chunk).await.is_err() {
            return;
        }
    }
}

async fn report_exit(
    tx: &ChannelWriteHalf<Msg>,
    handle: &Handle,
    id: ChannelId,
    status: ExitStatus,
) {
    if let Some(code) = status.code() {
        let _ = tx.exit_status(code as u32).await;
    } else if let Some(signo) = status.signal() {
        let _ = handle
            .exit_signal_request(
                id,
                sig(signo),
                status.core_dumped(),
                String::new(),
                String::new(),
            )
            .await;
    }
}

// The signals a command may be sent or end by, as named on the wire. The
// names russh has no variant for travel as `Custom`, both ways.
fn signals() -> [(libc::c_int, Sig); 13] {
    [
        (libc::SIGABRT, Sig::ABRT),
        (libc::SIGALRM, Sig::ALRM),
        (libc::SIGFPE, Sig::FPE),
        (libc::SIGHUP, Sig::HUP),
        (libc::SIGILL, Sig::ILL),
        (libc::SIGINT, Sig::INT),
        (libc::SIGKILL, Sig::KILL),
        (libc::SIGPIPE, Sig::PIPE),
        (libc::SIGQUIT, Sig::QUIT),
        (libc::SIGSEGV, Sig::SEGV),
        (libc::SIGTERM, Sig::TERM),
        (libc::SIGUSR1, Sig::USR1),
        (libc::SIGUSR2, Sig::Custom("USR2".into())),
    ]
}

fn sig(signo: libc::c_int) -> Sig {
    signals()
        .into_iter()
        .find(|(n, _)| *n == signo)
        .map_or_else(|| Sig::Custom(signo.to_string()), |(_, sig)| sig)
}

/// As sshd does, the signal goes to the command's own process only.
fn signal_child(pid: Option<u32>, signal: &Sig) {
    // Sig has no PartialEq.
    let same = |known: &Sig| match (known, signal) {
        (Sig::Custom(a), Sig::Custom(b)) => a == b,
        (a, b) => std::mem::discriminant(a) == std::mem::discriminant(b),
    };
    let Some((signo, _)) = signals().into_iter().find(|(_, known)| same(known)) else {
        return;
    };
    if let Some(pid) = pid {
        unsafe { libc::kill(pid as libc::pid_t, signo) };
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::mac_path;

    #[test]
    fn a_mac_login_finds_homebrew_and_what_installers_register() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("opt/homebrew/bin")).unwrap();
        fs::create_dir_all(root.path().join("etc/paths.d")).unwrap();
        fs::write(
            root.path().join("etc/paths"),
            "/usr/local/bin\n/usr/bin\n/bin\n/usr/sbin\n/sbin\n",
        )
        .unwrap();
        fs::write(root.path().join("etc/paths.d/20-b"), "/usr/bin\n/b/bin\n").unwrap();
        fs::write(root.path().join("etc/paths.d/10-a"), " /a/bin \n\n").unwrap();

        assert_eq!(
            mac_path(root.path()),
            "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/a/bin:/b/bin"
        );
    }

    #[test]
    fn a_mac_with_nothing_registered_keeps_sshds_list() {
        let root = tempfile::tempdir().unwrap();

        assert_eq!(mac_path(root.path()), "/usr/bin:/bin:/usr/sbin:/sbin");
    }
}
