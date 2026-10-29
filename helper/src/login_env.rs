//! What the account's shell files set up, which a command needs to find the
//! tools the user installed through them (nvm, pyenv, `~/.local/bin`). Read the
//! way desktop apps started with no terminal read it: by running the shell once
//! as Terminal does and printing the environment it ends with. Only the names
//! in `KEPT` are taken: shell files often export API keys, which must not reach
//! the agent unasked.

use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::account::Account;
use crate::process::{login_path, own_session};

/// The variables a command takes from the shell files, by name; a trailing `*`
/// takes every name that starts with what precedes it.
const KEPT: &[&str] = &[
    "PATH",
    "LANG",
    "LC_*",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NODE_EXTRA_CA_CERTS",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "all_proxy",
    "JAVA_HOME",
    "GOPATH",
    "GOROOT",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "PYENV_ROOT",
    "NVM_DIR",
    "PNPM_HOME",
    "BUN_INSTALL",
    "VOLTA_HOME",
    "HOMEBREW_PREFIX",
    "HOMEBREW_CELLAR",
    "HOMEBREW_REPOSITORY",
];

/// The files whose change can change what the shell sets up; `~/` is the
/// account's home.
const SHELL_FILES: &[&str] = &[
    "~/.zshenv",
    "~/.zprofile",
    "~/.zshrc",
    "~/.zlogin",
    "~/.bash_profile",
    "~/.bashrc",
    "~/.profile",
    "/etc/paths",
    "/etc/paths.d",
    "/etc/zshrc",
    "/etc/zprofile",
];

// A command waits on the read; the apps that do the same allow it this long.
const LIMIT: Duration = Duration::from_secs(5);
const CAP: usize = 1 << 20;

const BEGIN: &[u8] = b"__LA_ENV_BEGIN__";
const LINES: &[u8] = b"__LA_ENV_LINES__";
const END: &[u8] = b"__LA_ENV_END__";
// printf assembles the markers, so the script's own text never holds them
// should a shell file echo it. The environment is listed twice: NUL-separated,
// and one variable a line for the `env` of macOS 12, which has no `-0`. Two
// plain lists rather than a fallback, since fish has no `{ }` to group one in.
const SCRIPT: &str = "printf '__LA_ENV_%s__' BEGIN; /usr/bin/env -0 2>/dev/null; \
    printf '__LA_ENV_%s__' LINES; /usr/bin/env; printf '__LA_ENV_%s__' END";

pub type Vars = Arc<[(OsString, OsString)]>;
type Stamp = Vec<Option<SystemTime>>;

/// The account's shell environment, read for the first command and again
/// once a shell file changed. One read at a time: commands arriving during
/// one wait for it rather than start their own.
pub struct LoginEnv {
    account: Account,
    read: Mutex<Option<(Stamp, Vars)>>,
}

impl LoginEnv {
    pub fn new(account: Account) -> Self {
        Self {
            account,
            read: Mutex::new(None),
        }
    }

    pub async fn vars(&self) -> Vars {
        let mut read = self.read.lock().await;
        let stamp = stamp(&self.account.home);
        if let Some((at, vars)) = read.as_ref()
            && *at == stamp
        {
            return vars.clone();
        }
        let shell = self.account.shell.display();
        // A failure is kept too, until the files change: trying again would
        // hold up every command by the limit.
        let vars: Vars = match capture(&self.account, LIMIT).await {
            Some(vars) => {
                info!(%shell, kept = vars.len(), "Read what the shell files set");
                vars.into()
            }
            None => {
                warn!(%shell, "Could not read what the shell files set; using a login's PATH");
                Vec::new().into()
            }
        };
        *read = Some((stamp, vars.clone()));
        vars
    }
}

/// The kept variables the account's shell ends with, run as Terminal runs it
/// and, failing that, as a plain login. None when neither prints them within
/// `limit`.
pub async fn capture(account: &Account, limit: Duration) -> Option<Vec<(OsString, OsString)>> {
    // One limit for both tries, since a command waits on them.
    let deadline = Instant::now() + limit;
    for flags in ["-ilc", "-lc"] {
        if Instant::now() >= deadline {
            break;
        }
        if let Some(vars) = run_shell(account, flags, deadline).await {
            return Some(vars.into_iter().filter(|(name, _)| kept(name)).collect());
        }
    }
    None
}

async fn run_shell(
    account: &Account,
    flags: &str,
    deadline: Instant,
) -> Option<Vec<(OsString, OsString)>> {
    let mut cmd = Command::new(&account.shell);
    cmd.arg(flags)
        .arg(SCRIPT)
        .env_clear()
        .env("USER", &account.name)
        .env("LOGNAME", &account.name)
        .env("HOME", &account.home)
        .env("SHELL", &account.shell)
        .env("PATH", login_path())
        .env("TERM", "dumb")
        .current_dir(if account.home.is_dir() {
            account.home.as_path()
        } else {
            "/".as_ref()
        })
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(tz) = std::env::var_os("TZ") {
        cmd.env("TZ", tz);
    }
    // With no terminal, an interactive shell has none to stop on.
    own_session(&mut cmd);
    let mut child = cmd.spawn().ok()?;
    let mut stdout = child.stdout.take().expect("piped");
    let mut out = Vec::new();
    let read = async {
        let mut chunk = [0; 1 << 14];
        // Up to the end marker rather than the pipe's end, which something
        // the shell files started in the background may hold open.
        while out.len() < CAP && listing(&out).is_none() {
            match stdout.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => out.extend_from_slice(&chunk[..n]),
            }
        }
    };
    let _ = tokio::time::timeout_at(deadline, read).await;
    // The shell was run for this alone, so it goes with all it started. Not
    // yet reaped, its pid still names its group.
    if let Some(pid) = child.id() {
        unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
    }
    let _ = child.wait().await;
    parse(&out)
}

/// The `NAME=value` pairs between the markers, from the NUL-separated
/// listing or, where `env` printed none, the one a line, which cuts a value at
/// any newline it holds; whatever the shell files print around them is
/// ignored. A login always has a PATH, so a listing without one did not run.
fn parse(out: &[u8]) -> Option<Vec<(OsString, OsString)>> {
    let listing = listing(out)?;
    let lines_at = find(listing, LINES)?;
    pairs(&listing[..lines_at], 0).or_else(|| pairs(&listing[lines_at + LINES.len()..], b'\n'))
}

fn pairs(listing: &[u8], separator: u8) -> Option<Vec<(OsString, OsString)>> {
    let vars: Vec<(OsString, OsString)> = listing
        .split(|&b| b == separator)
        .filter_map(|pair| {
            let eq = pair.iter().position(|&b| b == b'=')?;
            let (name, value) = (&pair[..eq], &pair[eq + 1..]);
            Some((
                OsStr::from_bytes(name).into(),
                OsStr::from_bytes(value).into(),
            ))
        })
        .collect();
    vars.iter().any(|(name, _)| name == "PATH").then_some(vars)
}

/// What the shell printed between the markers, once it printed the end one.
fn listing(out: &[u8]) -> Option<&[u8]> {
    let start = find(out, BEGIN)? + BEGIN.len();
    let len = find(&out[start..], END)?;
    Some(&out[start..start + len])
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn kept(name: &OsStr) -> bool {
    let name = name.as_bytes();
    KEPT.iter().any(|kept| match kept.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix.as_bytes()),
        None => name == kept.as_bytes(),
    })
}

/// A command's PATH: the shell files' folders first, as in Terminal, then
/// `base`'s, so none a login has is lost; each folder once.
pub fn path(vars: &[(OsString, OsString)], base: &str) -> OsString {
    let captured = vars
        .iter()
        .find(|(name, _)| name == "PATH")
        .map(|(_, value)| value.as_os_str());
    let mut seen = HashSet::new();
    let dirs = captured
        .into_iter()
        .flat_map(std::env::split_paths)
        .chain(std::env::split_paths(base))
        // An empty entry would mean the current folder.
        .filter(|dir| !dir.as_os_str().is_empty() && seen.insert(dir.clone()));
    std::env::join_paths(dirs).unwrap_or_else(|_| base.into())
}

fn stamp(home: &Path) -> Stamp {
    SHELL_FILES
        .iter()
        .map(|file| match file.strip_prefix("~/") {
            Some(name) => home.join(name),
            None => PathBuf::from(file),
        })
        .map(|file| std::fs::metadata(file).and_then(|m| m.modified()).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::time::{Duration, Instant, SystemTime};

    use super::*;

    fn owned(vars: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        vars.iter()
            .map(|(n, v)| ((*n).into(), (*v).into()))
            .collect()
    }

    fn get<'a>(vars: &'a [(OsString, OsString)], name: &str) -> Option<&'a OsStr> {
        vars.iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_os_str())
    }

    /// An account whose shell is the system's `sh`, which as a login reads
    /// `profile` from the home folder made for it.
    fn account_with_profile(profile: &str) -> (tempfile::TempDir, Account) {
        let home = tempfile::tempdir().unwrap();
        fs::write(home.path().join(".profile"), profile).unwrap();
        let account = Account {
            name: "someone".into(),
            home: home.path().into(),
            shell: "/bin/sh".into(),
        };
        (home, account)
    }

    #[test]
    fn reads_only_between_the_markers() {
        let out = b"Welcome back!\n__LA_ENV_BEGIN__PATH=/bin\0B=x=y\0C=two\nlines\0\
            __LA_ENV_LINES__PATH=/bin\nB=x=y\nC=two\nlines\n__LA_ENV_END__bye\n";

        assert_eq!(
            parse(out),
            Some(owned(&[
                ("PATH", "/bin"),
                ("B", "x=y"),
                ("C", "two\nlines")
            ]))
        );
        assert_eq!(parse(b"no markers at all"), None);
        assert_eq!(parse(b"__LA_ENV_BEGIN__PATH=/bin\0cut short"), None);
    }

    #[test]
    fn reads_the_listing_one_a_line_where_env_has_no_nul_option() {
        let out = b"__LA_ENV_BEGIN____LA_ENV_LINES__PATH=/bin:/usr/bin\nLANG=C\nC=two\nlines\n__LA_ENV_END__";

        assert_eq!(
            parse(out),
            Some(owned(&[
                ("PATH", "/bin:/usr/bin"),
                ("LANG", "C"),
                ("C", "two")
            ]))
        );
    }

    // So that a shell that listed nothing falls back, and is logged, rather
    // than being kept as an environment with nothing in it.
    #[test]
    fn a_listing_without_a_path_is_no_listing() {
        assert_eq!(
            parse(b"__LA_ENV_BEGIN____LA_ENV_LINES____LA_ENV_END__"),
            None
        );
        assert_eq!(
            parse(b"__LA_ENV_BEGIN__LANG=C\0__LA_ENV_LINES__LANG=C\n__LA_ENV_END__"),
            None
        );
    }

    #[test]
    fn keeps_only_the_listed_names() {
        for name in [
            "PATH",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "https_proxy",
            "NVM_DIR",
        ] {
            assert!(kept(name.as_ref()), "{name}");
        }
        for name in [
            "OPENAI_API_KEY",
            "HOMEBREW_GITHUB_API_TOKEN",
            "MISE_GITHUB_TOKEN",
            "USER",
            "HOME",
            "LC",
        ] {
            assert!(!kept(name.as_ref()), "{name}");
        }
    }

    #[test]
    fn puts_the_shells_folders_first_each_once() {
        let vars = owned(&[("LANG", "C"), ("PATH", "/a:/usr/bin::/b:/a")]);

        assert_eq!(path(&vars, "/usr/bin:/bin"), "/a:/usr/bin:/b:/bin");
        assert_eq!(path(&[], "/usr/bin:/bin"), "/usr/bin:/bin");
    }

    #[tokio::test]
    async fn takes_what_the_shell_files_export_through_their_noise() {
        let (_home, account) = account_with_profile(
            "echo \"Welcome, $USER\"\n\
             trap 'echo goodbye' EXIT\n\
             export PATH=\"/from/profile:$PATH\" LC_ALL=C OPENAI_API_KEY=sk-1 \
             HOMEBREW_GITHUB_API_TOKEN=ghp-1 MISE_GITHUB_TOKEN=ghp-2\n",
        );

        let vars = capture(&account, Duration::from_secs(5)).await.unwrap();

        let path = get(&vars, "PATH").unwrap().to_str().unwrap();
        assert!(path.starts_with("/from/profile:"), "{path}");
        assert_eq!(get(&vars, "LC_ALL"), Some("C".as_ref()));
        assert!(vars.iter().all(|(name, _)| kept(name)));
    }

    #[tokio::test]
    async fn tries_a_plain_login_when_an_interactive_one_fails() {
        let (home, account) = account_with_profile(
            "case $- in *i*) echo i >> \"$HOME/tries\"; exit 1;; esac\n\
             echo l >> \"$HOME/tries\"\n\
             export LANG=from-plain\n",
        );

        let vars = capture(&account, Duration::from_secs(5)).await.unwrap();

        assert_eq!(get(&vars, "LANG"), Some("from-plain".as_ref()));
        assert_eq!(
            fs::read_to_string(home.path().join("tries")).unwrap(),
            "i\nl\n"
        );
    }

    #[tokio::test]
    async fn gives_up_on_a_shell_that_hangs_and_leaves_nothing_running() {
        let (home, account) = account_with_profile(
            "(while :; do echo . >> \"$HOME/beat\"; sleep 0.05; done) &\nsleep 30\n",
        );
        let limit = Duration::from_millis(500);

        let started = Instant::now();
        assert_eq!(capture(&account, limit).await, None);
        assert!(started.elapsed() < limit + Duration::from_secs(1));

        let beat = home.path().join("beat");
        let before = fs::metadata(&beat).unwrap().len();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(fs::metadata(&beat).unwrap().len(), before, "still beating");
    }

    #[tokio::test]
    async fn reads_again_only_once_a_shell_file_changed() {
        let (home, account) = account_with_profile("echo run >> \"$HOME/runs\"\n");
        let profile = home.path().join(".profile");
        let set_mtime = |at: SystemTime| {
            let file = File::options().write(true).open(&profile).unwrap();
            file.set_modified(at).unwrap();
        };
        let runs = || {
            fs::read_to_string(home.path().join("runs"))
                .unwrap()
                .lines()
                .count()
        };
        let earlier = SystemTime::now() - Duration::from_secs(3600);
        set_mtime(earlier);
        let env = LoginEnv::new(account);

        // Two commands at once share one read.
        tokio::join!(env.vars(), env.vars());
        env.vars().await;
        assert_eq!(runs(), 1);

        set_mtime(earlier + Duration::from_secs(60));
        env.vars().await;
        assert_eq!(runs(), 2);

        fs::write(home.path().join(".zshrc"), "").unwrap();
        env.vars().await;
        env.vars().await;
        assert_eq!(runs(), 3);
    }
}
