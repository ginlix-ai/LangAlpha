//! The folder the helper serves and every file in it. The app that installed
//! the helper writes `pairing` and reads `state`; the rest is the helper's
//! own. Its records are `key=value` lines, as the deployment's answers are,
//! and its keys are as OpenSSH writes them.

use std::fs::{self, DirBuilder, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use russh::keys::ssh_key::LineEnding;
use russh::keys::{Algorithm, HashAlg, PrivateKey, PublicKey};

use crate::account::Account;
use crate::keys;

const HOST_KEY: &str = "host_key";
const TUNNEL_KEY: &str = "id_ed25519";
const PAIRED: &str = "paired";
const REQUEST: &str = "pairing";
const TAKEN: &str = "pairing.taken";
const STATE: &str = "state";
const LOCK: &str = "lock";
const MACHINE: &str = "machine";
pub const LOG: &str = "helper.log";
// A pairing's failure is shown to the user as one line.
const DETAIL_LIMIT: usize = 500;

/// The folder named on the command line, with a leading `~` read as the
/// account's home: a login service's arguments are fixed when the app is
/// built, and launchd expands nothing in them.
pub fn open(dir: PathBuf) -> Result<PathBuf> {
    let dir = in_home(dir)?;
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    Ok(dir)
}

/// `path` with a leading `~` read as the account's home.
pub fn in_home(path: PathBuf) -> Result<PathBuf> {
    Ok(match path.strip_prefix("~") {
        Ok(rest) => Account::current()?.home.join(rest),
        Err(_) => path,
    })
}

/// Keeps the folder to this process until it exits, so two helpers never
/// claim the same request or hold the same tunnel.
pub fn lock(dir: &Path) -> Result<File> {
    let path = dir.join(LOCK);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => {
            bail!("another helper is already serving {}", dir.display())
        }
        Err(TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("locking {}", path.display()))
        }
    }
}

/// The value named `name` in `key=value` lines, if it is there and not empty.
/// Split at the first `=`, since a key line's options carry their own.
pub fn field<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix('='))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn required<'a>(text: &'a str, name: &str) -> Result<&'a str> {
    field(text, name).with_context(|| format!("no {name}"))
}

/// This machine's account and keys, read once at the start.
pub struct Identity {
    pub account: Account,
    /// What the backend pins on first contact.
    pub host_key: PrivateKey,
    /// What the gateway knows the tunnel by.
    pub tunnel_key: PrivateKey,
}

impl Identity {
    pub fn load(dir: &Path) -> Result<Self> {
        forget_another_machine(dir, this_machine().as_deref())?;
        Ok(Self {
            account: Account::current()?,
            host_key: own_key(&dir.join(HOST_KEY))?,
            tunnel_key: own_key(&dir.join(TUNNEL_KEY))?,
        })
    }

    /// The host key's fingerprint as the deployment shows it beside the
    /// computer.
    pub fn fingerprint(&self) -> String {
        self.host_key
            .public_key()
            .fingerprint(HashAlg::Sha256)
            .to_string()
    }

    pub fn host_key_line(&self) -> Result<String> {
        keys::public_line(&self.host_key)
    }

    pub fn tunnel_key_line(&self) -> Result<String> {
        keys::public_line(&self.tunnel_key)
    }
}

/// This machine's hardware UUID, as macOS shows it (`IOPlatformUUID`), or
/// Linux's machine id. None where neither can be read, and then no folder is
/// told apart from another machine's.
fn this_machine() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let mut id = [0u8; 16];
        let wait = libc::timespec {
            tv_sec: 5,
            tv_nsec: 0,
        };
        // SAFETY: `id` is the 16 bytes a uuid_t is, and `wait` outlives the call.
        if unsafe { libc::gethostuuid(id.as_mut_ptr(), &wait) } != 0 {
            return None;
        }
        let hex: String = id.iter().map(|b| format!("{b:02X}")).collect();
        Some(format!(
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..]
        ))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let id = fs::read_to_string("/etc/machine-id").ok()?;
        let id = id.trim();
        (!id.is_empty()).then(|| id.to_string())
    }
}

/// Drops the keys and pairing of a folder made on another machine, as one
/// that Migration Assistant or a backup restore carried over. Kept, they would
/// make this machine a second answer to the first one's tunnel and host key:
/// the deployment would take either for the other, and the app here would call
/// the first machine's computer this one. A folder that names no machine was
/// made by a helper from before this check, and is taken as this machine's.
fn forget_another_machine(dir: &Path, this: Option<&str>) -> Result<()> {
    let Some(this) = this else { return Ok(()) };
    let path = dir.join(MACHINE);
    match fs::read_to_string(&path) {
        Ok(made_on) if made_on.trim() == this => return Ok(()),
        Ok(made_on) => {
            tracing::warn!(
                made_on = made_on.trim(),
                "This folder was made on another machine; making this machine's own keys"
            );
            for name in [HOST_KEY, TUNNEL_KEY, PAIRED, STATE, TAKEN] {
                match fs::remove_file(dir.join(name)) {
                    Err(e) if e.kind() != ErrorKind::NotFound => {
                        return Err(e).with_context(|| format!("removing {name}"));
                    }
                    _ => {}
                }
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    }
    put(&path, &format!("{this}\n"))
}

/// A key of this machine's own, made the first time it is asked for. Both
/// outlive every restart: a new host key would fail the backend's pin, and a
/// new tunnel key would be a tunnel the gateway does not know.
fn own_key(path: &Path) -> Result<PrivateKey> {
    match fs::read_to_string(path) {
        Ok(text) => {
            PrivateKey::from_openssh(&text).with_context(|| format!("reading {}", path.display()))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
            put(path, &key.to_openssh(LineEnding::LF)?)?;
            Ok(key)
        }
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// What a pairing leaves: the tunnel the deployment set up and the key it logs
/// in with. One file, replaced whole and only once a pairing fully succeeded,
/// so a pairing that fails leaves the machine as it was.
#[derive(Clone, Debug, PartialEq)]
pub struct Paired {
    pub tunnel_id: String,
    pub gateway_host: String,
    pub gateway_port: u16,
    /// The gateway's `known_hosts` line, as the deployment gave it.
    pub gateway_pin: String,
    /// The deployment's `authorized_keys` line, as it gave it.
    pub authorized_key: String,
}

impl Paired {
    /// From the deployment's answer to `/tunnel`, whose words the record keeps,
    /// and the key line it logs in with.
    pub fn new(answer: &str, authorized_key: String) -> Result<Self> {
        let paired = Self {
            tunnel_id: required(answer, "tunnel_id")?.into(),
            gateway_host: required(answer, "gateway_host")?.into(),
            gateway_port: required(answer, "gateway_port")?
                .parse()
                .context("gateway_port")?,
            gateway_pin: required(answer, "known_hosts")?.into(),
            authorized_key,
        };
        // These go into a login and a host name, so nothing else may.
        for (name, value) in [
            ("tunnel_id", &paired.tunnel_id),
            ("gateway_host", &paired.gateway_host),
        ] {
            if !value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ".:-".contains(c))
            {
                bail!("{name} has characters it cannot use");
            }
        }
        paired.gateway_key()?;
        paired.authorized()?;
        Ok(paired)
    }

    /// The record the folder holds, if it was ever paired.
    pub fn load(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(PAIRED);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        Self::parse(&text)
            .map(Some)
            .with_context(|| format!("reading {}", path.display()))
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        put(&dir.join(PAIRED), &self.render())
    }

    fn parse(text: &str) -> Result<Self> {
        Self::new(text, required(text, "authorized_key")?.into())
    }

    fn render(&self) -> String {
        format!(
            "tunnel_id={}\ngateway_host={}\ngateway_port={}\nknown_hosts={}\nauthorized_key={}\n",
            self.tunnel_id,
            self.gateway_host,
            self.gateway_port,
            self.gateway_pin,
            self.authorized_key,
        )
    }

    pub fn gateway_key(&self) -> Result<PublicKey> {
        keys::key_in_line(&self.gateway_pin).context("the gateway's pin holds no key")
    }

    pub fn authorized(&self) -> Result<PublicKey> {
        keys::key_in_line(&self.authorized_key).context("the deployment's key line holds no key")
    }
}

/// The app's ask to pair, as it leaves it in `pairing`.
#[derive(Debug, PartialEq)]
pub struct Request {
    pub url: String,
    pub id: String,
}

/// A request that could not be read, by its id where it named a usable one.
pub struct Unreadable {
    pub id: String,
}

impl Request {
    /// Claims the request the app left, if any, by moving it aside. One the
    /// app leaves meanwhile is a new `pairing`, taken next, never one removed
    /// unread.
    pub fn claim(dir: &Path) -> Option<Result<Self, Unreadable>> {
        fs::rename(dir.join(REQUEST), dir.join(TAKEN)).ok()?;
        let text = fs::read_to_string(dir.join(TAKEN)).unwrap_or_default();
        Some(Self::parse(&text))
    }

    /// Ends the claimed request, however it went.
    pub fn finish(dir: &Path) {
        let _ = fs::remove_file(dir.join(TAKEN));
    }

    /// The id of a request an earlier run claimed and never finished, as when
    /// it crashed or was stopped. Not tried again: what ended that run could
    /// end this one too.
    pub fn abandoned(dir: &Path) -> Option<String> {
        let text = fs::read_to_string(dir.join(TAKEN)).ok()?;
        Self::finish(dir);
        Some(Self::parse(&text).map_or_else(|unreadable| unreadable.id, |request| request.id))
    }

    /// Leaves a request as the app does, for a pairing asked for from a
    /// terminal.
    pub fn write(dir: &Path, url: &str) -> Result<()> {
        let id = format!("{:016x}", rand::random::<u64>());
        put(&dir.join(REQUEST), &format!("url={url}\nid={id}\n"))
    }

    fn parse(text: &str) -> Result<Self, Unreadable> {
        let id = field(text, "id").filter(|id| {
            (1..=64).contains(&id.len()) && id.chars().all(|c| c.is_ascii_alphanumeric())
        });
        let url = field(text, "url").filter(|url| {
            (url.starts_with("https://") || url.starts_with("http://")) && url.ends_with("/script")
        });
        match (id, url) {
            (Some(id), Some(url)) => Ok(Self {
                url: url.into(),
                id: id.into(),
            }),
            (id, _) => Err(Unreadable {
                id: id.unwrap_or_default().into(),
            }),
        }
    }
}

/// How the tunnel stands, as `state=` names it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Connection {
    Unpaired,
    Connecting,
    Connected,
    /// The gateway refused the tunnel's login, as it does once the tunnel was
    /// removed.
    Refused,
    Stopped,
}

impl Connection {
    fn word(self) -> &'static str {
        match self {
            Self::Unpaired => "unpaired",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Refused => "refused",
            Self::Stopped => "stopped",
        }
    }
}

/// How the last pairing asked for went.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Running,
    Done,
    /// With the sentence the person who asked is shown.
    Failed(String),
}

/// What the app reads in `state`: the connection, the last pairing asked for
/// and how it went, and what tells this machine's computer apart.
#[derive(Clone, Debug, PartialEq)]
pub struct State {
    pub connection: Connection,
    pub pairing_id: String,
    pub pairing: Option<Outcome>,
    pub tunnel_id: String,
    pub fingerprint: String,
}

impl State {
    pub fn write(&self, dir: &Path) -> Result<()> {
        put(&dir.join(STATE), &self.render(std::process::id()))
    }

    fn render(&self, pid: u32) -> String {
        let (pairing, detail) = match &self.pairing {
            None => ("", ""),
            Some(Outcome::Running) => ("running", ""),
            Some(Outcome::Done) => ("done", ""),
            Some(Outcome::Failed(detail)) => ("failed", detail.as_str()),
        };
        let detail: String = detail
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(DETAIL_LIMIT)
            .collect();
        format!(
            "state={}\npairing_id={}\npairing={pairing}\npairing_detail={detail}\n\
             tunnel_id={}\nfingerprint={}\npid={pid}\n",
            self.connection.word(),
            self.pairing_id,
            self.tunnel_id,
            self.fingerprint,
        )
    }
}

/// Writes a file whole under another name and moves it over, so a crash
/// never leaves half of one that every later start fails to read.
fn put(path: &Path, text: &str) -> Result<()> {
    let staged = path.with_extension("new");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&staged)
        .with_context(|| format!("writing {}", staged.display()))?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    fs::rename(&staged, path).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FINGERPRINT: &str = "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU";
    const PAIRING_ID: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";

    fn key_line() -> String {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap();
        key.public_key().to_openssh().unwrap()
    }

    #[test]
    fn renders_the_state_the_app_reads() {
        let state = State {
            connection: Connection::Connected,
            pairing_id: PAIRING_ID.into(),
            pairing: Some(Outcome::Done),
            tunnel_id: "7c9e6679-7425-40de-944b-e07fc1f90ae7".into(),
            fingerprint: FINGERPRINT.into(),
        };
        assert_eq!(state.render(4242), include_str!("../contract/state"));
    }

    #[test]
    fn renders_a_failed_pairing_the_app_reads() {
        let state = State {
            connection: Connection::Unpaired,
            pairing_id: PAIRING_ID.into(),
            pairing: Some(Outcome::Failed(
                "That pairing command has expired.\nStart again from LangAlpha.".into(),
            )),
            tunnel_id: String::new(),
            fingerprint: FINGERPRINT.into(),
        };
        assert_eq!(state.render(4242), include_str!("../contract/state-failed"));
    }

    #[test]
    fn reads_the_request_the_app_writes() {
        let request = Request::parse(include_str!("../contract/pairing")).ok();
        let expected = Request {
            url: "https://example.com/api/v1/computers/ssh/pairings/\
                  0123456789abcdef0123456789abcdef/script"
                .into(),
            id: PAIRING_ID.into(),
        };
        assert_eq!(request, Some(expected));
    }

    #[test]
    fn refuses_a_request_for_anything_but_a_pairing_script() {
        let unreadable = Request::parse("url=https://example.com/other\nid=abc123\n").err();
        assert_eq!(unreadable.map(|u| u.id), Some("abc123".into()));
        let unreadable = Request::parse("url=https://example.com/script\nid=../x\n").err();
        assert_eq!(unreadable.map(|u| u.id), Some(String::new()));
    }

    #[test]
    fn claims_a_request_without_losing_the_next() -> Result<()> {
        let dir = tempfile::tempdir()?;
        Request::write(dir.path(), "https://example.com/script")?;
        let first = Request::claim(dir.path()).and_then(Result::ok);
        assert!(first.is_some(), "the request was not claimed");
        assert!(Request::claim(dir.path()).is_none(), "claimed twice");

        // The app asks again while the first is under way.
        Request::write(dir.path(), "https://example.org/script")?;
        Request::finish(dir.path());
        let next = Request::claim(dir.path()).and_then(Result::ok);
        assert_eq!(
            next.map(|r| r.url),
            Some("https://example.org/script".into())
        );
        Ok(())
    }

    #[test]
    fn drops_a_request_an_earlier_run_never_finished() -> Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(dir.path().join(TAKEN), include_str!("../contract/pairing"))?;
        assert_eq!(Request::abandoned(dir.path()), Some(PAIRING_ID.into()));
        assert_eq!(Request::abandoned(dir.path()), None);
        Ok(())
    }

    #[test]
    fn keeps_the_paired_record_whole() -> Result<()> {
        let answer = format!(
            "tunnel_id=7c9e6679-7425-40de-944b-e07fc1f90ae7\ngateway_host=gw.example.com\n\
             gateway_port=2222\nknown_hosts=[gw.example.com]:2222 {}\n",
            key_line(),
        );
        // As the deployment words it, options with `=` of their own included.
        let authorized = format!(
            "from=\"192.0.2.0/24,127.0.0.1,::1\",no-agent-forwarding,no-X11-forwarding {}",
            key_line(),
        );
        let paired = Paired::new(&answer, authorized)?;
        assert_eq!(paired.gateway_port, 2222);

        let dir = tempfile::tempdir()?;
        assert_eq!(Paired::load(dir.path())?, None);
        paired.save(dir.path())?;
        assert_eq!(Paired::load(dir.path())?, Some(paired));
        Ok(())
    }

    #[test]
    fn refuses_a_tunnel_it_cannot_use() {
        let pin = format!("known_hosts=gw {}", key_line());
        let authorized = key_line();
        let bad = [
            format!("tunnel_id=a b\ngateway_host=gw\ngateway_port=22\n{pin}"),
            format!("tunnel_id=t\ngateway_host=gw;x\ngateway_port=22\n{pin}"),
            format!("tunnel_id=t\ngateway_host=gw\ngateway_port=70000\n{pin}"),
            "tunnel_id=t\ngateway_host=gw\ngateway_port=22\nknown_hosts=gw".into(),
            "tunnel_id=t\ngateway_host=gw\ngateway_port=22".into(),
        ];
        for answer in bad {
            assert!(
                Paired::new(&answer, authorized.clone()).is_err(),
                "{answer}"
            );
        }
        let answer = format!("tunnel_id=t\ngateway_host=gw\ngateway_port=22\n{pin}");
        assert!(Paired::new(&answer, "not a key".into()).is_err());
    }

    #[test]
    fn makes_new_keys_in_a_folder_carried_over_from_another_machine() -> Result<()> {
        let dir = tempfile::tempdir()?;
        forget_another_machine(dir.path(), Some("MAC-A"))?;
        let first = own_key(&dir.path().join(HOST_KEY))?;
        fs::write(dir.path().join(PAIRED), "tunnel_id=t\n")?;
        fs::write(dir.path().join(STATE), "state=connected\n")?;
        fs::write(
            dir.path().join(REQUEST),
            "url=https://example.com/script\nid=a\n",
        )?;

        // The same machine starting again keeps everything.
        forget_another_machine(dir.path(), Some("MAC-A"))?;
        assert_eq!(own_key(&dir.path().join(HOST_KEY))?, first);
        assert!(dir.path().join(PAIRED).exists());

        forget_another_machine(dir.path(), Some("MAC-B"))?;
        for gone in [HOST_KEY, PAIRED, STATE] {
            assert!(!dir.path().join(gone).exists(), "{gone} was kept");
        }
        // The app's ask, written on this machine, still stands.
        assert!(dir.path().join(REQUEST).exists());
        assert_ne!(own_key(&dir.path().join(HOST_KEY))?, first);
        assert_eq!(fs::read_to_string(dir.path().join(MACHINE))?, "MAC-B\n");
        Ok(())
    }

    #[test]
    fn takes_a_folder_from_before_the_check_as_this_machines() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let first = own_key(&dir.path().join(HOST_KEY))?;
        forget_another_machine(dir.path(), Some("MAC-A"))?;
        assert_eq!(own_key(&dir.path().join(HOST_KEY))?, first);
        // Nothing to tell machines apart by: nothing is dropped or written.
        forget_another_machine(dir.path(), None)?;
        assert_eq!(fs::read_to_string(dir.path().join(MACHINE))?, "MAC-A\n");
        Ok(())
    }

    #[test]
    fn names_the_machine_as_the_app_reads_it() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let example = include_str!("../contract/machine");
        forget_another_machine(dir.path(), Some(example.trim()))?;
        assert_eq!(fs::read_to_string(dir.path().join(MACHINE))?, example);
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn reads_the_machine_as_the_app_does() -> Result<()> {
        let shown = std::process::Command::new("/usr/sbin/ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()?;
        let shown = String::from_utf8(shown.stdout)?;
        let uuid = shown
            .lines()
            .find_map(|line| line.split("\"IOPlatformUUID\" = \"").nth(1))
            .and_then(|rest| rest.split('"').next())
            .context("no IOPlatformUUID")?;
        assert_eq!(this_machine().as_deref(), Some(uuid));
        Ok(())
    }

    #[test]
    fn keeps_the_folder_to_one_helper() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let held = lock(dir.path())?;
        assert!(lock(dir.path()).is_err(), "a second helper took the folder");
        drop(held);
        lock(dir.path())?;
        Ok(())
    }
}
