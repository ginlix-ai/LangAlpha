//! The contract the backend's SSH provider holds a machine to, checked against
//! the helper's server over an in-memory stream: nothing listens and nothing
//! outside the test process can reach it.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use russh::client::{self, Handle};
use russh::keys::{Algorithm, PrivateKey, PrivateKeyWithHashAlg, PublicKeyOrCertificate};
use russh::{ChannelMsg, Sig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::account::Account;
use crate::keys;
use crate::server::Machine;
use crate::update::{self, Busy};

// The runner the backend execs: the script arrives on stdin.
const RUNNER: &str = r#"exec bash --noprofile --norc -c 's=$(cat); exec </dev/null; eval "$s"'"#;

struct Trusting;

impl client::Handler for Trusting {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, _: &PublicKeyOrCertificate) -> Result<bool> {
        Ok(true)
    }
}

struct Rig {
    dir: TempDir,
    machine: Arc<Machine>,
    key: Arc<PrivateKey>,
    busy: Busy,
}

impl Rig {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        // The deployment's line, as pairing records it.
        let line = format!(
            "no-agent-forwarding,no-X11-forwarding {}",
            key.public_key().to_openssh()?
        );
        let host_key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let authorized = keys::key_in_line(&line).context("the line holds no key")?;
        let busy = Busy::default();
        let machine = Machine::new(Account::current()?, host_key, authorized, busy.clone());
        Ok(Self {
            dir,
            machine: Arc::new(machine),
            key: Arc::new(key),
            busy,
        })
    }

    /// Whether nothing on the machine holds off a restart within a while.
    async fn idle(&self) -> bool {
        update::idle(
            &self.busy,
            Duration::from_millis(10),
            Duration::from_secs(10),
        )
        .await
    }

    async fn connect(&self) -> Result<Handle<Trusting>> {
        let (near, far) = tokio::io::duplex(1 << 16);
        tokio::spawn(self.machine.clone().serve(far, 50022));
        client::connect_stream(Arc::new(client::Config::default()), near, Trusting).await
    }

    async fn login_as(
        &self,
        user: &str,
        key: &Arc<PrivateKey>,
    ) -> Result<(Handle<Trusting>, bool)> {
        let mut handle = self.connect().await?;
        let admitted = handle
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(key.clone(), None))
            .await?
            .success();
        Ok((handle, admitted))
    }

    async fn login(&self) -> Result<Handle<Trusting>> {
        let (handle, admitted) = self.login_as(&self.machine.account.name, &self.key).await?;
        assert!(admitted, "the authorized key was refused");
        Ok(handle)
    }
}

#[derive(Default)]
struct Ran {
    stdout: String,
    stderr: String,
    status: Option<u32>,
    signal: Option<Sig>,
}

async fn exec(handle: &Handle<Trusting>, command: &str, input: &str) -> Result<Ran> {
    let mut channel = handle.channel_open_session().await?;
    channel.exec(true, command).await?;
    channel.data(input.as_bytes()).await?;
    channel.eof().await?;
    let mut ran = Ran::default();
    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { data } => ran.stdout.push_str(&String::from_utf8_lossy(&data)),
            ChannelMsg::ExtendedData { data, ext: 1 } => {
                ran.stderr.push_str(&String::from_utf8_lossy(&data))
            }
            ChannelMsg::ExitStatus { exit_status } => ran.status = Some(exit_status),
            ChannelMsg::ExitSignal { signal_name, .. } => ran.signal = Some(signal_name),
            ChannelMsg::Close => break,
            _ => {}
        }
    }
    Ok(ran)
}

#[tokio::test]
async fn runs_the_backends_script_runner() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let ran = exec(&handle, RUNNER, "echo out; echo err >&2; exit 3\n").await?;
    assert_eq!(ran.stdout, "out\n");
    assert_eq!(ran.stderr, "err\n");
    assert_eq!(ran.status, Some(3));
    Ok(())
}

#[tokio::test]
async fn starts_from_a_clean_environment_in_home() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    // cargo sets CARGO_MANIFEST_DIR for this process; a login must not see it.
    let script = r#"printf '%s\n' "$USER" "$HOME" "$SSH_CONNECTION" "$(pwd -P)" "${CARGO_MANIFEST_DIR-unset}" "$SHELL" "$PATH""#;
    let ran = exec(&handle, RUNNER, script).await?;
    let account = &rig.machine.account;
    let home = fs::canonicalize(&account.home)?;
    // What this machine's shell files put first, then a login's own list,
    // with nothing a shell file run for the command itself added.
    let vars = rig.machine.login_env.vars().await;
    let path = crate::login_env::path(&vars, &crate::process::login_path());
    let expected = [
        account.name.clone(),
        account.home.display().to_string(),
        "127.0.0.1 50022 127.0.0.1 22".into(),
        home.display().to_string(),
        "unset".into(),
        account.shell.display().to_string(),
        path.to_string_lossy().into_owned(),
    ];
    assert_eq!(ran.stdout.lines().collect::<Vec<_>>(), expected);
    assert_eq!(ran.status, Some(0));
    Ok(())
}

#[tokio::test]
async fn reports_a_death_by_signal() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let ran = exec(&handle, "kill -TERM $$", "").await?;
    assert!(matches!(ran.signal, Some(Sig::TERM)), "no TERM reported");
    assert_eq!(ran.status, None);
    Ok(())
}

#[tokio::test]
async fn passes_a_signal_to_the_command() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    // One russh has a variant for, and one it sends by name.
    for (signal, name) in [(Sig::TERM, "TERM"), (Sig::Custom("USR2".into()), "USR2")] {
        let mut channel = handle.channel_open_session().await?;
        let script =
            format!("trap 'echo {name}; exit 0' {name}; echo ready; while :; do sleep 0.1; done");
        channel.exec(true, script).await?;
        let (mut out, mut status, mut sent) = (String::new(), None, false);
        let ran = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(msg) = channel.wait().await {
                match msg {
                    ChannelMsg::Data { data } => {
                        out.push_str(&String::from_utf8_lossy(&data));
                        if !sent && out.contains("ready\n") {
                            channel.signal(signal.clone()).await?;
                            sent = true;
                        }
                    }
                    ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                    ChannelMsg::Close => break,
                    _ => {}
                }
            }
            anyhow::Ok(())
        });
        ran.await.context(name)??;
        assert_eq!(out, format!("ready\n{name}\n"));
        assert_eq!(status, Some(0), "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn leaves_a_command_running_when_its_channel_closes() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let marker = rig.dir.path().join("finished");
    let channel = handle.channel_open_session().await?;
    let command = format!("sleep 1; echo done > '{}'", marker.display());
    channel.exec(true, command).await?;
    channel.close().await?;
    drop(handle);
    // The first command waits on reading the shell files, which takes a
    // runner's own a while.
    let finished = tokio::time::timeout(Duration::from_secs(10), async {
        while fs::read_to_string(&marker).ok().as_deref() != Some("done\n") {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    finished.await.context("the command did not finish")?;
    Ok(())
}

#[tokio::test]
async fn serves_sftp_with_the_systems_server() -> Result<()> {
    let rig = Rig::new()?;
    if rig.machine.sftp_server.is_none() {
        eprintln!("no sftp-server here; skipped");
        return Ok(());
    }
    let handle = rig.login().await?;
    let mut channel = handle.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    // SSH_FXP_INIT at version 3, which sftp-server answers with its version.
    channel.data(&[0u8, 0, 0, 5, 1, 0, 0, 0, 3][..]).await?;
    let mut reply = Vec::new();
    while reply.len() < 5 {
        match channel.wait().await {
            Some(ChannelMsg::Success) => {}
            Some(ChannelMsg::Data { data }) => reply.extend_from_slice(&data),
            other => panic!("expected the server's version, got {other:?}"),
        }
    }
    const SSH_FXP_VERSION: u8 = 2;
    assert_eq!(reply[4], SSH_FXP_VERSION);
    Ok(())
}

#[tokio::test]
async fn refuses_what_the_backend_never_asks_for() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let refused = |msg: Option<ChannelMsg>| matches!(msg, Some(ChannelMsg::Failure));

    let mut channel = handle.channel_open_session().await?;
    channel
        .request_pty(true, "xterm", 80, 24, 0, 0, &[])
        .await?;
    assert!(refused(channel.wait().await), "pty");
    channel.set_env(true, "LC_ALL", "C").await?;
    assert!(refused(channel.wait().await), "env");
    channel.request_shell(true).await?;
    assert!(refused(channel.wait().await), "shell");
    channel.request_subsystem(true, "other").await?;
    assert!(refused(channel.wait().await), "subsystem");
    Ok(())
}

#[tokio::test]
async fn admits_only_the_account_and_its_paired_key() -> Result<()> {
    let rig = Rig::new()?;
    let (_, admitted) = rig.login_as("not-this-account", &rig.key).await?;
    assert!(!admitted, "another account");
    let stranger = Arc::new(PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?);
    let (_, admitted) = rig.login_as(&rig.machine.account.name, &stranger).await?;
    assert!(!admitted, "a key outside the paired record");
    Ok(())
}

#[tokio::test]
async fn names_itself_the_helper_in_the_handshake() -> Result<()> {
    let rig = Rig::new()?;
    let (mut near, far) = tokio::io::duplex(1 << 16);
    tokio::spawn(rig.machine.clone().serve(far, 50022));
    let mut line = Vec::new();
    while !line.ends_with(b"\r\n") {
        line.push(near.read_u8().await?);
    }
    let expected = format!("SSH-2.0-LangAlphaHelper_{}\r\n", env!("CARGO_PKG_VERSION"));
    assert_eq!(String::from_utf8(line)?, expected);
    Ok(())
}

#[tokio::test]
async fn forwards_to_this_machines_loopback_only() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await?;
        let mut buf = [0u8; 4];
        tcp.read_exact(&mut buf).await?;
        tcp.write_all(&buf).await?;
        anyhow::Ok(())
    });

    let channel = handle
        .channel_open_direct_tcpip("127.0.0.1", port.into(), "127.0.0.1", 0)
        .await?;
    let mut stream = channel.into_stream();
    stream.write_all(b"ping").await?;
    let mut echoed = [0u8; 4];
    stream.read_exact(&mut echoed).await?;
    assert_eq!(&echoed, b"ping");

    let elsewhere = handle
        .channel_open_direct_tcpip("192.0.2.1", 80, "127.0.0.1", 0)
        .await;
    assert!(elsewhere.is_err(), "a forward off this machine was opened");
    Ok(())
}

#[tokio::test]
async fn a_running_command_holds_off_a_restart() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let mut channel = handle.channel_open_session().await?;
    channel.exec(true, "echo ready; read line").await?;
    let mut out = String::new();
    while !out.contains("ready\n") {
        if let ChannelMsg::Data { data } = channel.wait().await.context("closed early")? {
            out.push_str(&String::from_utf8_lossy(&data));
        }
    }
    assert_eq!(rig.busy.in_flight(), 1);
    // `read` meets the end of its input, and the command ends.
    channel.eof().await?;
    while !matches!(channel.wait().await, Some(ChannelMsg::Close) | None) {}
    assert!(rig.idle().await, "still held once the command ended");
    Ok(())
}

#[tokio::test]
async fn an_open_forward_holds_off_a_restart() -> Result<()> {
    let rig = Rig::new()?;
    let handle = rig.login().await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    // Echoes until the forward's end of input, then closes.
    tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await?;
        let (mut read, mut write) = tcp.split();
        tokio::io::copy(&mut read, &mut write).await?;
        anyhow::Ok(())
    });

    let channel = handle
        .channel_open_direct_tcpip("127.0.0.1", port.into(), "127.0.0.1", 0)
        .await?;
    let mut stream = channel.into_stream();
    stream.write_all(b"ping").await?;
    let mut echoed = [0u8; 4];
    stream.read_exact(&mut echoed).await?;
    assert_eq!(rig.busy.in_flight(), 1);
    stream.shutdown().await?;
    assert!(rig.idle().await, "still held once the forward closed");
    Ok(())
}

#[tokio::test]
async fn an_sftp_session_does_not_hold_off_a_restart() -> Result<()> {
    let mut rig = Rig::new()?;
    // The backend keeps an SFTP channel open for as long as it keeps the
    // connection, so one that counted would hold the old build forever. With
    // no sftp-server here, `cat` stands in: only what the channel counts
    // toward is checked.
    let machine = Arc::get_mut(&mut rig.machine).context("the machine is shared")?;
    machine.sftp_server.get_or_insert_with(|| "/bin/cat".into());
    let handle = rig.login().await?;
    let mut channel = handle.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    channel.data(&[0u8, 0, 0, 5, 1, 0, 0, 0, 3][..]).await?;
    // Any answer: the server is running.
    loop {
        match channel.wait().await.context("closed early")? {
            ChannelMsg::Data { .. } => break,
            ChannelMsg::Failure => bail!("the subsystem was refused"),
            _ => {}
        }
    }
    assert_eq!(rig.busy.in_flight(), 0);
    Ok(())
}
