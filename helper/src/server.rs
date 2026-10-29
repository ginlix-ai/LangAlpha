//! The SSH server at the far end of the tunnel's forward. It offers what the
//! backend uses of a machine's sshd: a command or SFTP as this account, and a
//! connection to a port on this machine's loopback for previews. It refuses
//! the rest: shells, ptys, env, agent and X11 forwarding, any other forward.

use std::borrow::Cow;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use russh::keys::{PrivateKey, PublicKey};
use russh::server::{self, Auth, Msg, Session};
use russh::{Channel, ChannelId, ChannelOpenFailure, MethodKind, MethodSet, SshId};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::account::Account;
use crate::login_env::LoginEnv;
use crate::process::{self, Start};
use crate::update::Busy;

// Where each system keeps the SFTP server its sshd runs.
const SFTP_SERVERS: &[&str] = &[
    "/usr/libexec/sftp-server",
    "/usr/lib/openssh/sftp-server",
    "/usr/libexec/openssh/sftp-server",
    "/usr/lib/ssh/sftp-server",
];

// What the backend reads at its probe to know the helper, not an sshd,
// answers for the machine.
const SERVER_ID: &str = concat!("SSH-2.0-LangAlphaHelper_", env!("CARGO_PKG_VERSION"));

pub struct Machine {
    pub account: Account,
    pub sftp_server: Option<PathBuf>,
    pub login_env: LoginEnv,
    config: Arc<server::Config>,
    /// The deployment's key, the one login it lets in.
    authorized: PublicKey,
    busy: Busy,
}

impl Machine {
    pub fn new(account: Account, host_key: PrivateKey, authorized: PublicKey, busy: Busy) -> Self {
        let sftp_server = SFTP_SERVERS.iter().map(PathBuf::from).find(|p| p.is_file());
        if sftp_server.is_none() {
            warn!("No sftp-server on this machine; file transfer will be refused");
        }
        let config = server::Config {
            server_id: SshId::Standard(Cow::Borrowed(SERVER_ID)),
            methods: MethodSet::from(&[MethodKind::PublicKey][..]),
            keys: vec![host_key],
            // The backend's one key either matches or does not; nothing is
            // gained by making it wait on a refusal.
            auth_rejection_time: Duration::from_millis(200),
            auth_rejection_time_initial: Some(Duration::ZERO),
            max_auth_attempts: 3,
            ..Default::default()
        };
        info!(account = %account.name, "Serving logins for this account");
        Self {
            login_env: LoginEnv::new(account.clone()),
            account,
            sftp_server,
            config: Arc::new(config),
            authorized,
            busy,
        }
    }

    /// Serves one SSH connection arriving through the tunnel, the gateway's
    /// dial having come from `port`.
    pub async fn serve<S>(self: Arc<Self>, stream: S, port: u32) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let config = self.config.clone();
        let login = Login {
            machine: self,
            port,
            channels: HashMap::new(),
        };
        match server::run_stream(config, stream, login).await?.await {
            // The backend went away without a goodbye, as every login does
            // when the gateway drops its side of the dial.
            Err(e) if ended_quietly(&e) => Ok(()),
            other => other,
        }
    }

    fn admits(&self, user: &str, key: &PublicKey) -> bool {
        user == self.account.name && key.key_data() == self.authorized.key_data()
    }
}

struct Login {
    machine: Arc<Machine>,
    port: u32,
    // Session channels open and not yet running anything.
    channels: HashMap<ChannelId, Channel<Msg>>,
}

impl Login {
    /// Runs what a session channel asked for. Once: as with sshd, a channel
    /// runs one command, and asking again is refused.
    fn run(&mut self, id: ChannelId, start: Start, session: &mut Session) -> Result<()> {
        let Some(channel) = self.channels.remove(&id) else {
            session.channel_failure(id)?;
            return Ok(());
        };
        session.channel_success(id)?;
        // A command holds off a restart for an update while it runs. SFTP does
        // not: the backend keeps its channel open as long as the connection.
        let busy = matches!(start, Start::Shell(_)).then(|| self.machine.busy.hold());
        let run = process::run(
            channel,
            session.handle(),
            self.machine.clone(),
            self.port,
            start,
        );
        tokio::spawn(async move {
            let _busy = busy;
            run.await
        });
        Ok(())
    }
}

fn refused() -> Auth {
    Auth::Reject {
        proceed_with_methods: None,
        partial_success: false,
    }
}

impl server::Handler for Login {
    type Error = anyhow::Error;

    async fn auth_publickey_offered(&mut self, user: &str, key: &PublicKey) -> Result<Auth> {
        Ok(if self.machine.admits(user, key) {
            Auth::Accept
        } else {
            refused()
        })
    }

    async fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> Result<Auth> {
        Ok(if self.machine.admits(user, key) {
            Auth::Accept
        } else {
            refused()
        })
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut Session,
    ) -> Result<()> {
        reply.accept().await;
        self.channels.insert(channel.id(), channel);
        Ok(())
    }

    async fn channel_close(&mut self, id: ChannelId, _: &mut Session) -> Result<()> {
        self.channels.remove(&id);
        Ok(())
    }

    async fn channel_open_direct_tcpip(
        &mut self,
        channel: Channel<Msg>,
        host: &str,
        port: u32,
        _: &str,
        _: u32,
        reply: server::ChannelOpenHandle,
        _: &mut Session,
    ) -> Result<()> {
        let Some(address) = loopback(host, port) else {
            return Ok(()); // dropping the reply refuses the channel
        };
        let busy = self.machine.busy.hold();
        tokio::spawn(async move {
            let _busy = busy;
            match TcpStream::connect(address).await {
                Ok(mut tcp) => {
                    reply.accept().await;
                    let mut stream = channel.into_stream();
                    let _ = tokio::io::copy_bidirectional(&mut stream, &mut tcp).await;
                }
                Err(_) => reply.reject(ChannelOpenFailure::ConnectFailed).await,
            }
        });
        Ok(())
    }

    async fn exec_request(
        &mut self,
        id: ChannelId,
        command: &[u8],
        session: &mut Session,
    ) -> Result<()> {
        self.run(id, Start::Shell(command.to_vec()), session)
    }

    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<()> {
        match self.machine.sftp_server.clone() {
            Some(server) if name == "sftp" => self.run(id, Start::Sftp(server), session),
            _ => {
                session.channel_failure(id)?;
                Ok(())
            }
        }
    }

    async fn shell_request(&mut self, id: ChannelId, session: &mut Session) -> Result<()> {
        session.channel_failure(id)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn pty_request(
        &mut self,
        id: ChannelId,
        _: &str,
        _: u32,
        _: u32,
        _: u32,
        _: u32,
        _: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<()> {
        session.channel_failure(id)?;
        Ok(())
    }

    async fn env_request(
        &mut self,
        id: ChannelId,
        _: &str,
        _: &str,
        session: &mut Session,
    ) -> Result<()> {
        session.channel_failure(id)?;
        Ok(())
    }

    async fn x11_request(
        &mut self,
        id: ChannelId,
        _: bool,
        _: &str,
        _: &str,
        _: u32,
        session: &mut Session,
    ) -> Result<()> {
        session.channel_failure(id)?;
        Ok(())
    }
}

fn loopback(host: &str, port: u32) -> Option<SocketAddr> {
    let port = u16::try_from(port).ok()?;
    match host {
        "127.0.0.1" | "localhost" => Some((Ipv4Addr::LOCALHOST, port).into()),
        "::1" => Some((Ipv6Addr::LOCALHOST, port).into()),
        _ => None,
    }
}

fn ended_quietly(e: &anyhow::Error) -> bool {
    match e.downcast_ref::<russh::Error>() {
        Some(russh::Error::IO(io)) => io.kind() == std::io::ErrorKind::UnexpectedEof,
        Some(russh::Error::Disconnect | russh::Error::HUP) => true,
        _ => false,
    }
}
