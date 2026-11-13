//! The tunnel: one login to the gateway holding a `langalpha:22` forward, made
//! again whenever it drops, with the waits `tunnel.sh`'s loop keeps. And its
//! end once this machine paired again: a login that tells the gateway a newer
//! pairing superseded the tunnel.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use russh::client::{self, ChannelOpenHandle, Handle};
use russh::keys::{PrivateKey, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate};
use russh::{Channel, ChannelMsg};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::folder::{Connection, Paired};
use crate::server::Machine;

// The forward the gateway expects, the one `ssh -R langalpha:22:...` asks for.
const FORWARD_HOST: &str = "langalpha";
const FORWARD_PORT: u32 = 22;
// What a tunnel's last login runs at the gateway once a newer pairing
// superseded it.
const SUPERSEDE: &str = "supersede";
// It is told once, best effort: a gateway that never hears it reads the
// tunnel as down, as it would have.
const SUPERSEDE_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Gateway {
    host: String,
    port: u16,
    login: String,
    key: Arc<PrivateKey>,
    pin: PublicKey,
    // Set once a newer pairing superseded the tunnel, which is then not made
    // again: one made after the gateway recorded that would clear it.
    superseded: AtomicBool,
}

/// How one login to the gateway ended.
enum Ended {
    Dropped,
    /// The gateway refused the login itself, which most likely means the
    /// tunnel was removed.
    Refused,
}

impl Gateway {
    pub fn new(paired: &Paired, key: PrivateKey) -> Result<Self> {
        Ok(Self {
            host: paired.gateway_host.clone(),
            port: paired.gateway_port,
            login: paired.tunnel_id.clone(),
            key: Arc::new(key),
            pin: paired.gateway_key()?,
            superseded: AtomicBool::new(false),
        })
    }

    /// A login as the tunnel; None when the gateway refused it.
    async fn log_in<H>(&self, config: client::Config, handler: H) -> Result<Option<Handle<H>>>
    where
        H: client::Handler<Error = anyhow::Error> + Send + 'static,
    {
        let mut handle = tokio::time::timeout(
            Duration::from_secs(15),
            client::connect(Arc::new(config), (self.host.as_str(), self.port), handler),
        )
        .await
        .context("no answer from the gateway")??;
        let key = PrivateKeyWithHashAlg::new(self.key.clone(), None);
        let admitted = handle
            .authenticate_publickey(&self.login, key)
            .await?
            .success();
        Ok(admitted.then_some(handle))
    }

    /// One login, held until it drops.
    async fn hold_once(
        &self,
        machine: &Arc<Machine>,
        connection: &watch::Sender<Connection>,
    ) -> Result<Ended> {
        let config = client::Config {
            keepalive_interval: Some(Duration::from_secs(15)),
            keepalive_max: 3,
            inactivity_timeout: None,
            ..Default::default()
        };
        let client = Client {
            pin: Pinned(self.pin.clone()),
            machine: machine.clone(),
        };
        let Some(handle) = self.log_in(config, client).await? else {
            return Ok(Ended::Refused);
        };
        if self.superseded.load(Ordering::Relaxed) {
            return Ok(Ended::Dropped);
        }
        handle
            .tcpip_forward(FORWARD_HOST, FORWARD_PORT)
            .await
            .context("the gateway refused the forward")?;
        info!("Tunnel up");
        connection.send_replace(Connection::Connected);
        handle.await?;
        Ok(Ended::Dropped)
    }

    /// Tells the gateway a newer pairing superseded this tunnel, on a login of
    /// its own with the tunnel's key, so the deployment can tell whoever still
    /// has a computer on it that this machine moved, perhaps to another
    /// account, instead of calling it away.
    pub async fn supersede(&self) -> Result<()> {
        self.superseded.store(true, Ordering::Relaxed);
        let ran = async {
            let pinned = Pinned(self.pin.clone());
            let Some(handle) = self.log_in(client::Config::default(), pinned).await? else {
                bail!("the gateway refused the old tunnel's login");
            };
            let mut channel = handle.channel_open_session().await?;
            channel.exec(true, SUPERSEDE).await?;
            while let Some(msg) = channel.wait().await {
                match msg {
                    ChannelMsg::ExitStatus { exit_status: 0 } => return Ok(()),
                    ChannelMsg::ExitStatus { exit_status } => {
                        bail!("the gateway answered {exit_status}")
                    }
                    ChannelMsg::Failure => bail!("the gateway refused it"),
                    _ => {}
                }
            }
            bail!("the gateway closed it unanswered")
        };
        tokio::time::timeout(SUPERSEDE_TIMEOUT, ran)
            .await
            .context("no answer from the gateway")?
    }
}

/// Holds the tunnel until the task holding it is dropped, saying how it
/// stands through `connection`.
pub async fn hold(
    gateway: Arc<Gateway>,
    machine: Arc<Machine>,
    connection: watch::Sender<Connection>,
) {
    let mut step = 5;
    loop {
        if gateway.superseded.load(Ordering::Relaxed) {
            return;
        }
        let started = Instant::now();
        let ended = gateway
            .hold_once(&machine, &connection)
            .await
            .unwrap_or_else(|e| {
                warn!("Tunnel down: {e:#}");
                Ended::Dropped
            });
        if gateway.superseded.load(Ordering::Relaxed) {
            return;
        }
        // A connection that held for a minute starts the waits over.
        if started.elapsed() >= Duration::from_secs(60) {
            step = 5;
        }
        let wait = match ended {
            Ended::Dropped => {
                connection.send_replace(Connection::Connecting);
                let wait = step;
                step = (step * 2).min(30);
                wait
            }
            Ended::Refused => {
                connection.send_replace(Connection::Refused);
                // Asked again now and then rather than never, in case the
                // refusal was a mistake.
                warn!("The gateway refused this tunnel's login");
                600
            }
        };
        // Between half and one and a half times the step, so machines a
        // gateway restart drops together come back spread out.
        let pause = rand::random_range(wait * 500..=wait * 1500);
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }
}

/// Takes only the gateway the pairing pinned.
struct Pinned(PublicKey);

impl Pinned {
    fn admits(&self, key: &PublicKeyOrCertificate) -> bool {
        matches!(
            key,
            PublicKeyOrCertificate::PublicKey { key, .. } if key.key_data() == self.0.key_data()
        )
    }
}

impl client::Handler for Pinned {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool> {
        Ok(self.admits(key))
    }
}

struct Client {
    pin: Pinned,
    machine: Arc<Machine>,
}

impl client::Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool> {
        Ok(self.pin.admits(key))
    }

    async fn server_channel_open_forwarded_tcpip(
        &mut self,
        channel: Channel<client::Msg>,
        connected_address: &str,
        connected_port: u32,
        _originator_address: &str,
        originator_port: u32,
        reply: ChannelOpenHandle,
        _session: &mut client::Session,
    ) -> Result<()> {
        if connected_address != FORWARD_HOST || connected_port != FORWARD_PORT {
            return Ok(()); // dropping the reply refuses the channel
        }
        reply.accept().await;
        let machine = self.machine.clone();
        tokio::spawn(async move {
            if let Err(e) = machine.serve(channel.into_stream(), originator_port).await {
                warn!("A login ended with an error: {e:#}");
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use russh::keys::Algorithm;
    use russh::server::{self, Auth, Msg, Session};
    use russh::{ChannelId, MethodKind, MethodSet};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::*;

    const TUNNEL: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

    /// The gateway's side of one login: the tunnel's id with its key, and
    /// what it ran, answered with `answer`.
    struct FakeGateway {
        key: PublicKey,
        answer: u32,
        ran: mpsc::UnboundedSender<String>,
    }

    impl server::Handler for FakeGateway {
        type Error = anyhow::Error;

        async fn auth_publickey_offered(&mut self, user: &str, key: &PublicKey) -> Result<Auth> {
            self.auth_publickey(user, key).await
        }

        async fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> Result<Auth> {
            Ok(if user == TUNNEL && key.key_data() == self.key.key_data() {
                Auth::Accept
            } else {
                Auth::Reject {
                    proceed_with_methods: None,
                    partial_success: false,
                }
            })
        }

        async fn channel_open_session(
            &mut self,
            _: Channel<Msg>,
            reply: server::ChannelOpenHandle,
            _: &mut Session,
        ) -> Result<()> {
            reply.accept().await;
            Ok(())
        }

        async fn exec_request(
            &mut self,
            id: ChannelId,
            command: &[u8],
            session: &mut Session,
        ) -> Result<()> {
            self.ran.send(String::from_utf8_lossy(command).into())?;
            session.channel_success(id)?;
            session.exit_status_request(id, self.answer)?;
            session.close(id)?;
            Ok(())
        }
    }

    /// A gateway taking one login on loopback, and the tunnel's record that
    /// pins it.
    async fn gateway(
        key: PublicKey,
        answer: u32,
    ) -> Result<(Paired, mpsc::UnboundedReceiver<String>)> {
        let host_key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let pin = format!("[127.0.0.1]:0 {}", host_key.public_key().to_openssh()?);
        let config = Arc::new(server::Config {
            methods: MethodSet::from(&[MethodKind::PublicKey][..]),
            keys: vec![host_key],
            ..Default::default()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let (ran, seen) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let handler = FakeGateway { key, answer, ran };
            server::run_stream(config, stream, handler).await?.await
        });
        let authorized = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let paired = Paired::new(
            &format!(
                "tunnel_id={TUNNEL}\ngateway_host=127.0.0.1\ngateway_port={port}\nknown_hosts={pin}"
            ),
            authorized.public_key().to_openssh()?,
        )?;
        Ok((paired, seen))
    }

    #[tokio::test]
    async fn says_the_tunnel_was_superseded_logged_in_with_its_own_key() -> Result<()> {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let (paired, mut ran) = gateway(key.public_key().clone(), 0).await?;

        Gateway::new(&paired, key)?.supersede().await?;
        // The gateway's own word for it, which a shipped helper cannot change.
        assert_eq!(ran.recv().await.as_deref(), Some("supersede"));
        Ok(())
    }

    #[tokio::test]
    async fn a_superseded_tunnel_is_not_made_again() -> Result<()> {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let (paired, _ran) = gateway(key.public_key().clone(), 0).await?;
        let gateway = Arc::new(Gateway::new(&paired, key)?);
        gateway.supersede().await?;
        let machine = Machine::new(
            crate::account::Account::current()?,
            PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?,
            paired.authorized()?,
            crate::update::Busy::default(),
        );
        let (connection, _) = watch::channel(Connection::Connecting);

        // The gateway took its one login, so this one drops at once.
        tokio::time::timeout(
            Duration::from_secs(5),
            hold(gateway, Arc::new(machine), connection),
        )
        .await
        .context("it kept making the tunnel again")?;
        Ok(())
    }

    #[tokio::test]
    async fn fails_a_supersede_the_gateway_did_not_record() -> Result<()> {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let (paired, _ran) = gateway(key.public_key().clone(), 1).await?;

        assert!(Gateway::new(&paired, key)?.supersede().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn fails_under_another_key() -> Result<()> {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let other = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let (paired, mut ran) = gateway(key.public_key().clone(), 0).await?;

        assert!(Gateway::new(&paired, other)?.supersede().await.is_err());
        assert!(
            ran.try_recv().is_err(),
            "ran something for a login it refused"
        );
        Ok(())
    }
}
