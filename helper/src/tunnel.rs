//! The tunnel: one login to the gateway holding a `langalpha:22` forward, made
//! again whenever it drops, with the waits `tunnel.sh`'s loop keeps.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use russh::Channel;
use russh::client::{self, ChannelOpenHandle};
use russh::keys::{PrivateKey, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::folder::{Connection, Paired};
use crate::server::Machine;

// The forward the gateway expects, the one `ssh -R langalpha:22:...` asks for.
const FORWARD_HOST: &str = "langalpha";
const FORWARD_PORT: u32 = 22;

pub struct Gateway {
    host: String,
    port: u16,
    login: String,
    key: Arc<PrivateKey>,
    pin: PublicKey,
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
        })
    }

    /// One login, held until it drops.
    async fn hold_once(
        &self,
        machine: &Arc<Machine>,
        connection: &watch::Sender<Connection>,
    ) -> Result<Ended> {
        let config = Arc::new(client::Config {
            keepalive_interval: Some(Duration::from_secs(15)),
            keepalive_max: 3,
            inactivity_timeout: None,
            ..Default::default()
        });
        let client = Client {
            pin: self.pin.clone(),
            machine: machine.clone(),
        };
        let mut handle = tokio::time::timeout(
            Duration::from_secs(15),
            client::connect(config, (self.host.as_str(), self.port), client),
        )
        .await
        .context("no answer from the gateway")??;
        let key = PrivateKeyWithHashAlg::new(self.key.clone(), None);
        if !handle
            .authenticate_publickey(&self.login, key)
            .await?
            .success()
        {
            return Ok(Ended::Refused);
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
}

/// Holds the tunnel until the task holding it is dropped, saying how it
/// stands through `connection`.
pub async fn hold(gateway: Gateway, machine: Arc<Machine>, connection: watch::Sender<Connection>) {
    let mut step = 5;
    loop {
        let started = Instant::now();
        let ended = gateway
            .hold_once(&machine, &connection)
            .await
            .unwrap_or_else(|e| {
                warn!("Tunnel down: {e:#}");
                Ended::Dropped
            });
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

struct Client {
    pin: PublicKey,
    machine: Arc<Machine>,
}

impl client::Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool> {
        Ok(matches!(
            key,
            PublicKeyOrCertificate::PublicKey { key, .. } if key.key_data() == self.pin.key_data()
        ))
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
