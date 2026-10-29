//! The helper as a login service runs it: one folder, paired on request and
//! held from then on, with how it stands written where the app that installed
//! it can read it.
//!
//! The app never talks to this process. It leaves a `pairing` file to ask for
//! a pairing and reads `state` to learn how that went and whether the tunnel
//! is up, so either side may restart at any time without the other noticing.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::folder::{Connection, Identity, Outcome, Paired, Request, State};
use crate::pair::{self, LOCAL_FAILURE, PairError};
use crate::server::Machine;
use crate::tunnel::{self, Gateway};
use crate::update::{self, Busy, Executable, Hold};

// How often the folder is looked at for a request.
const POLL: Duration = Duration::from_secs(1);
// How often the executable is looked at for an update, and once one replaced
// it, how often the helper looks for nothing under way and how long it waits
// for that before it restarts anyway.
const UPDATE_CHECK: Duration = Duration::from_secs(60);
const IDLE_POLL: Duration = Duration::from_secs(1);
const IDLE_PATIENCE: Duration = Duration::from_secs(60 * 60);
// How long a pairing's outcome holds off that restart, so the app, which
// reads `state` four times a second, sees it before a new helper writes
// `state` without it.
const OUTCOME_SEEN: Duration = Duration::from_secs(5);

/// Serves the folder until told to stop, or until an update replaced this
/// helper.
pub async fn run(dir: &Path) -> Result<()> {
    let executable = Executable::current();
    let identity = Arc::new(Identity::load(dir)?);
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let busy = Busy::default();
    let mut service = Service::start(dir, identity, busy.clone());
    let update = update::restart_due(executable, &busy, UPDATE_CHECK, IDLE_POLL, IDLE_PATIENCE);
    tokio::select! {
        () = service.serve() => {}
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
        () = update => {
            // Not `stopped`, which tells the app remote control was turned
            // off. The new helper writes `state` as it starts.
            info!(
                in_flight = busy.in_flight(),
                "This helper was updated; exiting for launchd to start the new one"
            );
            return Ok(());
        }
    }
    // launchd's stop when the service is turned off. The state says so, or it
    // would go on saying the tunnel is up.
    info!("Stopped");
    service.publish(Connection::Stopped);
    Ok(())
}

/// A tunnel being held, and the record it was made from. Dropping it lets
/// the tunnel go.
struct Held {
    paired: Paired,
    connection: watch::Receiver<Connection>,
    task: JoinHandle<()>,
}

impl Held {
    fn start(paired: Paired, identity: &Identity, busy: &Busy) -> Result<Self> {
        let gateway = Gateway::new(&paired, identity.tunnel_key.clone())?;
        let machine = Machine::new(
            identity.account.clone(),
            identity.host_key.clone(),
            paired.authorized()?,
            busy.clone(),
        );
        let (publish, connection) = watch::channel(Connection::Connecting);
        let task = tokio::spawn(tunnel::hold(gateway, Arc::new(machine), publish));
        Ok(Self {
            paired,
            connection,
            task,
        })
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// With its hold on a restart for an update, taken as the request is claimed
// and let go a while after it settles.
type Pairing = (String, JoinHandle<Result<Held, PairError>>, Hold);

/// The only writer of `state`.
struct Service {
    dir: PathBuf,
    identity: Arc<Identity>,
    busy: Busy,
    held: Option<Held>,
    // The connection and tunnel are read from `held` as it is written.
    state: State,
    written: Option<State>,
}

impl Service {
    fn start(dir: &Path, identity: Arc<Identity>, busy: Busy) -> Self {
        let mut state = State {
            connection: Connection::Unpaired,
            pairing_id: String::new(),
            pairing: None,
            tunnel_id: String::new(),
            fingerprint: identity.fingerprint(),
        };
        if let Some(id) = Request::abandoned(dir) {
            warn!("A pairing was cut short by the end of an earlier run");
            state.pairing_id = id;
            state.pairing = Some(Outcome::Failed(LOCAL_FAILURE.into()));
        }
        let held = Paired::load(dir).and_then(|paired| {
            paired
                .map(|paired| Held::start(paired, &identity, &busy))
                .transpose()
        });
        let held = held.unwrap_or_else(|e| {
            warn!("This folder's tunnel cannot be held: {e:#}");
            None
        });
        Self {
            dir: dir.to_path_buf(),
            identity,
            busy,
            held,
            state,
            written: None,
        }
    }

    async fn serve(&mut self) {
        let mut pairing: Option<Pairing> = None;
        loop {
            self.publish(self.connection());
            tokio::select! {
                () = tokio::time::sleep(POLL), if pairing.is_none() => pairing = self.claim(),
                result = finished(&mut pairing) => {
                    if let Some((id, _, hold)) = pairing.take() {
                        self.settle(id, result);
                        tokio::spawn(async move {
                            tokio::time::sleep(OUTCOME_SEEN).await;
                            drop(hold);
                        });
                    }
                }
                () = changed(&mut self.held) => {}
            }
        }
    }

    /// Starts the pairing the app asked for, if it asked.
    fn claim(&mut self) -> Option<Pairing> {
        match Request::claim(&self.dir)? {
            Ok(Request { url, id }) => {
                info!(%id, "Pairing");
                self.state.pairing_id = id.clone();
                self.state.pairing = Some(Outcome::Running);
                let (identity, dir) = (self.identity.clone(), self.dir.clone());
                let busy = self.busy.clone();
                let hold = busy.hold();
                let task =
                    tokio::spawn(async move { pair_with(&url, &identity, &dir, &busy).await });
                Some((id, task, hold))
            }
            Err(unreadable) => {
                warn!("Ignored a pairing request that could not be read");
                self.state.pairing_id = unreadable.id;
                self.state.pairing = Some(Outcome::Failed(
                    "The request to pair could not be read.".into(),
                ));
                Request::finish(&self.dir);
                None
            }
        }
    }

    fn settle(&mut self, id: String, result: Result<Held, PairError>) {
        Request::finish(&self.dir);
        self.state.pairing_id = id;
        self.state.pairing = Some(match result {
            Ok(held) => {
                info!("Paired");
                self.held = Some(held);
                Outcome::Done
            }
            Err(e) => {
                warn!("Pairing failed: {e}");
                Outcome::Failed(e.user_message())
            }
        });
    }

    fn connection(&self) -> Connection {
        self.held
            .as_ref()
            .map_or(Connection::Unpaired, |held| *held.connection.borrow())
    }

    /// Writes `state` when it changed.
    fn publish(&mut self, connection: Connection) {
        self.state.connection = connection;
        self.state.tunnel_id = self
            .held
            .as_ref()
            .map(|held| held.paired.tunnel_id.clone())
            .unwrap_or_default();
        if self.written.as_ref() == Some(&self.state) {
            return;
        }
        // Left unwritten, it is tried again on the next turn of the loop.
        match self.state.write(&self.dir) {
            Ok(()) => self.written = Some(self.state.clone()),
            Err(e) => warn!("Could not write the state: {e:#}"),
        }
    }
}

/// Pairs with the deployment whose pairing script is at `url`. The new tunnel
/// is held beside whatever the folder holds while the deployment checks it,
/// and the record is replaced only once it has, so a pairing that fails
/// leaves the machine as it was.
async fn pair_with(
    url: &str,
    identity: &Identity,
    dir: &Path,
    busy: &Busy,
) -> Result<Held, PairError> {
    let offer = pair::offer(url, identity).await?;
    let held = match Held::start(offer.paired.clone(), identity, busy) {
        Ok(held) => held,
        Err(e) => return Err(offer.stop(PairError::Local(e)).await),
    };
    pair::confirm(&offer).await?;
    offer.paired.save(dir).map_err(PairError::Local)?;
    Ok(held)
}

/// Until the pairing under way ends; never, with none.
async fn finished(pairing: &mut Option<Pairing>) -> Result<Held, PairError> {
    match pairing {
        Some((_, task, _)) => task
            .await
            .unwrap_or_else(|e| Err(PairError::Local(e.into()))),
        None => std::future::pending().await,
    }
}

/// Until the tunnel held says it stands otherwise; never, with none.
async fn changed(held: &mut Option<Held>) {
    if let Some(held) = held
        && held.connection.changed().await.is_ok()
    {
        return;
    }
    std::future::pending().await
}
