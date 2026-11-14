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
use tokio::time::Instant;
use tracing::{info, warn};

use crate::account::Account;
use crate::folder::{self, Connection, Identity, Outcome, Paired, Request, State};
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
// How long a pairing may take from when the app asked: short of the 90
// seconds the app waits for one (the desktop's `timing.pair`), with room to
// tell the deployment it stopped and to write the outcome for the app to read.
const PAIRING_DEADLINE: Duration = Duration::from_secs(80);

/// Serves the folder until told to stop, or until an update replaced this
/// helper.
pub async fn run(dir: &Path) -> Result<()> {
    let executable = Executable::current();
    folder::prepare(dir)?;
    let account = Account::current()?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let busy = Busy::default();
    let mut service = Service::start(dir, account, busy.clone());
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

/// A tunnel being held, and the pairing it was made from. Dropping it lets
/// the tunnel go.
struct Held {
    paired: Paired,
    identity: Arc<Identity>,
    gateway: Arc<Gateway>,
    connection: watch::Receiver<Connection>,
    task: JoinHandle<()>,
}

impl Held {
    fn start(paired: Paired, identity: Arc<Identity>, busy: &Busy) -> Result<Self> {
        let gateway = Arc::new(Gateway::new(&paired, identity.tunnel_key.clone())?);
        let machine = Machine::new(
            identity.account.clone(),
            identity.host_key.clone(),
            paired.authorized()?,
            busy.clone(),
        );
        let (publish, connection) = watch::channel(Connection::Connecting);
        let task = tokio::spawn(tunnel::hold(gateway.clone(), Arc::new(machine), publish));
        Ok(Self {
            paired,
            identity,
            gateway,
            connection,
            task,
        })
    }

    /// Tells the gateway a newer pairing superseded this tunnel, and only then
    /// lets it go, so it never reads as down without the reason.
    async fn retire(self) {
        let tunnel = &self.paired.tunnel_id;
        match self.gateway.supersede().await {
            Ok(()) => info!(%tunnel, "Told the gateway the old tunnel was superseded"),
            // It then reads the old tunnel as down, as it would have.
            Err(e) => {
                warn!(%tunnel, "Could not tell the gateway the old tunnel was superseded: {e:#}")
            }
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// With its hold on a restart for an update, taken as the request is claimed
// and let go a while after it settles.
type Pairing = (String, JoinHandle<Attempt>, Hold);
type Attempt = Result<Held, (PairError, Option<Staged>)>;

/// The keys of a pairing that failed once its account was known, kept for
/// the next pairing for the same account, which then finds any tunnel the
/// deployment made for them rather than having another made. Another account's
/// would be refused them: the gateway takes a key from one account alone.
struct Staged {
    /// The deployment's key line for the account (see [`pair::account_key`]).
    account: String,
    identity: Arc<Identity>,
}

/// The only writer of `state`.
struct Service {
    dir: PathBuf,
    account: Account,
    busy: Busy,
    held: Option<Held>,
    staged: Option<Staged>,
    // The connection, tunnel and fingerprint are read from `held` as it is
    // written.
    state: State,
    written: Option<State>,
}

impl Service {
    fn start(dir: &Path, account: Account, busy: Busy) -> Self {
        let mut state = State {
            connection: Connection::Unpaired,
            pairing_id: String::new(),
            pairing: None,
            tunnel_id: String::new(),
            fingerprint: String::new(),
        };
        if let Some(id) = Request::abandoned(dir) {
            warn!("A pairing was cut short by the end of an earlier run");
            state.pairing_id = id;
            state.pairing = Some(Outcome::Failed(LOCAL_FAILURE.into()));
        }
        let held = Identity::load(dir).and_then(|loaded| {
            loaded
                .map(|(identity, paired)| Held::start(paired, Arc::new(identity), &busy))
                .transpose()
        });
        let held = held.unwrap_or_else(|e| {
            warn!("This folder's tunnel cannot be held: {e:#}");
            None
        });
        Self {
            dir: dir.to_path_buf(),
            account,
            busy,
            held,
            staged: None,
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
                let (account, dir) = (self.account.clone(), self.dir.clone());
                let staged = self.staged.take();
                // Counted from when the app asked, as its wait is: a request
                // may have waited on a pairing before it.
                let deadline =
                    Instant::now() + PAIRING_DEADLINE.saturating_sub(Request::waited(&self.dir));
                let busy = self.busy.clone();
                let hold = busy.hold();
                let task = tokio::spawn(async move {
                    pair_with(&url, staged, account, deadline, &dir, &busy).await
                });
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

    fn settle(&mut self, id: String, result: Attempt) {
        Request::finish(&self.dir);
        self.state.pairing_id = id;
        self.state.pairing = Some(match result {
            Ok(held) => {
                info!("Paired");
                if let Some(old) = self.held.replace(held) {
                    let hold = self.busy.hold();
                    tokio::spawn(async move {
                        let _hold = hold;
                        old.retire().await;
                    });
                }
                Outcome::Done
            }
            Err((e, staged)) => {
                warn!("Pairing failed: {e}");
                self.staged = staged;
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
        self.state.fingerprint = self
            .held
            .as_ref()
            .map(|held| held.identity.fingerprint())
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

/// Pairs with the deployment whose pairing script is at `url`, on keys of
/// its own: the folder's may be another account's, and the gateway refuses
/// one key to two. The new tunnel is held beside whatever the folder holds
/// while the deployment checks it, and the folder takes the new keys and
/// record only once it has, so a pairing that fails leaves the machine as it
/// was. It gives up at `deadline`, before the app stops waiting,
/// so a pairing the app reported failed never lands after. Keys `staged` by
/// an earlier try for the same account are used again.
async fn pair_with(
    url: &str,
    staged: Option<Staged>,
    account: Account,
    deadline: Instant,
    dir: &Path,
    busy: &Busy,
) -> Attempt {
    // Kept staged keys stay kept until a try shows they are another account's.
    let account_key = match pair::account_key(url, deadline).await {
        Ok(account_key) => account_key,
        Err(e) => return Err((e, staged)),
    };
    let identity = match staged_for(staged, &account_key) {
        Some(identity) => identity,
        None => Arc::new(Identity::fresh(account).map_err(|e| (PairError::Local(e), None))?),
    };
    // Kept for a retry by the same account from here, whatever ends this try:
    // the deployment may already hold a tunnel for these keys, and one that
    // does not makes it for them then.
    let keep = || {
        Some(Staged {
            account: account_key.clone(),
            identity: identity.clone(),
        })
    };
    let offer = pair::offer(url, &identity, account_key.clone(), deadline)
        .await
        .map_err(|e| (e, keep()))?;
    let held = match Held::start(offer.paired.clone(), identity.clone(), busy) {
        Ok(held) => held,
        Err(e) => return Err((offer.stop(PairError::Local(e)).await, keep())),
    };
    pair::confirm(&offer).await.map_err(|e| (e, keep()))?;
    identity
        .adopt(&offer.paired, dir)
        .map_err(|e| (PairError::Local(e), keep()))?;
    Ok(held)
}

/// The keys `staged` holds, if they were staged for the account whose key
/// line is `account_key`.
fn staged_for(staged: Option<Staged>, account_key: &str) -> Option<Arc<Identity>> {
    staged
        .filter(|staged| staged.account == account_key)
        .map(|staged| staged.identity)
}

/// Until the pairing under way ends; never, with none.
async fn finished(pairing: &mut Option<Pairing>) -> Attempt {
    match pairing {
        Some((_, task, _)) => task
            .await
            .unwrap_or_else(|e| Err((PairError::Local(e.into()), None))),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_staged_for_an_account_are_used_again_only_for_it() -> Result<()> {
        let identity = Arc::new(Identity::fresh(Account::current()?)?);
        let staged = || {
            Some(Staged {
                account: "ssh-ed25519 AAAAaccountA".into(),
                identity: identity.clone(),
            })
        };
        let again = staged_for(staged(), "ssh-ed25519 AAAAaccountA");
        assert!(again.is_some_and(|again| Arc::ptr_eq(&again, &identity)));
        assert!(staged_for(staged(), "ssh-ed25519 AAAAaccountB").is_none());
        assert!(staged_for(None, "ssh-ed25519 AAAAaccountA").is_none());
        Ok(())
    }
}
