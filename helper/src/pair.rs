//! Pairs this machine through the endpoints the pairing command's script
//! reports to, so a deployment takes a helper as it is. The helper reports its
//! own host key and that it is reached only through a tunnel, the deployment
//! sets one up for it as it does for a machine behind a router, and once the
//! helper holds it, the deployment is asked to connect through it.

use std::fmt;
use std::time::Duration;

use anyhow::{Context, anyhow};
use tracing::{info, warn};

use crate::folder::{Identity, Paired};
use crate::keys;

// The first line of the report's 202 that asks the machine to connect out.
const TUNNEL_CUE: &str = "tunnel";
// An answer longer than this is not a sentence for the person who asked, but
// something like a proxy's error page.
const SENTENCE_LIMIT: usize = 500;
/// What a failure on this machine shows; its cause is in the log.
pub const LOCAL_FAILURE: &str = "This Mac could not finish pairing. Its log has the details.";

#[derive(Debug)]
pub enum PairError {
    /// The deployment refused, in its own sentence.
    Deployment(String),
    /// The deployment's host, which could not be reached.
    Unreachable(String),
    Local(anyhow::Error),
}

impl PairError {
    /// What the person who asked is shown. Never this machine's own errors,
    /// which name its paths and are kept in the log instead.
    pub fn user_message(&self) -> String {
        match self {
            Self::Deployment(sentence) => sentence.clone(),
            Self::Unreachable(host) => format!("Could not reach LangAlpha at {host}."),
            Self::Local(_) => LOCAL_FAILURE.into(),
        }
    }
}

impl fmt::Display for PairError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deployment(sentence) => write!(f, "the deployment refused: {sentence}"),
            Self::Unreachable(host) => write!(f, "could not reach {host}"),
            Self::Local(e) => write!(f, "{e:#}"),
        }
    }
}

type Form = Vec<(&'static str, String)>;

/// The tunnel the deployment set up for this machine, for it to hold before
/// the deployment is asked to connect through it.
pub struct Offer {
    run: Run,
    report: Form,
    pub paired: Paired,
}

impl Offer {
    /// Tells the deployment this run stopped, and why.
    pub async fn stop(&self, e: PairError) -> PairError {
        self.run.stop(e).await
    }
}

/// Takes the deployment's key, reports the machine, and asks for the tunnel
/// the deployment's 202 says to connect out through.
pub async fn offer(url: &str, identity: &Identity) -> Result<Offer, PairError> {
    let base = url
        .strip_suffix("/script")
        .ok_or_else(|| PairError::Local(anyhow!("{url} is not a pairing script's URL")))?;
    let (code, body) = call(format!("{base}/key"), None).await?;
    let authorized_key = authorized_key(code, &body)?;

    let run = Run {
        base: base.into(),
        id: format!("{:016x}", rand::random::<u64>()),
    };
    let hostname = hostname();
    let report = report(identity, &hostname, &run.id).map_err(PairError::Local)?;
    info!("Asking the deployment to connect");
    let (code, body) = run.post("report", report.clone()).await?;
    cue(code, &body)?;

    info!("Asking the deployment for a tunnel");
    // From here the deployment's dialog waits on this run, so a failure is
    // told to it.
    let paired = async {
        let public_key = identity.tunnel_key_line().map_err(PairError::Local)?;
        let form = vec![("public_key", public_key), ("hostname", hostname)];
        let (code, answer) = run.post("tunnel", form).await?;
        tunnel(code, &answer, authorized_key)
    }
    .await;
    match paired {
        Ok(paired) => Ok(Offer {
            run,
            report,
            paired,
        }),
        Err(e) => Err(run.stop(e).await),
    }
}

/// Asks the deployment to connect through the tunnel, which is being held.
pub async fn confirm(offer: &Offer) -> Result<(), PairError> {
    info!("Asking the deployment to connect through the tunnel");
    let mut form = offer.report.clone();
    form.push(("tunnel_id", offer.paired.tunnel_id.clone()));
    let (code, body) = offer.run.post("report", form).await?;
    match confirmed(code, &body) {
        // A refusal settled the pairing on the deployment's side. The cue
        // again left it waiting on a tunnel, so it is told this run stopped.
        Err(e @ PairError::Local(_)) => Err(offer.stop(e).await),
        other => other,
    }
}

/// One run of the pairing, as the deployment follows it.
struct Run {
    base: String,
    id: String,
}

impl Run {
    async fn post(&self, path: &str, form: Form) -> Result<(u16, String), PairError> {
        call(format!("{}/{path}", self.base), Some(form)).await
    }

    /// Tells the deployment this run stopped and why, so its dialog says so
    /// too rather than waiting until the command expires. A deployment that
    /// could not be reached is not told.
    async fn stop(&self, e: PairError) -> PairError {
        if !matches!(e, PairError::Unreachable(_)) {
            let _ = self.post("report", stopped(&self.id, &e)).await;
        }
        e
    }
}

fn report(identity: &Identity, hostname: &str, run: &str) -> anyhow::Result<Form> {
    let os = if cfg!(target_os = "macos") {
        "Darwin"
    } else {
        "Linux"
    };
    Ok(vec![
        ("user", identity.account.name.clone()),
        ("os", os.into()),
        ("hostname", hostname.into()),
        ("host_keys", identity.host_key_line()?),
        ("run", run.into()),
        // No sshd here to dial: the helper answers only through its tunnel.
        ("tunnel_only", "1".into()),
    ])
}

/// The report that stops a run: the sentence alone, beside the run it stops.
fn stopped(run: &str, e: &PairError) -> Form {
    vec![("failed", e.user_message()), ("run", run.into())]
}

/// The line the deployment logs in with, from its answer to `/key`.
fn authorized_key(code: u16, body: &str) -> Result<String, PairError> {
    if code != 200 {
        return Err(refused(code, body));
    }
    if body.contains('\n') || keys::key_in_line(body).is_none() {
        return Err(PairError::Local(anyhow!(
            "the deployment's key could not be read"
        )));
    }
    Ok(body.into())
}

/// Whether the first report's answer asks this machine to connect out, the
/// only way a deployment reaches it.
fn cue(code: u16, body: &str) -> Result<(), PairError> {
    match code {
        202 if body.lines().next() == Some(TUNNEL_CUE) => Ok(()),
        200..=299 => Err(PairError::Local(anyhow!(
            "the deployment answered {code} without asking for a tunnel: {body}"
        ))),
        _ => Err(refused(code, body)),
    }
}

/// The tunnel the deployment set up, from its answer to `/tunnel`.
fn tunnel(code: u16, answer: &str, authorized_key: String) -> Result<Paired, PairError> {
    if code != 200 {
        return Err(refused(code, answer));
    }
    Paired::new(answer, authorized_key)
        .context("reading the deployment's answer")
        .map_err(PairError::Local)
}

/// Whether the confirming report's answer says the deployment reached this
/// machine through its tunnel.
fn confirmed(code: u16, body: &str) -> Result<(), PairError> {
    match code {
        200 => Ok(()),
        // The cue again: the tunnel was gone by the time the deployment
        // looked for it, and this run has set up the only one it can.
        202 => Err(PairError::Local(anyhow!(
            "the deployment lost the tunnel it set up and asked for another"
        ))),
        _ => Err(refused(code, body)),
    }
}

/// The deployment's refusal in its own sentence, where it gave one.
fn refused(code: u16, body: &str) -> PairError {
    PairError::Deployment(if body.is_empty() || body.len() > SENTENCE_LIMIT {
        format!("LangAlpha could not pair this Mac (error {code}). Try again.")
    } else {
        body.into()
    })
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::new();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

fn agent(url: &str) -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        // A deployment reached over https is never left for http on a
        // redirect, which would carry the key and the gateway's pin in clear.
        .https_only(url.starts_with("https://"))
        // A report waits while the deployment dials the machine.
        .timeout_global(Some(Duration::from_secs(150)))
        // The system store is what this Mac and the app already trust, so a
        // private or company-inspected root works here too.
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .root_certs(ureq::tls::RootCerts::PlatformVerifier)
                .build(),
        )
        .build()
        .into()
}

/// The host a URL names, for saying which one could not be reached.
fn host(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split('/').next().unwrap_or(rest)
}

/// A GET, or a POST of `form`, answered with its status and trimmed body.
async fn call(url: String, form: Option<Form>) -> Result<(u16, String), PairError> {
    let host = host(&url).to_string();
    let answer = tokio::task::spawn_blocking(move || -> Result<(u16, String), ureq::Error> {
        let mut response = match form {
            Some(form) => agent(&url).post(&url).send_form(form)?,
            None => agent(&url).get(&url).call()?,
        };
        let body = response.body_mut().read_to_string()?;
        Ok((response.status().as_u16(), body.trim().to_string()))
    })
    .await
    .map_err(|e| PairError::Local(e.into()))?;
    answer.map_err(|e| {
        warn!("Could not reach {host}: {e}");
        PairError::Unreachable(host)
    })
}

#[cfg(test)]
mod tests {
    use russh::keys::{Algorithm, PrivateKey};

    use super::*;

    // The deployment's answers, as `ssh_pairing.flow` words them.
    const ASKED_OUT: &str = "tunnel\nThis machine cannot be reached from the internet, so it \
                             will connect out to LangAlpha instead.";
    const EXPIRED: &str =
        "This command has expired. Copy a new one from LangAlpha and run it again.";

    fn key_line() -> String {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap();
        key.public_key().to_openssh().unwrap()
    }

    #[test]
    fn takes_the_deployments_key_line() {
        let line = format!("no-agent-forwarding,no-X11-forwarding {}", key_line());
        assert_eq!(authorized_key(200, &line).unwrap(), line);
        assert!(matches!(
            authorized_key(200, "<html>"),
            Err(PairError::Local(_))
        ));
        let expired = authorized_key(404, EXPIRED).unwrap_err();
        assert_eq!(expired.user_message(), EXPIRED);
    }

    #[test]
    fn connects_out_only_on_the_cue() {
        assert!(cue(202, ASKED_OUT).is_ok());
        assert!(matches!(cue(202, "other"), Err(PairError::Local(_))));
        assert!(matches!(cue(200, "Connected."), Err(PairError::Local(_))));
        let refused = cue(
            422,
            "This server has no tunnel for machines it cannot reach.",
        );
        assert_eq!(
            refused.unwrap_err().user_message(),
            "This server has no tunnel for machines it cannot reach."
        );
    }

    #[test]
    fn reads_the_tunnel_the_deployment_set_up() {
        let pin = format!("[gw.example.com]:2222 {}", key_line());
        let answer = format!(
            "tunnel_id=7c9e6679-7425-40de-944b-e07fc1f90ae7\ngateway_host=gw.example.com\n\
             gateway_port=2222\nknown_hosts={pin}"
        );
        let authorized = key_line();
        let paired = tunnel(200, &answer, authorized.clone()).unwrap();
        assert_eq!(
            paired,
            Paired {
                tunnel_id: "7c9e6679-7425-40de-944b-e07fc1f90ae7".into(),
                gateway_host: "gw.example.com".into(),
                gateway_port: 2222,
                gateway_pin: pin,
                authorized_key: authorized.clone(),
            }
        );
        let taken = "This account already has 5 machines connecting out to LangAlpha.";
        let refused = tunnel(409, taken, authorized.clone()).unwrap_err();
        assert_eq!(refused.user_message(), taken);
        let garbled = tunnel(200, "tunnel_id=x", authorized).unwrap_err();
        assert_eq!(garbled.user_message(), LOCAL_FAILURE);
    }

    #[test]
    fn fails_a_confirmation_that_asks_for_a_tunnel_again() {
        assert!(confirmed(200, "Connected. Go back to LangAlpha to add mac.").is_ok());
        let again = confirmed(202, ASKED_OUT).unwrap_err();
        assert_eq!(again.user_message(), LOCAL_FAILURE);
        let missed = "LangAlpha could not reach this machine through its tunnel.";
        assert_eq!(confirmed(422, missed).unwrap_err().user_message(), missed);
    }

    #[test]
    fn says_little_of_what_went_wrong_here() {
        let page = format!("<html>{}</html>", "x".repeat(SENTENCE_LIMIT));
        assert_eq!(
            refused(502, &page).user_message(),
            "LangAlpha could not pair this Mac (error 502). Try again."
        );
        assert_eq!(
            PairError::Unreachable(host("https://example.com:8443/api/script").into())
                .user_message(),
            "Could not reach LangAlpha at example.com:8443."
        );
        let local = PairError::Local(anyhow!("writing /Users/me/x: Permission denied"));
        assert_eq!(local.user_message(), LOCAL_FAILURE);
    }

    #[test]
    fn reports_a_machine_reached_only_through_its_tunnel() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let identity = Identity::load(dir.path())?;
        let form = report(&identity, "mac", "0123456789abcdef")?;
        let named = |name| {
            form.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(named("tunnel_only"), Some("1"));
        assert_eq!(named("can_tunnel"), None);
        assert_eq!(named("run"), Some("0123456789abcdef"));
        assert_eq!(named("host_keys"), Some(identity.host_key_line()?.as_str()));
        Ok(())
    }

    #[test]
    fn stops_a_run_with_the_sentence_shown_here() {
        let local = PairError::Local(anyhow!("the deployment lost the tunnel"));
        assert_eq!(
            stopped("0123456789abcdef", &local),
            vec![
                ("failed", LOCAL_FAILURE.to_string()),
                ("run", "0123456789abcdef".to_string())
            ]
        );
    }
}
