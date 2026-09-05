'use strict'

const http = require('node:http')
const { randomUUID } = require('node:crypto')
const { shell } = require('electron')
const origins = require('./origins')
const { navigate } = require('./navigate')
const { forDisplay } = require('./redact')

// ---------------------------------------------------------------------------
// System-browser OAuth (RFC 8252), intercepted in the shell.
//
// Google returns disallowed_useragent for OAuth inside an embedded webview
// (confirmed live; a user-agent spoof was tried and failed), so the authorize
// URL has to open in the user's real browser and the code has to come back over
// a loopback listener.
//
// The shell does this without either SPA knowing. It catches the authorize
// navigation, swaps redirect_to for the loopback, and when the code arrives
// sends the window that started the flow to the redirect_to it had ORIGINALLY
// asked for, with the code appended. That lands on each app's own callback
// route, where @supabase/ssr (flowType pkce, detectSessionInUrl on) redeems it
// against the verifier already sitting in that renderer's cookie jar.
//
// INVARIANT: the exchange happens in the renderer, never here. The PKCE
// verifier never leaves the cookie jar that created it, which is also what makes
// the loopback hop safe: anything listening on that port gets a code it cannot
// redeem.
//
// The same listener carries a second, unrelated flow: connecting a third-party
// MCP server whose authorization server allowlists only the native-app profile
// and refuses a hosted callback outright. That one is not intercepted. The page
// asks for the loopback URI up front (`beginMcp`), passes it to its own backend,
// and navigates to the authorize URL it gets back like any other outbound link;
// the shell's part is holding the listener and driving the window to the app's
// own callback when the code lands. The invariant above holds there too, more
// strongly: that verifier lives on the backend, so the code in transit here is
// redeemable by nothing on this machine at all.
// ---------------------------------------------------------------------------

// The port is the operating system's to choose (RFC 8252 7.3: an authorization
// server "MUST allow any port to be specified at the time of the request for
// loopback IP redirect URIs"). Asking for a free one is what every native OAuth
// client does, and it is the only way to be sure of getting one: three
// hand-picked numbers were three chances for something else on the machine to
// have taken them first, and when all three were gone the shell could not sign
// anybody in until it was restarted.
//
// It costs a wildcard entry on the Supabase side, since sign-in's redirect_to is
// matched against its Redirect URLs allowlist. A connector's authorization
// server was never the constraint: it matches loopback as a pattern, and this
// deployment's own backend allowlists any loopback port at or above 1024
// (`sanitize_loopback_redirect`).
const EPHEMERAL_PORT = 0
// Matched to the backend's own STATE_TTL_SECONDS. A shell that gives up first
// can only ever lose a flow the server would still have honoured, and brokerage
// consent behind 2FA or an approval push routinely runs past five minutes.
const FLOW_TIMEOUT_MINUTES = 10
const FLOW_TIMEOUT_MS = FLOW_TIMEOUT_MINUTES * 60_000

// The listener answers exactly these, and the path is what says which KIND of
// flow a callback belongs to. The two have nothing in common but the port, and
// a code for one is not redeemable by the other, so they are kept in separate
// stores rather than sharing one slot.
const SIGNIN_CALLBACK_PATH = '/callback'
const MCP_CALLBACK_PATH = '/mcp/callback'
const FLOW_PATHS = { [SIGNIN_CALLBACK_PATH]: 'signin', [MCP_CALLBACK_PATH]: 'connector' }

let callbackPort = null
// The sign-in slot, and it is exclusive: signing in takes the window over, so a
// second one from that window can only be the user starting again.
let pending = null
// Connector flows, which are not exclusive. The Plugins tab leaves every other
// row clickable while one connect is running, and the authorize URL opens in the
// system browser without the window moving, so two consent screens open at once
// is ordinary use rather than a mistake. Unlike a sign-in, a connector callback
// carries the `state` that says which flow it belongs to, so they can be told
// apart on arrival: one listener was never the constraint, one slot was.
let connectors = new Map()
// A page cannot have a meaningful number of consent screens open at once, and
// each armed flow holds a timer. Refusing past a sane bound keeps a page that
// arms in a loop from growing this without end.
const MAX_CONNECTOR_FLOWS = 8
let server = null
// The in-flight listener attempt, so two connects arming at once do not each
// open a server and leave one of them orphaned.
let starting = null
// The connector handoff on its way to the backend, or null when the window is
// free. There is one window, so two callbacks landing together are two loads of
// it and the second cancels the first -- which `navigate` treats as routine,
// because for a page load it is. Here the load IS the delivery: the backend
// only hears about a flow when this window asks for its callback URL, so a
// cancelled navigation is an authorization code that is never redeemed, on a
// flow whose browser tab has already been told it worked. So they queue.
let handoff = null
// How long the next handoff waits on the one ahead of it. A delivery that never
// settles must not hold the queue forever -- that swallows every callback
// behind it rather than racing one, which is worse than not queueing at all.
// Past this the two navigations race exactly as they used to, and only after a
// wait no real load takes.
const HANDOFF_TIMEOUT_MS = 30_000

/**
 * Supabase's authorize endpoint, matched by shape rather than by host: the shell
 * is not told which Supabase project the web build points at, and a self-hoster
 * brings their own.
 */
function isAuthorizeUrl(url) {
  try {
    const u = new URL(url)
    return (
      (u.protocol === 'https:' || u.protocol === 'http:') &&
      u.pathname.endsWith('/auth/v1/authorize') &&
      u.searchParams.has('redirect_to')
    )
  } catch {
    return false
  }
}

// What the tab in the browser is told. Named per flow because it is the only
// thing the person standing in the browser ever sees, and "Signed in" for a
// connector they were authorizing is the same lie in a smaller size.
const FLOW_TITLES = {
  signin: ['Signed in', 'Sign-in failed'],
  // Not 'Connected': at this point the shell is holding a code it has not yet
  // handed to the backend, so issuer validation, the token exchange, catalog
  // revalidation and the write are all still ahead. Claiming success here left
  // this tab contradicting the app whenever one of those failed.
  connector: ['Authorization received', 'Connection failed'],
}

function donePage(ok, detail, kind = 'signin') {
  const [good, bad] = FLOW_TITLES[kind] || FLOW_TITLES.signin
  const title = ok ? good : bad
  const body = ok
    ? 'You can close this tab and return to LangAlpha.'
    : detail || 'Something went wrong.'
  return `<!doctype html><meta charset="utf-8"><title>${title}</title>
  <body style="margin:0;display:grid;place-items:center;height:100vh;background:#191919;color:#e8e8e8;
               font:15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
    <div style="text-align:center"><h1 style="font-size:19px;margin:0 0 8px">${escapeHtml(title)}</h1>
    <p style="margin:0;opacity:.65">${escapeHtml(body)}</p></div></body>`
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ))
}

/**
 * Did a browser NAVIGATE here, rather than fetch this as part of some other page?
 *
 * The provider returns by sending the user's browser to this URL, which every
 * browser labels `document`. Any other value is a page on the open web reaching
 * a loopback port as a subresource, and `<img src="http://127.0.0.1:<port>/callback
 * ?error=x">` is the whole attack: no-cors means the attacker never has to read
 * the reply, because the damage is the side effect. The flow is consumed and the
 * window that was signing in is driven to a failure it never had.
 *
 * Requiring a parameter the attacker also supplies cannot separate the two; how
 * the request was made can. Absent is allowed and must stay allowed: that is
 * curl, a non-browser client, or a browser too old to send the header, and a web
 * page cannot suppress it from inside a browser that does.
 *
 * This does not address a hostile process on the same machine, which can send
 * anything. Nothing observable at this port can, which is why the PKCE invariant
 * above is what actually bounds that case.
 */
function isProviderNavigation(headers) {
  const dest = headers['sec-fetch-dest']
  return !dest || dest === 'document'
}

function handleCallback(req, res) {
  // A fixed base rather than `callbackPort`: only the path and query are read
  // here, and an in-flight keep-alive request being served while the listener is
  // torn down would otherwise build `http://127.0.0.1:null` and throw out of an
  // http handler.
  const url = new URL(req.url, 'http://127.0.0.1')
  const code = url.searchParams.get('code')
  // Kept apart, because they answer different questions and only one of them is
  // a wire value. `error` is the OAuth code the backend classifies on -- an
  // `access_denied` is the user pressing Cancel, not a provider fault -- and
  // `detail` is prose for the tab in the browser. Folding them lost the
  // classification and reported every cancel as a provider error.
  const error = url.searchParams.get('error')
  const detail = url.searchParams.get('error_description') || error
  const kind = FLOW_PATHS[url.pathname]

  // Carrying neither parameter is what disqualifies a request, not just the
  // path: the provider always comes back with one or the other, and anything on
  // this machine can reach a loopback port. A favicon fetch, a probe, or the URL
  // left in a browser tab would otherwise consume the flow the real callback is
  // still on its way to complete, and send the window to a 'sign-in failed' for
  // a sign-in that was fine.
  if (!kind || (!code && !detail)) {
    res.writeHead(404)
    return res.end()
  }

  // Every decision below reads a parameter with `get()`, which answers the
  // FIRST value, while the forward loop rebuilds the query with `set()`, which
  // keeps the LAST. A repeated parameter is therefore a callback this shell
  // approves on one value and hands the backend another -- `?state=<bound>&
  // state=<theirs>` passes the check below and is redeemed against neither.
  // Rejecting the whole ambiguous query beats naming the sensitive parameters:
  // that list would have to be kept in step with whatever the backend reads
  // next, and a real authorization server never sends one twice anyway.
  const names = [...url.searchParams.keys()]
  if (names.length !== new Set(names).size) {
    console.warn('[auth] discarding a callback whose query repeats a parameter')
    res.writeHead(404)
    return res.end()
  }

  if (!isProviderNavigation(req.headers)) {
    console.warn(`[auth] ignoring a callback fetched as '${req.headers['sec-fetch-dest']}'`)
    res.writeHead(404)
    return res.end()
  }

  const answer = (ok, detail) => {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
    res.end(donePage(ok, detail, kind))
  }

  // Answered after the flow is resolved rather than before it. A code that
  // arrives for no flow is discarded, and writing the page first told that tab
  // "Signed in" while the app was told nothing at all: the two surfaces the user
  // is looking at disagreed, and the browser was the one that lied.
  const stale = 'This is no longer the flow in progress. Return to LangAlpha and start it again.'

  // WHICH flow this code belongs to, asked before anything is spent.
  //
  // A connector names itself with the `state` its authorization server returns,
  // so several can be in flight and each code still finds its own. Matching on
  // 'a connector flow' instead was the bug this replaced: a late callback for
  // the first connect was handed to the second, completing a connection the user
  // was no longer waiting on and taking the second one's slot on the way.
  //
  // An unbound flow matches nothing, deliberately: it has no authorize URL in
  // the world yet, so no callback for it can exist. That is also what stops a
  // page on the open web from spending a flow by navigating this port with an
  // invented `?error=`, which `sec-fetch-dest: document` cannot tell from the
  // real thing. `state` is returned on an error response too (RFC 6749 4.1.2.1),
  // so this costs a real provider nothing.
  //
  // A sign-in has no such name and needs none: it owns the one slot, so a second
  // one started from this window has already replaced it.
  const flow = kind === 'connector'
    ? connectorFlowFor(url.searchParams.get('state'))
    : (pending && pending.kind === 'signin' ? pending : null)
  if (!flow) {
    console.warn(`[auth] a ${kind} callback arrived for no flow in progress`)
    return answer(false, stale)
  }

  if (flow === pending) pending = null
  else connectors.delete(flow.id)
  clearTimeout(flow.timer)
  answer(!!code && !error, detail)
  // `provider: true` separates what the authorization server said from what
  // this shell decided on its own (a timeout, a supersede). Only the first is
  // an outcome worth reporting onward as one.
  //
  // `params` verbatim rather than a hand-picked few: the backend declares more
  // of them than this file should have to keep in step (`iss` is the one that
  // was already missing, and dropping it makes a server that identifies itself
  // fail closed), and it ignores what it does not declare.
  flow.finish({ code, error, detail, params: url.searchParams, provider: true })
}

/**
 * Deliver one connector callback to the backend, and only one at a time.
 *
 * The step runs at the front of the queue rather than being decided when the
 * callback lands, because everything it asks about the window -- whether the
 * window has moved on, where to send it back to -- is only true at the moment
 * it acts.
 */
function handOff(step) {
  // Straight through when the window is free, which is every ordinary case: one
  // flow finishing on its own is not waiting on anybody, and putting it on a
  // microtask would only move the shell's own timers a tick later than the
  // moment they fire.
  const done = handoff ? handoff.then(step, step) : Promise.resolve(step())
  let timer
  const queued = Promise.race([
    done.catch(() => {}),
    new Promise((resolve) => {
      timer = setTimeout(resolve, HANDOFF_TIMEOUT_MS)
    }),
  ]).then(() => {
    clearTimeout(timer)
    // Empty the queue behind the last one, so the next lone handoff goes
    // straight through as well.
    if (handoff === queued) handoff = null
  })
  handoff = queued
  return done
}

/** The armed connector flow that named this `state`, or null if none did. */
function connectorFlowFor(state) {
  if (!state) return null
  for (const flow of connectors.values()) if (flow.state === state) return flow
  return null
}

// Bumped by every stop, so a bind that lands afterwards knows it was already
// asked to go away. `stopCallbackServer` can only close a server it has, and a
// start still in flight has none to hand it: the listener came up seconds later
// with nothing waiting on it and nobody left to close it. No caller could reach
// that until `begin` started one without awaiting it.
let era = 0

function startCallbackServer() {
  const mine = era
  return new Promise((resolve) => {
    const srv = http.createServer(handleCallback)
    const failed = (err) => {
      // No fallback to try in the same attempt: the port is the OS's to pick, so
      // there is no second number to reach for. The conditions that get here --
      // no descriptors, no loopback to bind, a sandbox refusing the socket -- are
      // the machine's rather than ours, which is why the retry belongs to the
      // next caller (`ensureCallbackServer`) and not to a timer here.
      console.error(`[auth] could not open a callback listener: ${err.code || err.message}`)
      resolve(null)
    }
    srv.once('error', failed)
    srv.listen(EPHEMERAL_PORT, '127.0.0.1', () => {
      // Only a *bind* failure ends the attempt. Left attached, a later socket
      // error would resolve this promise a second time and report a listener
      // that is still up as never having opened.
      srv.removeListener('error', failed)
      // Replaced, never just removed: a listening server with no 'error'
      // listener *throws* on the next socket error, which ends the app.
      srv.on('error', (err) => console.error(`[auth] callback server: ${err.code || err.message}`))
      if (mine !== era) {
        srv.close()
        return resolve(null)
      }
      server = srv
      callbackPort = srv.address().port
      console.log(`[auth] callback listening on http://127.0.0.1:${callbackPort}/callback`)
      resolve(callbackPort)
    })
  })
}

/**
 * The listening port, opening one now if the last attempt did not get one.
 *
 * Startup opens the listener once, and a failure there used to be latched:
 * `callbackPort` stayed null for the life of the process and every flow in that
 * session was refused for a condition that may have cleared in seconds. Retrying
 * on demand is cheap and costs one bind, so no caller has to inherit the state
 * the app happened to boot into.
 */
function ensureCallbackServer() {
  if (callbackPort) return Promise.resolve(callbackPort)
  // Shared rather than per-caller: two rows connecting at once would otherwise
  // each open a listener, and the second would repoint `callbackPort` while the
  // authorize URL already in the browser still names the first.
  if (!starting) starting = startCallbackServer().finally(() => { starting = null })
  return starting
}

/**
 * Release the port and drop every flow waiting on it. No listener means no way
 * for a code to arrive, so a flow left armed could only ever time out.
 */
function stopCallbackServer() {
  era += 1
  if (pending) {
    clearTimeout(pending.timer)
    pending = null
  }
  for (const flow of connectors.values()) clearTimeout(flow.timer)
  connectors.clear()
  // No flow can come home to a listener that is down, so there is no delivery
  // left for the next one to queue behind. Left set, a handoff still loading
  // when the listener stopped would hold the first callback after the next
  // listener comes up.
  handoff = null
  if (!server) return
  server.close()
  server = null
  callbackPort = null
}

/** Append a query param to a URL that may already carry one. */
function withParam(rawUrl, key, value) {
  const u = new URL(rawUrl)
  u.searchParams.set(key, value)
  return u.toString()
}

/** The shape both kinds share: what a flow is, to anyone who has to name it. */
function makeFlow(kind, win, finish) {
  return {
    kind,
    finish,
    // What this flow is, to anyone who has to name it later. The renderer holds
    // it across the round trip that mints the flow, and both of the things it
    // can ask for afterwards -- bind this state to it, stand it down -- are
    // refused unless they name the flow they mean. Without it the only handle
    // is 'whatever this window has pending', which is a different flow the
    // moment the user clicks a second broker.
    id: randomUUID(),
    // The authorization server's `state`, once the backend has minted the flow
    // and the page has handed it back. Null until then, and a connector flow
    // that has not been bound accepts no callback at all: no authorize URL was
    // ever issued for it, so nothing legitimate can arrive.
    state: null,
    // Which window this flow belongs to, so a second one from the same window
    // can be told apart from a second one somewhere else.
    win,
    timer: null,
  }
}

/**
 * Take the single sign-in slot, and start the flow's clock.
 *
 * A second sign-in from the SAME window is the user clicking the button again,
 * not an outcome to report: that window has not moved, so telling the displaced
 * flow would drive it to a failure while the one they just started is still
 * running. The new flow owns it.
 */
function armSignin(win, finish) {
  if (pending) {
    clearTimeout(pending.timer)
    if (pending.win !== win) pending.finish({ error: 'superseded by another flow' })
    pending = null
  }
  const flow = makeFlow('signin', win, finish)
  flow.timer = setTimeout(() => {
    if (pending !== flow) return
    pending = null
    flow.finish({ error: `timed out after ${FLOW_TIMEOUT_MINUTES} minutes` })
  }, FLOW_TIMEOUT_MS)
  pending = flow
  return flow
}

/**
 * Open a slot for a connector flow. Returns null when there is no room.
 *
 * Connector flows do NOT displace one another. Each callback names its own flow
 * by `state`, so several can be in flight without ambiguity -- and displacing
 * was never survivable here the way it is for a sign-in: the window does not
 * move when a connect starts, so the loser sat on a spinner while its consent
 * screen went on collecting a grant nothing would ever redeem.
 */
function armConnector(win, finish) {
  if (connectors.size >= MAX_CONNECTOR_FLOWS) {
    console.warn(`[auth] refusing a connector flow: ${connectors.size} are already armed`)
    return null
  }
  const flow = makeFlow('connector', win, finish)
  startConnectorClock(flow)
  connectors.set(flow.id, flow)
  return flow
}

/** Start, or restart, the window a connector flow has to come home in. */
function startConnectorClock(flow) {
  clearTimeout(flow.timer)
  flow.timer = setTimeout(() => {
    if (connectors.get(flow.id) !== flow) return
    connectors.delete(flow.id)
    flow.finish({ error: `timed out after ${FLOW_TIMEOUT_MINUTES} minutes` })
  }, FLOW_TIMEOUT_MS)
}

/**
 * Take over an authorize navigation. `win` is the window that tried to make it,
 * and the one the code is handed back to.
 *
 * Returns false when the flow is not ours to take, in which case the caller must
 * let the navigation proceed normally.
 */
function begin(rawUrl, win) {
  const authorize = new URL(rawUrl)
  const originalRedirect = authorize.searchParams.get('redirect_to')

  // The source, not only the destination. `isAuthorizeUrl` is host-agnostic on
  // purpose, so a self-hoster can bring their own Supabase project, and
  // `setWindowOpenHandler` sends every `window.open` through here. Without this,
  // anything the shell renders could hand us an authorize URL on a host of its
  // choosing and have the flow claimed: superseding a sign-in already in flight,
  // or steering this window to a path of its choosing on our own origin, since
  // `isOurs` answers for the origin and not the path. A real sign-in click only
  // ever comes from a page we serve.
  const from = win.isDestroyed() ? '' : win.webContents.getURL()
  if (!origins.isOurs(from)) {
    console.warn(`[auth] an authorize navigation from '${forDisplay(from)}' is not ours to take`)
    return false
  }

  // Only redirect back into an app we own. Without this the shell would happily
  // drive its own window to wherever a crafted authorize URL pointed, using a
  // code the user just authorized. Asked first, because everything below claims
  // the navigation and a flow that is not ours has to stay unclaimed.
  if (!origins.isOurs(originalRedirect)) {
    console.warn(`[auth] redirect_to '${originalRedirect}' is not one of ours; not intercepting`)
    return false
  }

  // Ours, and unserviceable. Declining here would let the navigation proceed as
  // 'external', which opens the authorize URL in the system browser: the flow
  // then completes into a browser profile holding none of the PKCE verifier this
  // renderer just minted, so it cannot be redeemed and the window is never told
  // why. Claim it and say so.
  //
  // Reads the port rather than awaiting one: `decide` in main answers Electron's
  // navigation handlers, which take a verdict in the same tick and no promise, so
  // this flow is refused either way. Start the listener anyway and let it land
  // after the refusal -- otherwise the boot failure stays latched for the life of
  // the process exactly as it used to, and a user who only ever signs in never
  // reaches the one path (`beginMcp`) that retries. What the message owes them is
  // the way in that needs no listener at all; the click after it may well work.
  if (!callbackPort) {
    console.error('[auth] no loopback listener; refusing the flow rather than sending it somewhere it cannot finish')
    ensureCallbackServer().catch(() => {})
    // Claimed either way: a window that closed under us still must not have its
    // authorize URL handed to a browser that cannot finish it.
    if (win.isDestroyed()) return true
    navigate(win, withParam(originalRedirect, 'error',
      'Sign-in with a provider could not start: this machine would not give the app a local port '
      + 'to listen on. Signing in with your email and password still works.'))
    return true
  }

  authorize.searchParams.set('redirect_to', `http://127.0.0.1:${callbackPort}/callback`)

  // Where the window was when the flow started, so the two paths that end a flow
  // without the user asking can tell "still waiting to sign in" from "gave up
  // and went back to work". Turns here run for minutes.
  const startedAt = win.webContents.getURL()

  const finish = ({ code, error, detail }) => {
    if (win.isDestroyed()) return
    // The provider's prose when there is some, this shell's own words otherwise
    // (a timeout and a supersede carry no description). Sign-in shows whichever
    // reached it; only a connector flow needs the bare code kept separate.
    const reason = detail || error
    // A code means the user just completed a sign-in and is coming back for the
    // result, so that lands wherever they are. An error is the shell's own
    // timer or a second flow talking, and neither is worth throwing away a page
    // the user moved on to: ten minutes is shorter than a research turn, and
    // `superseded` needs no network round trip at all, so any page in the shell
    // could force a navigation just by starting two flows.
    if (!code && win.webContents.getURL() !== startedAt) {
      console.warn(`[auth] dropping '${reason}': the window has moved on`)
      return
    }
    win.show()
    win.focus()
    // Hand the result to the app's own callback route either way: it already
    // renders a signing-in state and knows where to go next, and an error shown
    // in the app beats a dead-end page in the browser.
    const target = code
      ? withParam(originalRedirect, 'code', code)
      : withParam(originalRedirect, 'error', reason || 'sign-in failed')
    navigate(win, target)
  }

  const flow = armSignin(win, finish)

  console.log('[auth] handing the authorize URL to the system browser')
  shell.openExternal(authorize.toString()).catch((err) => {
    console.error(`[auth] the system browser refused the authorize URL: ${err.message}`)
    // Nothing is ever coming back: no browser opened, so no callback will. The
    // in-app navigation was already prevented on the strength of this flow
    // starting, so leaving it pending is ten minutes of a window that silently
    // refused to go anywhere, ending in a timeout that blames the user's wait.
    if (pending !== flow) return
    clearTimeout(flow.timer)
    pending = null
    flow.finish({ error: 'could not open your browser' })
  })
  return true
}

/**
 * Arm the listener for a connector's OAuth and answer with the loopback URI the
 * page should ask its backend to mint the flow against. `returnUrl` is that
 * backend's own callback, where the code is driven when it lands.
 *
 * Nothing is taken over here, unlike `begin`. The page still calls its backend
 * and still navigates to the authorize URL it gets back, which the ordinary
 * 'external' verdict opens in the browser; the shell contributes the listener
 * and the trip home. That split is forced rather than chosen: the redirect_uri
 * is bound into the flow when the backend mints it and is checked again at the
 * token exchange, so a value this shell substituted in transit would be a
 * mismatch at the far end rather than a fix.
 *
 * Returns null when there is nothing to offer. What that means is the caller's
 * to decide: inside the shell a hosted callback cannot come home at all, so the
 * page treats it as a refusal rather than a fallback.
 */
async function beginMcp(returnUrl, win) {
  // Same two questions `begin` asks, and for the same reason: any page the shell
  // renders can call this, so neither the asker nor the destination may be taken
  // on trust. Without them a page could park a flow that drives this window to a
  // URL of its choosing, carrying a code the user just authorized.
  //
  // Asked before the listener is raised, so a caller we are going to decline
  // never gets a loopback port opened on its behalf.
  const from = win.isDestroyed() ? '' : win.webContents.getURL()
  if (!origins.isOurs(from)) {
    console.warn(`[auth] a connector flow from '${forDisplay(from)}' is not ours to take`)
    return null
  }
  if (!origins.isOurs(returnUrl)) {
    console.warn(`[auth] connector return '${forDisplay(returnUrl)}' is not one of ours; declining`)
    return null
  }

  const port = await ensureCallbackServer()
  if (!port) {
    console.error('[auth] no loopback listener; a connector flow has nowhere to come home to')
    return null
  }

  const startedAt = win.webContents.getURL()

  const finish = ({ code, error, detail, params, provider }) => handOff(() => {
    if (win.isDestroyed()) return
    if (!code && win.webContents.getURL() !== startedAt) {
      console.warn(`[auth] dropping '${detail || error}': the window has moved on`)
      return
    }
    win.show()
    win.focus()
    // A timeout or a supersede is this shell talking, and inventing an
    // authorization-server error out of it would have the backend explain a
    // failure that never happened there. Send the window back where it started
    // instead: the page reloads out of its connecting state, which is the whole
    // of what it needs.
    if (!provider) return navigate(win, startedAt)
    const target = new URL(returnUrl)
    // Everything the server sent, unedited. Which of these matter is the
    // backend's question, not this shell's: it reads `state` to find the flow,
    // `code` to redeem, `iss` to prove the answer came from the server the
    // request went to, and `error`/`error_description` to say what went wrong.
    // Choosing a few of them here is how `iss` came to be dropped, and nothing
    // downstream could tell that it had been. Anything undeclared is ignored on
    // arrival, so passing it costs nothing and keeps the two ends from drifting.
    for (const [key, value] of params) target.searchParams.set(key, value)
    return navigate(win, target.toString())
  })

  const flow = armConnector(win, finish)
  if (!flow) return null
  const redirectUri = `http://127.0.0.1:${port}${MCP_CALLBACK_PATH}`
  console.log(`[auth] connector flow armed; redirect_uri ${redirectUri}`)
  return { redirectUri, flowId: flow.id }
}

/**
 * Tell an armed connector flow which `state` its authorization server will
 * return, once the backend has minted it.
 *
 * Two steps rather than one because the order is forced: the redirect_uri has to
 * be in hand before the request that mints the flow, and the state only exists
 * after it. Until this lands the flow accepts no callback at all, which is what
 * makes the gap safe rather than merely brief.
 *
 * Scoped by flow id and window, like `cancelMcp`, so a second connect started
 * while the first was still minting binds its state onto its own flow.
 */
function bindMcp(win, flowId, state) {
  const flow = flowId ? connectors.get(flowId) : null
  if (!flow || flow.win !== win || !state) return false
  // The clock restarts here rather than running on from `armConnector`. The
  // state arriving now is the one the callback will be checked against, and its
  // own expiry starts when it is minted -- after discovery and, for a vendor
  // that has not registered a client yet, registration too. Those are round
  // trips to the vendor, and counting them here spent them out of the user's
  // time at the consent screen, so the shell could discard a flow the backend
  // would still have honoured. Only the first bind moves it, so a flow cannot
  // be held open by binding it again.
  if (!flow.state) startConnectorClock(flow)
  flow.state = state
  return true
}

/**
 * Stand the listener down for a connector flow that never launched.
 *
 * The page has to arm before it calls its backend, because the redirect_uri
 * travels with that request — so a start that fails leaves a flow armed for a
 * code that is never coming. Left alone it runs the full ten minutes and then
 * raises this window to reload it, long after the user was told the connect
 * failed. Nothing is reported onward: there is no outcome here, only a flow
 * that turned out not to exist.
 *
 * Scoped to the flow the caller actually armed, by id. A sign-in armed in the
 * meantime is not this caller's to cancel, another window's is not either, and
 * neither is the caller's own *next* connect: the tab keeps per-row busy state
 * so a second broker can be started while the first is still going, so 'the
 * connector flow in this window' is a different flow a second later. Cancelling
 * by that description let a slow start that failed tear down a live flow the
 * user was still watching.
 */
function cancelMcp(win, flowId) {
  const flow = flowId ? connectors.get(flowId) : null
  if (!flow || flow.win !== win) return false
  clearTimeout(flow.timer)
  connectors.delete(flow.id)
  console.log('[auth] connector flow stood down; it never started')
  return true
}

module.exports = {
  isAuthorizeUrl, begin, beginMcp, bindMcp, cancelMcp, startCallbackServer, stopCallbackServer,
  isProviderNavigation, MCP_CALLBACK_PATH,
}
