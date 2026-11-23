'use strict'

const config = require('./config')
const store = require('./store')

// Which backend this OSS install uses: a server the user named, or the stack the
// app carries. Everything that asks "where is the app" asks here, so no caller
// has to know that there are two answers.
//
// Two store keys, each meaning one thing. `serverUrl` is only ever the user's own
// server, and `stackMode: 'bundled'` says the stack is the choice. Earlier builds
// of the stack stored its loopback origin as `serverUrl`, which lost the user's
// address on the way in, and on a build with no stack (Windows, a thin build)
// opened a loopback nothing answers on instead of the picker.
//
// Choosing the stack takes effect once it is up. Until then the app stays on
// what it was on, so a window still showing the user's server keeps working
// through a start that takes a minute, and a start that fails leaves that
// server the choice.

let stack = null
// The stack was asked for and is not the stored choice yet: a first run's start,
// or a switch from a server.
let pending = false
let stopping = null

/** The stack this build carries, or null. Main calls this once, before anything reads. */
function attach(value) {
  stack = value
  migrate()
}

const loopback = (port) => (port ? `http://127.0.0.1:${port}` : null)

/** Builds before this one stored the stack's origin as `serverUrl`; it named no server of the user's. */
function migrate() {
  const stored = store.get('serverUrl')
  if (store.get('stackMode') === 'bundled' && stored && stored === loopback(store.get('stackPort'))) {
    store.reset('serverUrl')
  }
}

function serverOrigin() {
  const stored = store.get('serverUrl')
  if (!stored) return null
  try {
    return new URL(stored).origin
  } catch {
    // Unparseable: no choice, so the picker asks again.
    return null
  }
}

/**
 * `{ kind: 'bundled' }`, `{ kind: 'server', origin }`, or null when this install
 * has nothing it can open: a first run, or a stack chosen on a build that has none.
 */
function choice() {
  if (config.isSaas) return null
  if (store.get('stackMode') === 'bundled') return stack ? { kind: 'bundled' } : null
  const origin = serverOrigin()
  return origin ? { kind: 'server', origin } : null
}

const runsBundled = () => choice()?.kind === 'bundled'

/** Asked for or chosen: a failure is worth showing, and its logs worth offering. */
const wantsStack = () => Boolean(stack) && (pending || runsBundled())

/**
 * The app's origin under the choice, or null. The stack's is known before it is
 * up from the port it keeps between launches, so a window left on it while it
 * restarts is still the app's.
 */
function origin() {
  const chosen = choice()
  if (!chosen) return null
  if (chosen.kind === 'server') return chosen.origin
  return stack.origin || loopback(store.get('stackPort'))
}

/** Whether the app can be loaded now: a server always can, the stack once it is up. */
function isUp() {
  const chosen = choice()
  return Boolean(chosen) && (chosen.kind === 'server' || stack.state === 'ready')
}

const isStarting = () => wantsStack() && stack.state === 'starting'

/** The stack is the choice and a quit stopped it: one an install began, then a page cancelled. */
const stoppedForQuit = () => runsBundled() && stack.state === 'stopped'

/**
 * The chrome flags are learned from a declaration the loaded page makes, so they
 * are an answer about the origin that gave it. A server that ships the
 * declaration followed by one that does not left the old `true` in place (a page
 * that cannot answer is deliberately not read as a "no"), and the next launch
 * opened frameless against a build that reserves nothing.
 */
function forgetChrome() {
  store.reset('appChrome')
  store.reset('platformChrome')
}

/**
 * Point this install at a server. Answers `{ ok, origin, moved }`, or `{ ok: false,
 * error }` for an address it cannot store. Choosing a server over the stack stops
 * it; its data stays for whenever this install switches back.
 */
function useServer(rawUrl) {
  let parsed
  try {
    parsed = new URL(rawUrl)
  } catch {
    return { ok: false, error: 'That is not a valid URL.' }
  }
  // The scheme, not just the parse. `new URL('file:///x')` parses, and its origin
  // is the *string* "null", truthy, so it stored fine and the install could never
  // reach its picker again. The setup page prefixes `http://` and probes before it
  // gets here, so this guards the IPC boundary rather than that flow.
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return { ok: false, error: 'Use an http:// or https:// address.' }
  }
  // A path is not part of an origin, and `parsed.origin` drops it silently: the
  // page would probe `https://host/langalpha` and then open `https://host/`.
  // Nothing downstream is path-aware, since `origins` compares origins throughout.
  if ((parsed.pathname && parsed.pathname !== '/') || parsed.search || parsed.hash) {
    return { ok: false, error: 'Enter the address only, without a path.' }
  }
  // Dropped by `parsed.origin` exactly as a path is, and invisible to the path
  // check: `https://u:p@host` has pathname '/'. A server that needs basic auth
  // cannot be reached by an origin that no longer carries it.
  if (parsed.username || parsed.password) {
    return { ok: false, error: 'Enter the address only, without a username or password.' }
  }
  const before = origin()
  const next = parsed.origin
  pending = false
  store.set('serverUrl', next)
  store.reset('stackMode')
  stop()
  if (next !== before) forgetChrome()
  return { ok: true, origin: next, moved: next !== before }
}

/**
 * Run the stack and make it the choice once it is up. Answers the supervisor's
 * result with `moved`, or `{ ok: false, superseded: true }` when a server was
 * chosen while it started.
 */
async function useBundled() {
  const before = origin()
  pending = true
  const result = await stack.start()
  if (!pending && !runsBundled()) {
    // The stack came up, or failed, for nobody.
    stop()
    return { ok: false, superseded: true }
  }
  if (!result.ok) return result
  pending = false
  store.set('stackMode', 'bundled')
  const moved = result.origin !== before
  if (moved) forgetChrome()
  return { ...result, moved }
}

/**
 * Stop whatever of the stack is running, server first and Postgres last, or null
 * when nothing is. A stop already under way is the one answered, so a quit
 * during a switch still waits for it.
 */
function stop() {
  if (!stack) return null
  if (!stopping && !['starting', 'ready', 'failed'].includes(stack.state)) return null
  stopping ||= stack.stop()
    .catch((err) => console.error(`[stack] stopping failed: ${err.message}`))
    .finally(() => { stopping = null })
  return stopping
}

module.exports = {
  attach,
  hasStack: () => Boolean(stack),
  choice,
  runsBundled,
  wantsStack,
  origin,
  isUp,
  isStarting,
  stoppedForQuit,
  serverAddress: () => store.get('serverUrl'),
  useServer,
  useBundled,
  stop,
}
