'use strict'

const path = require('node:path')
const { app } = require('electron')
const origins = require('./origins')
const config = require('./config')

// ---------------------------------------------------------------------------
// The custom scheme (`langalpha://` hosted, `langalpha-oss://` self-hosted).
//
// A magic link or email confirmation is clicked minutes or hours later, quite
// possibly with the app closed, so the loopback listener used for OAuth cannot
// serve it: nothing is listening. A registered scheme is the only handoff the OS
// will hold open across a cold start.
//
// The payload lands on an app-side callback route, same as the OAuth path, so
// @supabase/ssr redeems it against the verifier in that renderer's cookie jar.
// In the SaaS edition either of our origins can do that: the verifier is stored
// as `langalpha-auth-code-verifier` through the same cookie adapter, and
// production scopes those cookies to the parent domain, so both subdomains share
// it. The OSS edition has one origin and no ambiguity to begin with.
// ---------------------------------------------------------------------------

// Per edition, and read from config rather than spelled here: both builds
// registering the same scheme leaves the OS to choose between them, and it
// chooses once for the machine. A hosted magic link opening the self-hosted
// build lands a code on an origin that cannot redeem it.
const SCHEME = config.scheme

let deliver = null
let queued = null

function register() {
  // In dev, Electron is launched through the electron binary with this
  // directory as an argument, so the OS has to be told both parts or the
  // registration points at a bare `electron` that opens nothing useful.
  if (process.defaultApp && process.argv.length >= 2) {
    app.setAsDefaultProtocolClient(SCHEME, process.execPath, [path.resolve(process.argv[1])])
  } else {
    app.setAsDefaultProtocolClient(SCHEME)
  }
}

/** Pull a langalpha:// URL out of an argv vector (Windows and Linux hand it over that way). */
function fromArgv(argv) {
  return (argv || []).find((a) => typeof a === 'string' && a.startsWith(`${SCHEME}://`)) || null
}

/**
 * Turn `langalpha://callback?code=…` into a URL on one of our origins.
 *
 * The host part of a custom-scheme URL is not a real host, so the path is taken
 * from the target app rather than from the link: everything after the scheme is
 * treated as query, and the route is fixed.
 */
function toAppUrl(raw, currentUrl) {
  let parsed
  try {
    parsed = new URL(raw)
  } catch {
    return null
  }
  if (parsed.protocol !== `${SCHEME}:`) return null

  // Stay on whichever of our apps the window is already showing, so a link
  // clicked mid-onboarding does not throw the user back to the app root.
  const base = origins.isOurs(currentUrl) ? origins.originOf(currentUrl) : origins.appOrigin()
  const target = new URL('/callback', base)
  for (const [key, value] of parsed.searchParams) target.searchParams.set(key, value)
  return target.toString()
}

// ---------------------------------------------------------------------------
// An integration login or install, handed back from the browser.
//
// The console connects an integration through the provider's own page: a
// sign-in, or an install that adds the app to one of the user's teams there.
// That page opens in the system browser like every other provider page, and the
// provider returns to the console's callback in that browser, which does not
// hold the app's session. So a login or install started in the app is not
// finished there: that page hands its query back through the scheme, and the
// shell opens the same callback page on the console's origin in its own
// window, where the console finishes it.
//
// One of the two links whose host and path are read at all (a console page,
// below, is the other), and they still do not choose where it goes. The shape
// is matched whole, the origin is always the configured console, and the path
// is rebuilt from a kind named in `RETURNS` and a name that can only be
// lowercase letters, digits and hyphens.
// ---------------------------------------------------------------------------

const RETURN_HOST = 'integrations'
const RETURN_PATH = /^\/([^/]*)\/([^/]*)\/callback$/
const RETURN_NAME = /^[a-z][a-z0-9-]{0,31}$/
// Each kind of return, and what it carries and nothing else. The console's
// callback page reads these, and anything the link adds beyond them is not the
// provider's.
const RETURNS = new Map([
  // What a provider sends back from a sign-in.
  ['login', new Set(['code', 'state', 'error', 'error_description'])],
  // Which team an install added the app to and what it is called, or why it
  // did not.
  ['install', new Set(['team', 'team_name', 'error'])],
])

function parseOurs(raw) {
  try {
    const parsed = new URL(raw)
    return parsed.protocol === `${SCHEME}:` ? parsed : null
  } catch {
    return null
  }
}

/** The kind and name of a link addressed to an integration return, or null for any other link. */
function returnSegments(parsed) {
  if (parsed.host !== RETURN_HOST) return null
  const match = RETURN_PATH.exec(parsed.pathname)
  return match && RETURNS.has(match[1]) ? { kind: match[1], name: match[2] } : null
}

/**
 * `langalpha://integrations/<kind>/<name>/callback?…` as the console's
 * `/integrations/<kind>/<name>/callback` with the same answer, where the kind
 * is `login` or `install`.
 *
 * Null means the link goes nowhere: a name outside the pattern is not one the
 * console mints, and `carry` refuses the rest.
 */
function integrationUrl(parsed, { kind, name }) {
  if (!RETURN_NAME.test(name)) return null
  return carry(parsed, `/integrations/${kind}/${name}/callback`, RETURNS.get(kind))
}

// ---------------------------------------------------------------------------
// A console page asked to be shown again, handed back from the browser.
//
// The console sometimes sends the user to a third party's page in the system
// browser, and that page returns to the console in the browser, which does not
// hold the app's session either. The console page it lands on hands its query
// back through the scheme, and the shell loads the console page the link names
// in its own window, where the session is.
//
// Held tighter than an integration return. The path is matched whole against
// `CONSOLE_PAGES` and never read past that, the origin is always the configured
// console, and every value carried is one short token, because one of them says
// where the console goes next and a token cannot be read as a path or an
// address.
// ---------------------------------------------------------------------------

const CONSOLE_HOST = 'console'
// Each console page a link may name, and the only parameters carried to it.
// The key is the page's path on the console, and a link names a page only by
// matching a key whole, so nothing else the link spells reaches the path. Each
// key is an absolute path, one leading slash, so the URL built on it stays on
// the console's origin.
const CONSOLE_PAGES = new Map([
  // Where the console lands the user back from a third party's page: where to
  // go next, how it went, and the third party's id for the session it ran.
  ['/desktop/return', new Set(['to', 'outcome', 'session_id'])],
])
// Every value carried to a console page. No slash, dot, colon, `%` or space,
// so a value can never be read as a path, a host or something still encoded.
const CONSOLE_VALUE = /^[A-Za-z0-9_-]{1,255}$/

/**
 * `langalpha://console/<page>?…` as that page on the console's origin, where
 * the page is a key of `CONSOLE_PAGES`.
 *
 * Null means the link goes nowhere: a host that only reads as the console (a
 * port, another case) is not the console's, a path that is not exactly a known
 * page is not one the console sends, and `carry` refuses the rest, including a
 * carried value outside `CONSOLE_VALUE`.
 */
function consolePageUrl(parsed) {
  if (parsed.host !== CONSOLE_HOST) return null
  const carried = CONSOLE_PAGES.get(parsed.pathname)
  if (carried === undefined) return null
  return carry(parsed, parsed.pathname, carried, CONSOLE_VALUE)
}

// ---------------------------------------------------------------------------
// What every link addressed to the console shares.
// ---------------------------------------------------------------------------

/**
 * The link's query, carried onto `path` on the configured console: the
 * parameters in `allowed` and nothing else, so one the page does not carry is
 * dropped rather than refused.
 *
 * Null means the link goes nowhere: an edition with no console has no page to
 * open it on, and a repeated parameter is refused whole, for the reason
 * oauth.js gives: whatever reads it takes one value, and a real provider never
 * sends two. With a `valuePattern`, so is a carried value outside it.
 */
function carry(parsed, path, allowed, valuePattern = null) {
  const platform = origins.platformOrigin()
  if (!platform) return null

  const names = [...parsed.searchParams.keys()]
  if (names.length !== new Set(names).size) return null

  const target = new URL(path, platform)
  for (const [key, value] of parsed.searchParams) {
    if (!allowed.has(key)) continue
    if (valuePattern && !valuePattern.test(value)) return null
    target.searchParams.set(key, value)
  }
  return target.toString()
}

/**
 * Where a link addressed to the console goes. Asked before `toAppUrl`, because
 * the answer decides the window as well as the URL, and because such a link
 * must never fall through to `/callback`: that route would try to redeem it as
 * a sign-in.
 *
 * Null for any other link, which goes on to `toAppUrl`. Otherwise
 * `{ kind, url }`, where `url` is the console page to open, or null when the
 * link is recognised and refused:
 *
 * - `integration`: a login or install handed back, `integrations` host, its
 *   shape matched whole and asked first.
 * - `console`: a console page asked to be shown again, which is any link under
 *   the `console` host whatever its path, so a page this build does not know is
 *   refused rather than redeemed. Recognised more loosely than it is accepted:
 *   a port or another case still reads as the console here, and is then
 *   refused. Every link that looks like one is either opened exactly or
 *   dropped, never redeemed.
 */
function toConsole(raw) {
  const parsed = parseOurs(raw)
  if (parsed === null) return null
  const segments = returnSegments(parsed)
  if (segments !== null) return { kind: 'integration', url: integrationUrl(parsed, segments) }
  if (parsed.hostname.toLowerCase() === CONSOLE_HOST) return { kind: 'console', url: consolePageUrl(parsed) }
  return null
}

/**
 * Register the OS hooks. `onUrl` is called with the raw langalpha:// URL, and
 * anything that arrives before it is set is held until it is.
 */
function attach(onUrl) {
  deliver = onUrl
  if (queued) {
    const held = queued
    queued = null
    deliver(held)
  }
}

function accept(raw) {
  if (!raw) return
  if (deliver) deliver(raw)
  else queued = raw
}

function init() {
  register()

  // macOS delivers through open-url, which can fire before the app is ready.
  app.on('open-url', (event, url) => {
    event.preventDefault()
    accept(url)
  })

  // Windows and Linux relaunch the binary instead, and the single-instance lock
  // turns that into an event on the instance already running.
  app.on('second-instance', (_event, argv) => accept(fromArgv(argv)))

  // A cold start on Windows/Linux carries the URL in our own argv.
  accept(fromArgv(process.argv))
}

module.exports = { SCHEME, init, attach, toAppUrl, fromArgv, toConsole }
