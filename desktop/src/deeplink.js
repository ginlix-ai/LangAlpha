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

module.exports = { SCHEME, init, attach, toAppUrl, fromArgv }
