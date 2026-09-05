'use strict'

const config = require('./config')
const store = require('./store')

/**
 * Which origins are "ours".
 *
 * The spike classified the account console by the path `/account`, a shape that
 * exists only in dev where nginx stitches both SPAs onto one host. Production
 * serves them from separate origins and the shipped bundle links to the absolute
 * platform host, so that test never matched and a packaged build threw its own
 * console at the system browser. Origin is the only thing true in both layouts.
 */

/** The langalpha SPA. In the OSS edition the user's stored server wins. */
function appOrigin() {
  if (!config.isSaas) {
    const stored = store.get('serverUrl')
    if (stored) {
      try {
        return new URL(stored).origin
      } catch {
        // Unparseable stored value: fall through to the compiled default rather
        // than throw, and let the picker correct it.
      }
    }
  }
  return config.appOrigin
}

/** The account console, or null in the OSS edition, which ships without one. */
function platformOrigin() {
  return config.isSaas ? config.platformOrigin : null
}

/**
 * The origin of a URL, but only for a document the network served us.
 *
 * The scheme check is the load-bearing half. A `blob:` URL inherits the origin
 * of the page that minted it, so `blob:https://app.example.com/…` answers
 * `https://app.example.com` and would otherwise classify as ours — which let a
 * `window.open` of agent-generated widget HTML be treated as an app navigation
 * and take over the main window, ending whatever turn was streaming in it and
 * handing that HTML the preload bridge. Same reasoning for `data:` and
 * `filesystem:`. Being minted BY our origin is not the same as being served by
 * it, and only the second is what "ours" means here.
 */
function originOf(url) {
  try {
    const u = new URL(url)
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return null
    return u.origin
  } catch {
    return null
  }
}

function isApp(url) {
  return originOf(url) === appOrigin()
}

function isPlatform(url) {
  const platform = platformOrigin()
  return !!platform && originOf(url) === platform
}

/** True for anything the shell should keep inside the app rather than hand off. */
function isOurs(url) {
  return isApp(url) || isPlatform(url)
}

/**
 * May this URL be handed to the OS to open?
 *
 * `shell.openExternal` does not fetch anything: it gives the string to the
 * platform's URL handler, which on macOS launches an application bundle for a
 * `file:` URL and, for any scheme some installed app has registered, that app.
 * Both callers pass a URL the renderer chose - one from a navigation it
 * attempted, one from a bridge call it made — so the scheme is the boundary.
 *
 * An allowlist rather than a blocklist, because the dangerous set is whatever
 * the user happens to have installed and is not knowable from here.
 */
const OPENABLE_SCHEMES = new Set(['http:', 'https:', 'mailto:'])

function isExternallyOpenable(url) {
  try {
    return OPENABLE_SCHEMES.has(new URL(url).protocol)
  } catch {
    return false
  }
}

module.exports = {
  appOrigin, platformOrigin, originOf, isApp, isPlatform, isOurs, isExternallyOpenable,
}
