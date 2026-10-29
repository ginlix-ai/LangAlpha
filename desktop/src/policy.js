'use strict'

const config = require('./config')
const origins = require('./origins')
const store = require('./store')

// ---------------------------------------------------------------------------
// What the shell decides, separated from what it does about it.
//
// Every function here is a pure read of config, the store and a URL. Nothing
// opens a window, hands a URL to the system browser, or writes a setting: main
// carries the verdicts out. That split is not tidiness. `classifyNavigation`
// used to record "this install has reached the app" as it classified, which
// recorded it for a navigation that was merely *allowed* — so a first run whose
// link dropped between the verdict and the load skipped platform sign-in on
// every launch afterwards. A module with no way to write cannot make that
// mistake again.
//
// It also means these decisions are testable on their own, without the entry
// module's lifecycle hooks.
// ---------------------------------------------------------------------------

/**
 * Where a launch lands.
 *
 * A SaaS first run enters at the platform's sign-in page, never at onboarding
 * directly: whether an account still needs setting up is a question only the
 * platform can answer, and its post-auth funnel already answers it: a returning
 * customer (signed in, or signing in on a new machine) goes straight to the app,
 * and only a genuinely new one is routed into the wizard. The shell asking
 * instead would need credentials it deliberately does not have.
 *
 * `reachedApp` is a shortcut past that round trip, not the decision: once this
 * install has landed on the app, later launches open it directly. A launch that
 * finds the app signed out is sent back to platform sign-in by the app itself
 * (`web/src/lib/hostedSignIn.ts`), so the shortcut never skips the funnel for a
 * new account.
 */
function entryUrl() {
  if (!config.isSaas) {
    return store.get('serverUrl') ? origins.appOrigin() : null
  }
  if (store.get('reachedApp')) return origins.appOrigin()
  return new URL(config.loginPath, origins.platformOrigin()).toString()
}

/**
 * Can the console share the main window?
 *
 * It should, and in the browser it does: "Usage & Plan" is a plain link and the
 * console has its own way back, so a second window is the shell inventing
 * something the product does not have. The one case where it cannot is a main
 * window with no titlebar showing a page that reserves no strip for the window
 * buttons, which is the buttons-on-the-logo bug. Both halves are observed, not
 * assumed, so this resolves to "yes" again the moment the console reserves one.
 */
function platformFitsMainWindow() {
  return !store.get('appChrome') || store.get('platformChrome')
}

/**
 * Where a navigation belongs: 'allow' | 'app-window' | 'platform-window' |
 * 'external'.
 *
 * Deliberately knows nothing about OAuth. Taking over an authorize URL means
 * opening the system browser and parking a pending flow, which is an action and
 * not a verdict; main checks for it before asking this.
 */
function classifyNavigation(url, { isMainWindow }) {
  if (origins.isApp(url)) return isMainWindow ? 'allow' : 'app-window'

  if (origins.isPlatform(url)) {
    if (!isMainWindow) return 'allow'
    return platformFitsMainWindow() ? 'allow' : 'platform-window'
  }

  return 'external'
}

/**
 * Where a window.open or target=_blank belongs: the navigation verdicts, with
 * 'allow' resolved to 'external'.
 *
 * Ours asking for a second surface is the opposite of a navigation asking to
 * stay put, and loading it into this window would replace whatever turn is
 * streaming here; the shell keeps no popup windows, so the browser is it. The
 * one exception is the console handing a page to the app while they share the
 * main window, which is the thread links on its usage page: the console has no
 * turn to lose, and in the browser the app may not be signed in at all. When
 * the console has a window of its own the same link is already 'app-window'.
 */
function classifyWindowOpen(url, { isMainWindow, openerUrl }) {
  const verdict = classifyNavigation(url, { isMainWindow })
  if (verdict !== 'allow') return verdict
  if (isMainWindow && origins.isApp(url) && origins.isPlatform(openerUrl)) return 'app-window'
  return 'external'
}

/** Which chrome flag a loaded URL answers for, or null if the page is not ours. */
function chromeKeyFor(url) {
  if (origins.isApp(url)) return 'appChrome'
  if (origins.isPlatform(url)) return 'platformChrome'
  return null
}

module.exports = { entryUrl, platformFitsMainWindow, classifyNavigation, classifyWindowOpen, chromeKeyFor }
