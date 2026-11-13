'use strict'

const { app } = require('electron')

const config = require('./config')
const origins = require('./origins')
const outage = require('./outage')
const policy = require('./policy')
const store = require('./store')

// The contract between a window and the page it loads, both ways: what the
// shell tells a page about its window (`recordWindowFacts`, read by the
// preload's `shell:window-facts`), and what a finished load tells the shell
// about the page (`watchLoads`: whether the build reserves the window-button
// strip, and whether the app was reached at all).

/**
 * Ask the loaded page whether this build reserves the window-button strip.
 *
 * A declaration, not a hit-test of the rendered page. The strip is drawn only
 * when this shell reports the titlebar hidden, so measuring pixels would
 * measure the shell's own decision coming back around: a page told `native`
 * paints nothing there, the shell would read "does not reserve", and every
 * later window would stay framed. Self-locking, and it fires on exactly the
 * sidebar-less screens a first launch opens on.
 *
 * The meta answers what the build can do, which is the question, and does not
 * move with the window it happens to be in. It is also in `<head>`, so it is
 * true before the bundle runs and on the login and outage screens too.
 *
 * Returns null for a page older than this contract, which is not the same
 * answer as "no" and must not be recorded as one.
 */
const CHROME_DECLARATION = `(function () {
  var el = document.querySelector('meta[name="langalpha-window-chrome"]')
  return el ? el.content === 'reserves' : null
})()`

/**
 * Reaching the app is what retires onboarding, and reaching it means a load that
 * FINISHED, of a page the server was willing to serve. Recording it from the
 * navigation policy instead recorded it for a navigation that was merely
 * allowed, so a first run whose link dropped between the verdict and the load
 * came up on the app origin, unauthenticated, on every launch afterwards,
 * having never once signed in.
 *
 * `status` is what closes the other half of the same hole: a 502 from the edge
 * is a completely successful load of an error document, and treating that as
 * arrival retires onboarding forever on a single bad gateway. Status 0 is a
 * statusless load — file://, which the app origin never is.
 */
function noteAppReached(url, status) {
  if (!config.isSaas || !origins.isApp(url)) return
  if (!(status > 0 && status < 400)) return
  if (store.get('reachedApp')) return
  store.set('reachedApp', true)
}

/**
 * Everything a completed load teaches the shell about the page it just loaded.
 * `onFinish(url, status)` hears each load once its status is settled: the main
 * window's tells the menu bar the app is there.
 */
function watchLoads(win, { onFinish } = {}) {
  // `did-finish-load` says the body arrived, not that it was the page asked
  // for: an edge answering 502 is a perfectly successful load of an error
  // document, which is why outage.js reads the status too. Without it one bad
  // gateway on a SaaS first run retired onboarding permanently — every launch
  // afterwards opened the app origin unauthenticated, for a user who had never
  // signed in. `did-navigate` carries the status and fires first, so the last
  // one seen is the one that belongs to the load about to finish.
  //
  // Paired with its URL, not kept as a bare status: "the last one seen" holds
  // only while one navigation is in flight. The outage screen is a second one —
  // it aborts the load that failed and navigates this window to its own page —
  // and its status then answered for the page that finished. A 502 was read as
  // a served document that declared nothing, which revoked the chrome flag on
  // exactly the bad gateway this pairing exists to survive.
  let lastNav = { url: null, status: 0 }
  win.webContents.on('did-navigate', (_event, url, httpResponseCode) => {
    lastNav = { url, status: httpResponseCode }
  })
  // A same-document navigation moves the URL and keeps the document, and the
  // status is the document's. Both SPAs move their own URL as they boot: the
  // app's first run goes / → /dashboard → /welcome by replaceState, and when
  // that lands before the load event the URL read at `did-finish-load` is no
  // longer the one `did-navigate` reported. Matched on the old URL alone, that
  // load was credited with no status at all, and the first run that went
  // straight into the welcome flow never recorded reaching the app. Main frame
  // only: a subframe's in-page move is not this document changing its address.
  win.webContents.on('did-navigate-in-page', (_event, url, isMainFrame) => {
    if (isMainFrame === false || lastNav.url === null) return
    lastNav = { url, status: lastNav.status }
  })
  // A load that fails still finishes: Chromium commits its own error document
  // at the URL that failed and reports it with `did-fail-load`, never with
  // `did-navigate`. A reload fails at the URL the window is already on, so the
  // page it replaced was still paired with that URL, and the error document,
  // which declares nothing, was credited with that page's 200 and revoked the
  // flag on a server that was only unreachable. Forgotten here, the error
  // document finishes with no status, and the next real load sets one again.
  //
  // ERR_ABORTED is left alone, as the outage screen leaves it. It commits no
  // document, and it is how a load cut short by a newer one is reported. When
  // the newer one is on another site, that report arrives after the newer
  // one's `did-navigate`, so forgetting then would take that load's status.
  win.webContents.on('did-fail-load', (_event, code, _description, _url, isMainFrame) => {
    if (outage.shouldShow({ code, isMainFrame })) lastNav = { url: null, status: 0 }
  })

  win.webContents.on('did-finish-load', async () => {
    const url = win.webContents.getURL()
    // No match means the status belongs to some other navigation, and 0 is the
    // honest answer: every reader below treats it as "not known to be served",
    // which keeps a stale flag rather than revoking one on a guess.
    const lastStatus = lastNav.url === url ? lastNav.status : 0
    noteAppReached(url, lastStatus)
    if (onFinish) onFinish(url, lastStatus)

    const key = policy.chromeKeyFor(url)
    if (!key) return
    try {
      // `userGesture: false`: this reads a meta tag, and the flag exists to make
      // the script count as a user activation. Passing true handed every page
      // the shell loads a fresh gesture token on every load, which is what
      // gates popups, fullscreen and autoplay — the shell would have been
      // quietly re-arming the surface its own window-open policy exists to hold.
      const reserves = await win.webContents.executeJavaScript(CHROME_DECLARATION, false)
      // `null` means the page declared nothing, which is a real "does not
      // reserve" only when the server actually served the app. An error page
      // from the same origin declares nothing either, and reading that as a no
      // would reframe the window on a bad gateway. Gated on the status, this
      // covers the one revocation nothing else does: a rollback to a build that
      // predates the declaration, at an origin that never changed, otherwise
      // leaves the flag true forever and the window opens frameless against a
      // page with no reserved strip and no drag region anywhere.
      const served = lastStatus >= 200 && lastStatus < 300
      const answer = reserves === null && served ? false : reserves
      if (typeof answer !== 'boolean') return
      if (answer === store.get(key)) return
      store.set(key, answer)
      // Not applied to this window: titleBarStyle is fixed at construction, and
      // recreating a live window would cost whatever turn is streaming in it.
      // The next launch opens in the right frame, and this one is not wrong in
      // the meantime: the page was told it is framed and laid out for that.
      console.log(`[shell] ${key}: ${answer ? 'reserves' : 'does not reserve'} the window-button strip`)
    } catch {
      // A page that will not answer keeps whatever the last one said.
    }
  })
}

/**
 * What a window tells its page about itself, by the window's WebContents.
 *
 * `windowChrome` is the authoritative answer to "did this shell hide the
 * titlebar", and it is the only input to whether the page reserves the button
 * strip. Per window rather than per platform: the console is framed in a window
 * of its own and hidden when it shares the main one, and the page has no way to
 * work that out for itself.
 *
 * Answered over IPC from the sender, never put on the renderer's command line.
 * `additionalArguments` belong to the renderer *process*, and a window's page
 * does not always land in a process launched with them: a page reached that way
 * read every switch as absent, version 0.0.0 and titlebar `native`, and drew its
 * logo under the window buttons of a window whose titlebar was hidden, with no
 * drag region anywhere. The WebContents is the window's own, whatever process
 * its page is in.
 */
const windowFacts = new WeakMap()

function recordWindowFacts(win, { chromeHidden, role = 'other' }) {
  windowFacts.set(win.webContents, {
    // The sandboxed preload cannot read package.json. See the note in preload.js.
    version: app.getVersion(),
    windowChrome: chromeHidden ? 'hidden' : 'native',
    // Which scheme this edition answers on. The page cannot derive it: both
    // editions install side by side, and an email link marked for the wrong one
    // opens the other build, or nothing.
    scheme: config.scheme,
    // Whether this build has a console to finish a return from the browser on:
    // an integration login or install, or a console page asked to be shown
    // again. An edition with no platform origin drops those links, so it must
    // not say it can.
    consoleReturns: Boolean(origins.platformOrigin()),
    // Which of the shell's windows this is. The menu bar window's page is the
    // one that posts notifications, and the main window's the one that takes
    // menu commands, so each page has to know which it is.
    windowRole: role,
  })
}

/** Null for a WebContents the shell did not record, which the preload reads as framed. */
function windowFactsOf(webContents) {
  return windowFacts.get(webContents) ?? null
}

// `noteAppReached` too: the one store write a completed load makes, and its
// status rule is easier to pin directly than through a driven load.
module.exports = { recordWindowFacts, windowFactsOf, watchLoads, noteAppReached }
