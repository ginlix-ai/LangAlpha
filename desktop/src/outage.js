'use strict'

const path = require('node:path')
const { pathToFileURL } = require('node:url')
const { BrowserWindow, ipcMain, net, shell } = require('electron')

const config = require('./config')
const store = require('./store')
const theme = require('./theme')
const { probe } = require('./probe')
const captive = require('./captive')
const { forDisplay } = require('./redact')

// ---------------------------------------------------------------------------
// The outage page.
//
// A remote-URL shell shows whatever the network gives it, and what a failed load
// gives it is a blank window. That is the one failure mode this architecture
// buys, so it gets a real screen: what we tried, what came back, whether the
// machine is even online, and a way out.
//
// The page is a local file, which is what makes it work at all: anything fetched
// would fail for the same reason the app just did.
// ---------------------------------------------------------------------------

const PAGE = path.join(__dirname, '..', 'outage', 'outage.html')
const PAGE_URL = pathToFileURL(PAGE).href

// -3 is ERR_ABORTED, which a router push or a superseded load produces routinely.
const BENIGN = new Set([-3])

// -21 is ERR_NETWORK_CHANGED: switching access point, waking from sleep, or
// raising a VPN. The load really did fail, but the network that failed it no
// longer exists, so the honest response is to try the new one once before
// telling the user anything is wrong.
const TRANSIENT = new Set([-21])

/**
 * One record per window, rather than a container per fact. The four this
 * replaced drifted apart: three were weak and the fourth was a Map keyed by
 * `webContents.id` that nothing ever deleted, so it held a closure per window
 * for the life of the process.
 *
 * `showing` is separate from `url` because clearing the page must not clear the
 * retry budget: the budget is spent per outage and returned by a load that
 * completes, and `clear` runs on the navigation *away*, which is the moment
 * before that load.
 */
const windows = new WeakMap()

function recordFor(win) {
  let record = windows.get(win)
  if (!record) {
    record = { showing: false, url: null, reason: null, retried: false, probing: null, generation: 0, onChangeServer: null }
    windows.set(win, record)
  }
  return record
}

function isShowing(win) {
  return !!windows.get(win)?.showing
}

function targetFor(win) {
  const record = windows.get(win)
  return record && record.showing ? record.url : null
}

function reasonShowing(win) {
  const record = windows.get(win)
  return record && record.showing ? record.reason : null
}

/**
 * A token for the outage a window is on, and the question of whether it still is.
 *
 * Every path that acts on an outage probes first, and a probe lasts long enough
 * for the user to hit Retry, pick Home, or connect a different server. All of
 * those clear the record, and the stale continuation would otherwise land on top
 * of the page they just got back. Named rather than open-coded because there are
 * three call sites and the two that had it hand-rolled were written twice.
 */
function generation(win) {
  return recordFor(win).generation
}

function movedOn(win, since) {
  return win.isDestroyed() || recordFor(win).generation !== since
}

function clear(win) {
  const record = windows.get(win)
  if (record) {
    record.showing = false
    record.url = null
    record.reason = null
    // Anything already in flight for the outage being cleared must not land.
    record.generation += 1
  }
}

/**
 * Should a failed load replace the window? Subframe failures are the page's own
 * business, and an aborted main-frame load usually means a newer one won.
 */
function shouldShow({ code, isMainFrame }) {
  return !!isMainFrame && !BENIGN.has(code)
}

/**
 * An HTTP error the page itself cannot explain. Anything under 500 is the app's
 * to render: a 404 is a route, a 401 is a login.
 *
 * This matters more than it looks. A transport failure fires did-fail-load, but
 * an edge returning 502 is a *successful* load of an error body, so without this
 * the user gets the CDN's page with no way back.
 */
function isServerError(status) {
  return Number.isInteger(status) && status >= 500
}

/**
 * A URL fit to be shown and logged.
 *
 * The failing target is often a callback: a magic link or an OAuth redirect
 * carries its one-time credential in the query string, and this page is reached
 * precisely when that load did not work. Printed whole it goes into the console,
 * into any screenshot the user sends to support, and onto the screen behind
 * them. Retry does not read this — it uses the full URL held in the window's
 * record — so nothing needs the parameters except the eye.
 */
/**
 * `portal` is supplied by the caller rather than measured here so this stays a
 * pure function of its inputs; the probe that produces it lives in `captive`.
 */
function reasonFor({ code, status, portal }) {
  if (isServerError(status)) return 'server-error'
  // net.isOnline is about this machine's connectivity, not about reaching us,
  // which is exactly the distinction the user needs drawn for them. A false is
  // reliable; a true is documented as inconclusive, which is why a portal can
  // still be sitting behind it.
  if (!net.isOnline()) return 'offline'
  if (portal) return 'captive-portal'
  return 'unreachable'
}

/**
 * Replace a window's content with the outage page. `target` is where it should
 * have been, and is what a retry goes back to.
 */
async function show(win, { target, code, description, status }) {
  if (!win || win.isDestroyed()) return
  const record = recordFor(win)
  // Scoped to the outage it was started for, not a plain flag. A probe lasts up
  // to its timeout, and a navigation inside that window clears the record; the
  // next failure then belongs to a new generation and needs its own screen. A
  // global latch turned it away, and the old probe stood down on `movedOn`, so
  // the failure that mattered got no outage page at all.
  if (record.probing === record.generation) return
  const url = target || targetFor(win)
  if (!url) return

  // Claim the window before the probe, so a second failure arriving while it is
  // in flight neither starts its own nor slips past `isShowing`.
  Object.assign(record, { showing: true, url, reason: 'unreachable' })

  // Only the "online, but we still could not get there" branch has anything to
  // learn: a 5xx already answered, and an offline machine cannot be behind a
  // portal. Those two show instantly; this one may wait up to the probe timeout.
  let portal = false
  if (!isServerError(status) && net.isOnline()) {
    const since = generation(win)
    record.probing = since
    try {
      portal = await captive.behindPortal(url)
    } finally {
      if (record.probing === since) record.probing = null
    }
    if (movedOn(win, since)) {
      console.log('[outage] the window recovered while the portal probe was out; standing down')
      return
    }
  }

  const reason = reasonFor({ code, status, portal })
  Object.assign(record, { showing: true, url, reason })
  console.error(`[outage] ${reason} for ${forDisplay(url)} (code=${code ?? '-'} status=${status ?? '-'} ${description || ''})`)

  win.loadFile(PAGE, {
    query: {
      target: forDisplay(url),
      reason,
      edition: config.edition,
      theme: theme.isTheme(store.get('theme')) ? store.get('theme') : 'dark',
      detail: [description, code ? `(${code})` : '', status ? `HTTP ${status}` : '']
        .filter(Boolean).join(' ').trim(),
    },
  }).catch((err) => {
    // The last-resort screen failing to render is the one failure with nothing
    // behind it, so it is worth a log rather than an unhandled main-process
    // rejection that says nothing about where it came from.
    console.error(`[outage] could not render the outage page: ${err.message}`)
  })
  win.show()
}

/**
 * Attach to a window. `onChangeServer` is the OSS escape hatch; SaaS passes
 * nothing and the page hides that button.
 */
function attach(win, { onChangeServer } = {}) {
  // A cleared outage page is a dead end: Retry, Open and Change Server all check
  // `isShowing`, and clearing is what makes that false. Left in history, Back —
  // or a macOS two-finger swipe, which never reaches the menu — lands on
  // something that still looks like a recovery screen and answers nothing.
  // Pruned on arrival somewhere else, which is the one point every recovery path
  // passes through, rather than in `clear`, which runs before the navigation
  // that replaces it.
  win.webContents.on('did-navigate', (_event, url) => {
    if (!url || url.startsWith(PAGE_URL)) return
    const history = win.webContents.navigationHistory
    if (!history) return
    const entries = history.getAllEntries()
    // Backwards: removing an entry shifts every index above it. The entry being
    // displayed needs no exception — the guard above already returned if this
    // window had landed on the outage page, so none of the matches below is it.
    for (let i = entries.length - 1; i >= 0; i -= 1) {
      if (entries[i].url.startsWith(PAGE_URL)) history.removeEntryAtIndex(i)
    }
  })

  win.webContents.on('did-fail-load', (_event, code, description, url, isMainFrame) => {
    if (!shouldShow({ code, isMainFrame })) return
    // A file:// load failing is our own page, and retrying it would loop.
    if (url && url.startsWith('file://')) return

    // One silent retry on a transient code, then never again until something
    // loads. Without the second half this is a reload loop on a dead network.
    const record = recordFor(win)
    if (TRANSIENT.has(code) && !record.retried) {
      record.retried = true
      // The fourth site of the same race, and the one with the widest blast
      // radius: this timer outlives any navigation the user starts inside the
      // second it waits, and `did-start-navigation` clearing the record is
      // exactly what makes `isShowing` false again. Unguarded, the retry loads
      // the URL that just failed over the page that replaced it, which can be a
      // live app page or a streaming turn.
      const since = generation(win)
      setTimeout(() => {
        if (movedOn(win, since) || isShowing(win)) return
        // This URL just failed, so the retry failing too is the expected case.
        // Unhandled, that rejection is a main-process fault, not a renderer one.
        win.loadURL(url).catch((err) => {
          console.warn(`[outage] silent retry failed: ${err.message}`)
        })
      }, 1000)
      return
    }

    show(win, { target: url, code, description })
  })

  // Only a load that actually completed earns the retry back.
  win.webContents.on('did-finish-load', () => { recordFor(win).retried = false })

  win.webContents.on('did-navigate', (_event, url, status) => {
    if (!isServerError(status)) return
    if (!/^https?:/.test(url)) return
    show(win, { target: url, status })
  })

  win.webContents.on('did-start-navigation', (_event, url, _isInPlace, isMainFrame) => {
    // Leaving the outage page for a real URL means the window recovered.
    if (isMainFrame && url && !url.startsWith('file://')) clear(win)
  })

  if (onChangeServer) recordFor(win).onChangeServer = onChangeServer
}

function registerIpc({ onRetry }) {
  // Probe before reloading so a failed retry does not flash the app's loading
  // screen and bounce straight back here.
  ipcMain.handle('outage:retry', async (event) => {
    const win = showingWindow(event)
    if (!win) return { ok: false, error: 'Nothing to retry.' }
    const url = targetFor(win)
    // Same race the portal probe in `show` guards, at the other call site: this
    // await lasts up to the probe timeout, and in it the user can pick Home or
    // connect a different server. Both clear the record, and a stale success
    // would then load the failed URL over the page they just got back.
    const since = generation(win)
    const result = await probe(url)
    if (!result.ok) return result
    if (movedOn(win, since)) {
      console.log('[outage] the window moved on while the retry probe was out; standing down')
      return { ok: false, error: 'Nothing to retry.' }
    }
    // `probe` calls a 500 reachable on purpose, since the server picker wants to
    // adopt a server that answers at all. A retry wants the opposite: reloading
    // renders the server's own error document and comes straight back here, so
    // the button would report success and then visibly fail.
    if (isServerError(result.status)) {
      return { ok: false, error: `The server answered with HTTP ${result.status}.` }
    }
    clear(win)
    onRetry(win, url)
    return { ok: true }
  })

  // Behind a portal our own address is exactly the thing that will not load;
  // the check URL is cleartext, so the portal answers it with the sign-in page.
  // Main picks the URL rather than accepting one, so a renderer cannot use this
  // to open anything it likes.
  ipcMain.handle('outage:open-external', (event) => {
    const win = showingWindow(event)
    if (!win) return false
    const url = reasonShowing(win) === 'captive-portal' ? captive.CHECK_URL : targetFor(win)
    // Caught for the same reason main's own opener is: `openExternal` rejects
    // when the OS has nothing to hand the URL to, and unhandled that reaches the
    // process handler and puts a modal error box on top of the outage screen,
    // which is the one surface in the app that must never raise one.
    shell.openExternal(url).catch((err) => {
      console.warn(`[outage] the system opener refused '${forDisplay(url)}': ${err.code || err.message}`)
    })
    return true
  })

  ipcMain.handle('outage:change-server', (event) => {
    const win = showingWindow(event)
    if (!win) return false
    const handler = windows.get(win)?.onChangeServer
    if (handler) handler()
    return !!handler
  })
}

/**
 * The window behind this call, but only while it is actually showing the outage
 * page. The preload already withholds these from remote pages; this is the half
 * that does not depend on the renderer behaving.
 */
function showingWindow(event) {
  const win = BrowserWindow.fromWebContents(event.sender)
  return win && isShowing(win) ? win : null
}

module.exports = {
  attach, show, registerIpc, isShowing, targetFor, reasonShowing, clear,
  shouldShow, isServerError, reasonFor, forDisplay, generation, movedOn,
}
