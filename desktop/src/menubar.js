'use strict'

const { BrowserWindow, screen } = require('electron')

const origins = require('./origins')
const { navigate } = require('./navigate')
const { panelPosition, panelSize, WIDTH, DEFAULT_HEIGHT } = require('./panelGeometry')

// The menu bar window: the app's `/desktop/menubar` page in a window of its own,
// kept for as long as the app runs.
//
// It has two jobs, and the second is why it outlives the icon. It is the panel a
// click on the menu bar icon opens, and it is the one page that posts the app's
// notifications: it holds the user's feed whether or not any other window is
// open, so a turn that finishes with every window closed still reaches the user.
// Turning the icon off takes the icon away and leaves the window.
//
// macOS and Windows show it as a panel anchored to the icon, as tall as its page
// (within bounds), hidden again when it loses focus. Linux shows it as an
// ordinary small window ("Market Glance"), because an AppIndicator reports no
// clicks and gives a panel nowhere to anchor.
//
// Focus: on macOS the panel is a non-activating panel (`type: 'panel'`), so
// show() and focus() make it the key window without activating the app. The app
// the user was in stays frontmost and the main window is not raised, yet the
// panel takes the keyboard (Esc closes it) and resigns key on a click anywhere
// else, which is the `blur` that hides it. On Windows the icon click is the user
// input that lets the panel take the foreground, with the same result.

const PAGE_PATH = '/desktop/menubar'
// Linux's window, which the user sizes; the panel sizes itself to its page.
const SIZE = { width: WIDTH, height: 600 }

// A click on the icon blurs an open panel before the click arrives, so the panel
// hides itself and the click would then open it again. A hide this recent counts
// as the click's own.
const BLUR_GRACE_MS = 250

// How long a loaded page has to call one of the menu bar's methods. The page
// sends its tray lines as it mounts, signed in or out.
const PROOF_MS = 30_000

// A crash this soon after the last crash-triggered reload is not reloaded again.
const CRASH_RELOAD_MS = 60_000

function pageUrl() {
  const origin = origins.appOrigin()
  return origin ? new URL(PAGE_PATH, origin).toString() : null
}

/** Whether a navigation inside the menu bar window stays there: only its own page does. */
function isMenubarPage(url) {
  if (!origins.isApp(url)) return false
  try {
    return new URL(url).pathname === PAGE_PATH
  } catch {
    return false
  }
}

/**
 * `route(url)` takes any navigation the page makes away from itself: a link to
 * the app opens in the main window, anything else goes wherever the main
 * window's own policy would send it. `recordFacts(win)` tells the page it is
 * the menu bar window. `anchor()` is the icon's bounds, which the panel opens
 * against. `isQuitting()` says a quit is under way, which is the one time the
 * window may close. `onUserClose()` hears the user close the Market Glance
 * window, which is one of their windows, so closing it can be closing the last.
 * `onUnproven()` stands in when asked to show a page that has not yet shown it
 * is the menu bar page (see `markProven`). `onPageStart()` hears each new load
 * of the page, which starts with none of the last one's state.
 */
function createMenubar({
  traits, preload, recordFacts, route, backgroundColor, anchor, isQuitting,
  onUserClose = () => {}, onUnproven = () => {}, onPageStart = () => {},
}) {
  const panel = Boolean(traits.panel)
  let win = null
  let failed = false
  // Whether the loaded page has called one of the menu bar's own methods. A
  // server whose web build predates the menu bar answers the page's address
  // with its whole app, which must never be what the panel shows.
  let proven = false
  let proofTimer = null
  // When a crash last reloaded the page at once; a page that keeps crashing
  // must not be reloaded in a tight loop.
  let lastCrashReload = 0
  let hiddenAt = 0
  // How tall the page says it is, from `preferred-size-changed`; null until it has.
  let preferredHeight = null
  // Windows a quit has let close. Until `closed` arrives one is still there, and
  // a quit cancelled in that gap needs a new window, not the one on its way out.
  const closing = new WeakSet()

  const alive = () => Boolean(win) && !win.isDestroyed() && !closing.has(win)

  function options() {
    const base = {
      width: SIZE.width,
      height: panel ? DEFAULT_HEIGHT : SIZE.height,
      show: false,
      // Otherwise the page reads itself visible until first shown and hidden,
      // and polls and streams quotes for a panel nobody has opened.
      paintWhenInitiallyHidden: false,
      backgroundColor: backgroundColor(),
      webPreferences: {
        contextIsolation: true,
        nodeIntegration: false,
        preload,
        // The panel is as tall as its page, which says how tall that is
        // through `preferred-size-changed`.
        enablePreferredSizeMode: panel,
        // Left throttled on purpose: the page reads its own visibility to stop
        // polling quotes while the panel is closed, and the feed it listens to
        // is a stream, which timer throttling does not slow.
      },
    }
    if (!panel) {
      return { ...base, title: 'Market Glance', minWidth: 320, minHeight: 360, autoHideMenuBar: true }
    }
    return {
      ...base,
      frame: false,
      resizable: false,
      movable: false,
      minimizable: false,
      maximizable: false,
      fullscreenable: false,
      skipTaskbar: true,
      // Under the menu bar, a non-activating panel (see the note on focus), on
      // the material macOS gives its own menu bar panels: the page leaves its
      // background clear and the desktop shows through, blurred.
      ...(traits.panel === 'menubar'
        ? {
            type: 'panel',
            hiddenInMissionControl: true,
            vibrancy: 'popover',
            visualEffectState: 'active',
            backgroundColor: '#00000000',
          }
        : { alwaysOnTop: true }),
    }
  }

  function attach(w) {
    const wc = w.webContents
    const police = (event, url) => {
      if (event && event.isMainFrame === false) return
      if (isMenubarPage(url)) return
      event.preventDefault()
      route(url)
      hide()
    }
    wc.on('will-navigate', police)
    wc.on('will-redirect', police)
    // That older app may also route itself away in place (a history push),
    // which no `will-navigate` reports. It is not this window's to run.
    wc.on('did-navigate-in-page', (_event, url, isMainFrame) => {
      if (isMainFrame === false || isMenubarPage(url)) return
      console.warn('[menubar] this server has no menu bar page; the main window stands in for it')
      proven = false
      failed = true
      wc.loadURL('about:blank')
    })
    wc.setWindowOpenHandler(({ url }) => {
      if (url !== 'about:blank') {
        route(url)
        hide()
      }
      return { action: 'deny' }
    })

    // A load that did not arrive is retried when the main window next reaches
    // the app, which is the moment the server is known to answer again.
    //
    // One log line per load, status only: this window is never on screen until
    // asked for, so the log is the only place a live check can see whether the
    // notification source is up. A failed load also finishes (on Chromium's
    // error document), so the failure is the line and the finish adds none.
    let status = 0
    let reported = false
    wc.on('did-start-loading', () => {
      onPageStart()
      reported = false
      status = 0
      proven = false
      clearTimeout(proofTimer)
    })
    wc.on('did-navigate', (_event, url, code) => {
      // The shell's own unload of a page that failed must not clear the failure.
      if (url === 'about:blank') return
      status = code
      failed = code >= 500
    })
    wc.on('did-fail-load', (_event, code, _description, _url, isMainFrame) => {
      if (isMainFrame === false || code === -3) return
      failed = true
      if (!reported) console.warn(`[menubar] the page failed to load (${code})`)
      reported = true
    })
    wc.on('did-finish-load', () => {
      if (!reported) console.log(`[menubar] the page loaded (HTTP ${status || '?'})`)
      reported = true
      // A page that never calls in is an older build's app, left running
      // hidden with its own feed and polling. Unload it; it is tried again when
      // the main window next reaches the app.
      if (failed || proven || wc.getURL() === 'about:blank') return
      clearTimeout(proofTimer)
      proofTimer = setTimeout(() => {
        if (proven || w !== win || !alive()) return
        console.warn('[menubar] the page never called in; unloading it')
        failed = true
        wc.loadURL('about:blank')
      }, PROOF_MS)
      proofTimer.unref?.()
    })

    // A crashed renderer leaves the window alive on a dead page, and this page
    // is the app's only notification source, so it is reloaded at once. A second
    // crash within a minute is left failed for the retry paths instead.
    wc.on('render-process-gone', (_event, details) => {
      if (w !== win) return
      console.warn(`[menubar] the page's renderer went away (${details && details.reason})`)
      proven = false
      failed = true
      clearTimeout(proofTimer)
      if (Date.now() - lastCrashReload <= CRASH_RELOAD_MS) return
      lastCrashReload = Date.now()
      reload()
    })

    // The page has nothing to save, so nothing it registers may hold up a quit:
    // a hidden window that refused would cancel the quit with nothing on screen
    // to say why.
    wc.on('will-prevent-unload', (event) => event.preventDefault())

    if (panel) {
      wc.on('preferred-size-changed', (_event, size) => {
        const height = size && Number(size.height)
        if (w !== win || !(height > 0) || height === preferredHeight) return
        preferredHeight = height
        // Open, it follows the page at once and stays anchored to the icon;
        // closed, the next show() sizes it.
        if (w.isVisible()) place()
      })
      w.on('blur', () => {
        if (!w.isVisible()) return
        hiddenAt = Date.now()
        w.hide()
      })
      wc.on('before-input-event', (event, input) => {
        if (input.type === 'keyDown' && input.key === 'Escape') {
          event.preventDefault()
          hide()
        }
      })
    }

    // Closing it hides it: it is the notification source, and the menu bar icon
    // has nothing to open without it. Only quitting takes it down.
    // Windows ends a session (shutdown, restart, sign-out) without the quit
    // events, and a window that refused to close there would hold it up.
    let sessionEnding = false
    w.on('session-end', () => { sessionEnding = true })
    w.on('close', (event) => {
      if (isQuitting() || sessionEnding) {
        closing.add(w)
        return
      }
      event.preventDefault()
      const wasOpen = w.isVisible()
      w.hide()
      if (!panel && wasOpen) onUserClose()
    })
    w.on('closed', () => { if (win === w) win = null })
  }

  /** Make the window if the app has an origin to load it from. Safe to call again. */
  function ensure() {
    if (alive()) return win
    const url = pageUrl()
    if (!url) return null
    preferredHeight = null
    win = new BrowserWindow(options())
    recordFacts(win)
    attach(win)
    if (traits.panel === 'menubar') {
      win.setVisibleOnAllWorkspaces(true, { visibleOnFullScreen: true, skipTransformProcessType: true })
    }
    failed = false
    navigate(win, url)
    return win
  }

  /** Load the page again from scratch, after the app's origin moved. */
  function reload() {
    const url = pageUrl()
    if (!alive() || !url) return
    failed = false
    navigate(win, url)
  }

  function reloadIfFailed() {
    if (alive() && failed) reload()
  }

  /**
   * After a sleep or a locked screen: its feed connection may be dead with
   * nothing to say so (a hidden page's timers are throttled), and a web deploy
   * since then left it on old code, which a page nobody looks at never learns
   * of. Reloaded only while closed, so the user never sees it happen.
   */
  function reloadIfHidden() {
    if (alive() && !win.isVisible()) reload()
  }

  /** Size the panel to its page and anchor it to the icon. Recomputed whole, so a panel that grows stays under (or beside) it. */
  function place() {
    const trayBounds = anchor()
    const display = trayBounds && trayBounds.width > 0
      ? screen.getDisplayMatching(trayBounds)
      : screen.getDisplayNearestPoint(screen.getCursorScreenPoint())
    const size = panelSize({ preferredHeight, workArea: display.workArea })
    const next = panelPosition({ anchor: traits.panel, trayBounds, display, size })
    const now = win.getBounds()
    if (now && now.x === next.x && now.y === next.y && now.width === next.width && now.height === next.height) return
    win.setBounds(next)
  }

  function show() {
    if (!ensure()) return
    if (!proven) {
      // A panel that failed after the main window already loaded the app has no
      // load left to retry it, so the click does.
      reloadIfFailed()
      onUnproven()
      return
    }
    if (panel) place()
    win.show()
    win.focus()
    // Whether the panel took key focus is what decides that a click elsewhere
    // or Esc closes it; a panel that did not take it stays up until the icon is
    // clicked again.
    if (panel && !win.isFocused()) console.warn('[menubar] the panel opened without key focus')
  }

  function hide() {
    if (alive() && win.isVisible()) {
      hiddenAt = Date.now()
      win.hide()
    }
  }

  function toggle() {
    if (!ensure()) return
    if (win.isVisible()) {
      hide()
      return
    }
    if (Date.now() - hiddenAt < BLUR_GRACE_MS) return
    show()
  }

  /** The Market Glance window is on screen, which makes it one of the user's windows. */
  function isOpenAsWindow() {
    return !panel && alive() && win.isVisible()
  }

  return {
    ensure,
    reload,
    reloadIfFailed,
    reloadIfHidden,
    show,
    hide,
    toggle,
    isOpenAsWindow,
    window: () => (alive() ? win : null),
    isMenubar: (w) => alive() && w === win,
    /** The panel on macOS's material, whose background the page keeps clear. */
    isMaterial: (w) => traits.panel === 'menubar' && alive() && w === win,
    /** The page called one of the menu bar's methods, so it is that page. */
    markProven: () => { proven = true },
  }
}

module.exports = { createMenubar, isMenubarPage, pageUrl, PAGE_PATH, SIZE }
