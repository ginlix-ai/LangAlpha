'use strict'

const path = require('node:path')
const { app, BrowserWindow, Menu, ipcMain, shell, dialog } = require('electron')

const config = require('./config')
const origins = require('./origins')
const policy = require('./policy')
const store = require('./store')
const oauth = require('./oauth')
const deeplink = require('./deeplink')
const theme = require('./theme')
const updater = require('./updater')
const outage = require('./outage')
const pdf = require('./pdf')
const downloads = require('./downloads')
const { probe } = require('./probe')
const { navigate } = require('./navigate')
const { forDisplay } = require('./redact')
const { buildMenu, syncNavMenu } = require('./menu')

// Before anything can ask where userData lives.
//
// `getPath('userData')` is derived from `app.getName()`, and `getName()` reads
// the packaged package.json `productName` — NOT the Info.plist `CFBundleName`
// electron-builder writes per edition. So without this the two editions ship
// correct bundle metadata, install side by side, appear separate, and then
// share one settings.json: whichever built last decides the directory both
// resolve. Setting it from `config` makes the runtime identity come from the
// same table the packaging does, and covers `electron .` too, where there is
// no plist at all.
app.setName(config.appName)

let mainWindow = null
let platformWindow = null
let setupWindow = null
// A deep link that arrived before this install had a server to resolve it
// against. An OSS first run opens the picker, and the link used to be dropped
// there: the user had to go back to the browser and click again, which a
// single-use magic link may not survive. Held until `server:use` gives it an
// origin, and only one, because the newest link is the one being acted on.
let deferredLink = null

const SETUP_DIR = path.join(__dirname, '..', 'setup')

// ---------------------------------------------------------------------------
// Navigation policy
// ---------------------------------------------------------------------------

/**
 * The verdict for a navigation, including the one case that is not a verdict at
 * all: an authorize URL the shell takes over itself. That check lives here
 * rather than in `policy` because taking it over opens the system browser and
 * parks a pending flow, and `policy` decides without doing.
 *
 * Returns 'handled' when OAuth took it, or whatever `policy` says otherwise.
 */
function decide(url, win, { isMainWindow }) {
  // A refused interception falls through to the ordinary policy, and must: the
  // authorize check matches on a pathname suffix, so ANY host can present
  // `/auth/v1/authorize?redirect_to=…` and be recognised. `begin` then declines
  // it, because the redirect_to is not ours — and answering 'allow' there let a
  // path decide what an origin is supposed to, loading the foreign page into
  // this window with the preload bridge. Declining to take a navigation over is
  // not the same as vouching for it.
  if (oauth.isAuthorizeUrl(url) && oauth.begin(url, win)) return 'handled'
  return policy.classifyNavigation(url, { isMainWindow })
}

/**
 * Hand a URL to the user's browser, or refuse.
 *
 * The one place the shell calls `shell.openExternal` with a URL that came from
 * a renderer. Both routes here — a navigation classified 'external', and the
 * bridge method the page can call directly — let the loaded page choose the
 * string, and `openExternal` is the OS's URL handler rather than a fetch, so an
 * unfiltered scheme is a way to launch whatever the user has installed.
 *
 * The outage and update paths do not come through here: main picks those URLs
 * itself, so there is nothing to validate.
 */
function openExternally(url) {
  if (!origins.isExternallyOpenable(url)) {
    console.warn(`[shell] refusing to open '${forDisplay(String(url)).slice(0, 120)}': not a browser scheme`)
    return false
  }
  // Caught rather than left floating. `openExternal` rejects when the OS has
  // nothing to hand the URL to, and an unhandled rejection here reaches the
  // process handler at the bottom of this file, which opens a modal error box.
  // A page that can make one rejection can make a thousand, and a thousand
  // synchronous modals is an app the user can only force-quit. A refusal to
  // open a link is worth a log line, not a dialog.
  shell.openExternal(url).catch((err) => {
    // Redacted for the same reason navigate() is: an external URL here can be a
    // magic link or a provider callback, so its query is a live credential. The
    // slice stays as a length bound; it was never a redaction.
    console.warn(`[shell] the system opener refused '${forDisplay(url).slice(0, 120)}': ${err.code || err.message}`)
  })
  return true
}

/**
 * Carry out a verdict. Never called with 'allow': letting a navigation happen
 * means something different to each caller, so each keeps that one line itself.
 *
 * `isTopLevel` is the single fact the two callers disagree on. A will-navigate
 * we swallow leaves the window sitting on whatever it was showing, and that has
 * two consequences: leaving the console for the app means the console window
 * has nothing left to show, and swallowing a navigation to somewhere foreign can
 * strand the window on a loading screen that reads as a hang. Neither applies to
 * a window.open, where the page that asked for a second surface is still using
 * the one it has.
 */
function applyVerdict(verdict, { url, win, isMainWindow, isTopLevel }) {
  if (verdict === 'handled') return

  if (verdict === 'app-window') {
    showInMainWindow(url)
    // Deferred: this runs inside that window's own event dispatch.
    if (isTopLevel) setImmediate(() => { if (!win.isDestroyed()) win.close() })
    return
  }

  if (verdict === 'platform-window') {
    openPlatformWindow(url)
    return
  }

  openExternally(url)
  // The outage page is the one non-ours page we must not navigate away from:
  // it is already the recovery surface.
  const here = win.webContents.getURL()
  // `here` is about:blank until something commits, and a cold-start entry URL
  // that redirects somewhere foreign gets here before anything has. Re-homing
  // then loads the same entry URL, which redirects again and opens another
  // browser tab, with nothing in the loop to stop it.
  const committed = !!here && here !== 'about:blank'
  if (isTopLevel && committed && !origins.isOurs(here) && !outage.isShowing(win)) {
    goHome(win, { isMainWindow })
  }
}

/**
 * Send `win` back to whatever the shell considers home. Home may be the app
 * while `win` is the console (the first-run case, where the sign-in window is
 * the only window there is), so it goes through the same handoff.
 */
function goHome(win, { isMainWindow }) {
  const home = policy.entryUrl()
  if (!home) return
  if (!isMainWindow && origins.isApp(home)) {
    showInMainWindow(home)
    win.close()
    return
  }
  // Home is a navigation like any other, so it answers to the same classifier
  // that policed the one the user is leaving. Before sign-in on a SaaS install
  // home is the platform page, and if the console reserves no strip while the
  // main window is frameless, loading it here is the buttons-over-the-header
  // case that the `platform-window` verdict exists to prevent.
  if (policy.classifyNavigation(home, { isMainWindow }) === 'platform-window') {
    openPlatformWindow(home)
    return
  }
  navigate(win, home)
}

function attachPolicy(win, { isMainWindow }) {
  const police = (event, url) => {
    // This policy decides where a WINDOW goes, so a subframe is out of scope:
    // cancelling one kills the frame's own load and `applyVerdict` may then
    // hand it to the system browser. `will-navigate` is main-frame-only in
    // modern Electron, but `will-redirect` fires for any frame, so a plain 302
    // inside an embedded chart was being read as the window leaving.
    // Absent is treated as the main frame: policing is the safe default.
    if (event && event.isMainFrame === false) return
    const verdict = decide(url, win, { isMainWindow })
    if (verdict === 'allow') return
    event.preventDefault()
    applyVerdict(verdict, { url, win, isMainWindow, isTopLevel: true })
  }

  // Both, because a 30x is not a navigation as far as `will-navigate` is
  // concerned: it fires once, for the URL the page asked for, and never again
  // for where the server sent it. On its own it leaves any origin that can
  // answer with a redirect — ours after a misconfiguration, or an attacker who
  // got one of our URLs to bounce — able to move this window somewhere the
  // policy never saw, with the preload bridge still attached.
  win.webContents.on('will-navigate', police)
  win.webContents.on('will-redirect', police)

  win.webContents.setWindowOpenHandler(({ url }) => {
    // The only about:blank popup in either app is the OAuth one, opened
    // synchronously to keep the user gesture. Denying it makes both SPAs take
    // their popup-blocked fallback, a same-tab navigation to the authorize URL,
    // which will-navigate above then intercepts.
    if (url === 'about:blank') return { action: 'deny' }

    const verdict = decide(url, win, { isMainWindow })
    // 'allow' means the URL is ours, which for a *navigation* means "stay put".
    // A window.open is the opposite request — a second surface — and loading it
    // into the first one is not a lesser version of that, it is the destructive
    // one: the app opens served HTML this way for its new-tab and print actions,
    // and doing it here replaced whatever turn was streaming in this window.
    // The shell keeps no popup windows, so the second surface is the browser.
    if (verdict === 'allow') openExternally(url)
    else applyVerdict(verdict, { url, win, isMainWindow, isTopLevel: false })
    return { action: 'deny' }
  })

  // A remote-URL shell shows a blank window when the network does not answer.
  // This is the screen that replaces it.
  outage.attach(win, {
    onChangeServer: config.isSaas ? null : showServerPicker,
  })

  // The History menu is built once, so its enablement has to be pushed at it.
  // `did-navigate-in-page` is the one that matters most: both apps are SPAs and
  // almost every navigation a user makes is a pushState, not a document load.
  // Gated on focus because the menu is global and its click handler is
  // delivered to the FOCUSED window: a pushState in the background console
  // would otherwise publish its own history as the app window's, greying out a
  // Back the app can do. The focus handler below is unconditional, so a window
  // that navigated while hidden still corrects the menu when it comes forward.
  const syncIfFocused = () => { if (!win.isDestroyed() && win.isFocused()) syncNavMenu(win) }
  win.webContents.on('did-navigate', syncIfFocused)
  win.webContents.on('did-navigate-in-page', syncIfFocused)
  win.on('focus', () => syncNavMenu(win))
}

// ---------------------------------------------------------------------------
// Menu actions
// ---------------------------------------------------------------------------

/**
 * Reload means "get me back to the app", and on the outage page that is a
 * different URL from the one the window is showing.
 *
 * Probe before loading, the same order the page's own Retry button uses: a
 * reload that cannot succeed would otherwise flash the app's loading screen and
 * bounce straight back here. A failed probe re-renders the outage page rather
 * than returning silently, so the keystroke always visibly did something.
 */
async function reloadWindow(win) {
  if (!outage.isShowing(win)) {
    win.webContents.reload()
    return
  }
  const url = outage.targetFor(win)
  if (!url) return
  const since = outage.generation(win)
  const result = await probe(url)
  // The same race the page's own Retry button guards: this await lasts up to the
  // probe timeout, and in it the user can pick Home or connect a different
  // server. Both clear the record, and a stale result would then load the failed
  // URL over the page they just got back, or repaint the outage on top of it.
  if (outage.movedOn(win, since)) {
    console.log('[outage] the window moved on while the reload probe was out; standing down')
    return
  }
  if (result.ok && !outage.isServerError(result.status)) {
    outage.clear(win)
    navigate(win, url)
    return
  }
  // A 500 is `ok` to `probe`, because for the server picker a server that
  // answers is the right server. Reloading into one only renders its error
  // document and lands back here, so the outage page keeps the window instead.
  const description = outage.isServerError(result.status)
    ? `The server answered with HTTP ${result.status}.`
    : result.error
  await outage.show(win, { target: url, status: result.status, description })
}

/**
 * Hand the current page to the real browser. On the outage page the address bar
 * holds a `file://` URL, which is both useless to the user and not something to
 * pass to the system opener, so the app URL it is standing in for goes instead.
 */
function openCurrentInBrowser(win) {
  const url = outage.isShowing(win) ? outage.targetFor(win) : win.webContents.getURL()
  if (url) openExternally(url)
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

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

/** Everything a completed load teaches the shell about the page it just loaded. */
function watchLoads(win) {
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

  win.webContents.on('did-finish-load', async () => {
    const url = win.webContents.getURL()
    // No match means the status belongs to some other navigation, and 0 is the
    // honest answer: every reader below treats it as "not known to be served",
    // which keeps a stale flag rather than revoking one on a guess.
    const lastStatus = lastNav.url === url ? lastNav.status : 0
    noteAppReached(url, lastStatus)

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
 * What a window tells its page about itself.
 *
 * `window-chrome` is the authoritative answer to "did this shell hide the
 * titlebar", and it is the only input to whether the page reserves the button
 * strip. Per window rather than per platform: the console is framed in a window
 * of its own and hidden when it shares the main one, and the page has no way to
 * work that out for itself.
 */
function shellArgs({ chromeHidden }) {
  return [
    // The sandboxed preload cannot read package.json, so the version travels
    // as a switch. See the note in preload.js.
    `--langalpha-shell-version=${app.getVersion()}`,
    `--langalpha-window-chrome=${chromeHidden ? 'hidden' : 'native'}`,
    // Which scheme this edition answers on. The page cannot derive it: both
    // editions install side by side, and an email link marked for the wrong one
    // opens the other build, or nothing.
    `--langalpha-shell-scheme=${config.scheme}`,
  ]
}

function createMainWindow(url) {
  // Read once, then used twice: it sets the frame and it is what the page is
  // told about the frame. Two reads could not disagree today, but the page
  // reserving a strip this window does not have is exactly the bug being fixed,
  // so there is one answer and both sides get it.
  //
  // `hiddenInset` is macOS-only, so on Windows and Linux the titlebar is there
  // whatever the store says. Folded in here to keep the flag honest rather than
  // asking every consumer to re-derive the platform rule.
  const chromeHidden = process.platform === 'darwin' && store.get('appChrome')
  mainWindow = new BrowserWindow({
    width: 1440,
    height: 900,
    // Wider than the app's 767px phone breakpoint. Below that the sidebar stops
    // rendering, and the sidebar is what reserves the strip the window buttons
    // float over, so a narrower window lays the phone UI out underneath them.
    // 800 rather than 768 so the frame never lands on the boundary itself.
    minWidth: 800,
    minHeight: 520,
    // Hiding the titlebar hands the top strip to the page, so it is only safe
    // for a build that has said it takes it. See `appChrome` in store.js.
    titleBarStyle: chromeHidden ? 'hiddenInset' : 'default',
    backgroundColor: theme.backgroundFor(store.get('theme')),
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      // A research turn streams for minutes and the user will be in another
      // window for most of it; throttling would stall the SSE consumer.
      backgroundThrottling: false,
      preload: path.join(__dirname, 'preload.js'),
      additionalArguments: shellArgs({ chromeHidden }),
    },
  })

  attachPolicy(mainWindow, { isMainWindow: true })
  watchLoads(mainWindow)
  mainWindow.on('closed', () => { mainWindow = null })
  navigate(mainWindow, url)
  return mainWindow
}

/** The app, always in the main window. See the note on `classify`. */
function showInMainWindow(url) {
  if (!mainWindow || mainWindow.isDestroyed()) return createMainWindow(url)
  navigate(mainWindow, url)
  mainWindow.show()
  mainWindow.focus()
  return mainWindow
}

/**
 * What a launch (or a dock click with no windows left) opens. An OSS build with
 * no server configured has nothing to show, so it gets the picker and no empty
 * frame behind it.
 */
function openInitialWindow() {
  // Never reload a live window: a dock click during a streaming turn must not
  // cost the user the turn.
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.focus()
    return mainWindow
  }
  const url = policy.entryUrl()
  if (!url) {
    showServerPicker()
    return null
  }
  // Sign-in and onboarding are platform pages. They open in the main window on
  // the same terms as any other console page, which on a first run is always
  // (nothing has taught this install to hide its titlebar yet).
  if (origins.isPlatform(url) && !policy.platformFitsMainWindow()) return openPlatformWindow(url)
  return showInMainWindow(url)
}

/**
 * The console in a window of its own: the fallback for when it cannot share the
 * main one (`policy.platformFitsMainWindow`). Same Electron session so cookies carry,
 * and a standard titlebar, since a page that reserves no strip in a window with
 * no titlebar is a window with no visible way to close it.
 */
function openPlatformWindow(url) {
  if (platformWindow && !platformWindow.isDestroyed()) {
    platformWindow.focus()
    if (url) navigate(platformWindow, url)
    return platformWindow
  }
  platformWindow = new BrowserWindow({
    width: 1100,
    height: 820,
    title: 'Account',
    backgroundColor: theme.backgroundFor(store.get('theme')),
    // The same bridge the app gets. It is origin-agnostic by design (version,
    // platform, theme, open-in-browser) and this window is now a first run's
    // only window, so leaving it out would take the shell's theme sync away
    // from the one screen a new user actually sees first.
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      preload: path.join(__dirname, 'preload.js'),
      // This window keeps its titlebar unconditionally, so it says so, whatever
      // the console's own flag happens to be.
      additionalArguments: shellArgs({ chromeHidden: false }),
    },
  })
  platformWindow.on('closed', () => { platformWindow = null })
  attachPolicy(platformWindow, { isMainWindow: false })
  // Watched here too, so the day the console reserves the strip is the day this
  // window stops being needed, without anyone having to notice.
  watchLoads(platformWindow)
  navigate(platformWindow, url)
  return platformWindow
}

function focusedOrMain() {
  return BrowserWindow.getFocusedWindow() || mainWindow
}

// ---------------------------------------------------------------------------
// OSS server picker
// ---------------------------------------------------------------------------

/**
 * Its own window, with its own preload. Folding this into the main window would
 * mean the remote page's bridge also carried `server:use`, which repoints the
 * whole app, not something to hand to whatever the loaded page becomes.
 */
function showServerPicker() {
  if (setupWindow && !setupWindow.isDestroyed()) {
    setupWindow.focus()
    return setupWindow
  }
  setupWindow = new BrowserWindow({
    width: 520,
    height: 460,
    resizable: false,
    title: 'Connect to your server',
    backgroundColor: theme.backgroundFor(store.get('theme')),
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      preload: path.join(SETUP_DIR, 'setup-preload.js'),
    },
  })
  setupWindow.on('closed', () => {
    setupWindow = null
    // Closing the picker with nothing configured leaves no way back in, so the
    // app has nothing left to do.
    if (!store.get('serverUrl') && (!mainWindow || mainWindow.isDestroyed())) app.quit()
  })
  setupWindow.loadFile(path.join(SETUP_DIR, 'setup.html'))
  return setupWindow
}

// ---------------------------------------------------------------------------
// IPC
// ---------------------------------------------------------------------------

/**
 * Point this install at a stack: validate it, forget what the last one taught
 * the shell, and store it. Separated from the IPC handler because everything
 * here is a decision about persisted state, and the handler around it is window
 * work — the same split `policy` and `main` keep everywhere else.
 */
function adoptServer(rawUrl) {
  let parsed
  try {
    parsed = new URL(rawUrl)
  } catch {
    return { ok: false, error: 'That is not a valid URL.' }
  }
  // The scheme, not just the parse. `new URL('file:///x')` parses, and its
  // origin is the *string* "null" — truthy, so it stores fine and `entryUrl`
  // then treats the picker as answered, while `origins.appOrigin` throws on it
  // and falls back to the compiled default. The result is an install that can
  // never reach its picker again. The setup page prefixes `http://` and probes
  // before it gets here, so this guards the IPC boundary rather than that flow.
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return { ok: false, error: 'Use an http:// or https:// address.' }
  }
  // A path is not part of an origin, and `parsed.origin` drops it silently. The
  // setup page would probe `https://host/langalpha`, report that it reached it,
  // and then open `https://host/`. Refusing is the honest answer rather than
  // quietly opening somewhere else: nothing downstream is path-aware, since
  // `origins` compares origins throughout.
  if ((parsed.pathname && parsed.pathname !== '/') || parsed.search || parsed.hash) {
    return { ok: false, error: 'Enter the address only, without a path.' }
  }
  // Credentials are dropped by `parsed.origin` exactly as a path is, and the
  // path check above does not see them: `https://u:p@host` has pathname '/'. The
  // honest answer is the same one — a stack that needs basic auth cannot be
  // reached by storing an origin that no longer carries it, so say so here
  // rather than opening an address the user did not type.
  if (parsed.username || parsed.password) {
    return { ok: false, error: 'Enter the address only, without a username or password.' }
  }
  const origin = parsed.origin
  // The chrome flags are learned from a declaration the loaded page makes, so
  // they are an answer about the origin that gave it, not about this install.
  // Pointing at a different stack makes them answers about somewhere else: a
  // server that ships the declaration followed by one that does not left the old
  // `true` in place (a page that cannot answer is deliberately not read as a
  // "no"), and the next launch opened frameless against a build that reserves
  // nothing — window buttons over the app's own header, and no drag region
  // anywhere. Which is the exact failure this mechanism exists to prevent.
  if (origin !== store.get('serverUrl')) {
    store.reset('appChrome')
    store.reset('platformChrome')
  }
  store.set('serverUrl', origin)
  return { ok: true, origin }
}

/**
 * Resolve a deep link against where the window is and go there.
 *
 * `base` overrides that for the one case where the window has not loaded
 * anything yet: the link arrived at the picker, and the origin it belongs to is
 * the one just adopted. Returns whether the link actually resolved, so a caller
 * with a fallback destination knows whether it was used.
 */
function openDeepLink(win, raw, base) {
  const target = deeplink.toAppUrl(raw, base || win.webContents.getURL())
  if (!target) return false
  win.show()
  win.focus()
  navigate(win, target)
  return true
}

function registerIpc() {
  ipcMain.handle('shell:open-external', (_event, url) => openExternally(url))

  // The one auth-shaped thing the page may ask for, and it hands back a local
  // URL rather than anything secret. Our own sign-in stays intercepted and
  // unreachable from here; this is for a connector whose authorization server
  // refuses a hosted callback, where the page has to name the loopback URI when
  // it asks its backend to mint the flow. `oauth.beginMcp` re-checks the asking
  // window itself, so a message from anywhere unexpected is refused there.
  ipcMain.handle('shell:mcp-oauth-begin', (event, returnUrl) => {
    const win = BrowserWindow.fromWebContents(event.sender)
    return win ? oauth.beginMcp(returnUrl, win) : null
  })

  // The second half of the handshake: the flow's `state` exists only after the
  // backend has minted it, and until the shell has been told it, the armed flow
  // accepts no callback. `oauth.bindMcp` re-checks the window and the flow id.
  ipcMain.handle('shell:mcp-oauth-bind', (event, flowId, state) => {
    const win = BrowserWindow.fromWebContents(event.sender)
    return win ? oauth.bindMcp(win, flowId, state) : false
  })

  // The other half of that handshake: the page armed before it knew whether
  // its backend would mint a flow at all, so it needs a way to say it did not.
  // Named by flow id, so a start that failed cannot stand down a later connect
  // that is still running.
  ipcMain.handle('shell:mcp-oauth-cancel', (event, flowId) => {
    const win = BrowserWindow.fromWebContents(event.sender)
    return win ? oauth.cancelMcp(win, flowId) : false
  })

  // Theme has to come from the page: the shell cannot read a CSS variable, and
  // the user's choice lives in the renderer's localStorage, not the OS setting.
  ipcMain.on('shell:set-theme', (event, value) => {
    // Answered rather than dropped: the ack exists so a renderer can tell a
    // shell that understands this message from one that does not, and returning
    // undefined for a value this shell simply rejected says the wrong thing.
    if (!theme.isTheme(value)) {
      event.returnValue = false
      return
    }
    // Painted before the store is consulted, because the guard below is about
    // the disk write and this is an in-memory native call. A window is created
    // with whatever colour the store held then, so a sibling window moving the
    // store between this one's creation and its first report would leave this
    // one wearing the old theme with the guard suppressing the correction.
    //
    // Only the window that reported it. The app and the console are separate
    // deploys resolving their own preference, so painting both from one message
    // gives the other window a frame in a theme its page is not using, which is
    // the mismatched resize band this exists to prevent.
    const reporting = BrowserWindow.fromWebContents(event.sender)
    if (reporting && !reporting.isDestroyed()) reporting.setBackgroundColor(theme.backgroundFor(value))
    // Unchanged is the common case, not the rare one: this is sendSync, so the
    // renderer blocks on it, and the effect that calls it runs on ordinary
    // renders rather than only on a real theme change. Writing anyway put a
    // synchronous mkdir and a synchronous file write on the main event loop of
    // every one of them, which is the whole app — menus, both windows — waiting
    // on a disk for a value that already had that value.
    if (store.get('theme') !== value) store.set('theme', value)
    // Acknowledged only so a renderer can tell an old shell from a new one.
    event.returnValue = true
  })

  outage.registerIpc({ onRetry: (win, url) => navigate(win, url) })

  ipcMain.handle('server:probe', async (_event, rawUrl) => probe(rawUrl))

  ipcMain.handle('server:use', (_event, rawUrl) => {
    const result = adoptServer(rawUrl)
    if (!result.ok) return result
    const { origin } = result
    const win = mainWindow && !mainWindow.isDestroyed() ? mainWindow : createMainWindow(origin)
    // A link that arrived while the picker was up now has an origin to resolve
    // against, so it lands instead of the bare app root. Cleared either way:
    // one that no longer maps to this server must not outlive this attempt.
    const link = deferredLink
    deferredLink = null
    if (link && openDeepLink(win, link, origin)) {
      if (setupWindow && !setupWindow.isDestroyed()) setupWindow.close()
      return result
    }
    navigate(win, origin)
    win.show()
    win.focus()
    if (setupWindow && !setupWindow.isDestroyed()) setupWindow.close()
    return result
  })

  ipcMain.handle('server:current', () => store.get('serverUrl'))

  pdf.registerIpc()
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Without this the second launch of a deep link starts a whole new process and
// the running instance never hears about the URL.
if (!app.requestSingleInstanceLock()) {
  app.quit()
} else {
  // Whichever window this install currently has, not only the main one: an OSS
  // first run has the server picker and no main window, and a SaaS fallback can
  // have only the account window. Returning early there made launching the app
  // again do nothing at all, because this process has already given up the
  // single-instance lock and the live window stayed buried.
  app.on('second-instance', () => {
    const win = mainWindow || setupWindow || platformWindow
    if (!win || win.isDestroyed()) return
    const which = win === mainWindow ? 'app' : win === setupWindow ? 'server picker' : 'account'
    console.log(`[shell] a second launch; focusing the ${which} window`)
    if (win.isMinimized()) win.restore()
    win.show()
    win.focus()
  })

  deeplink.init()

  app.whenReady().then(async () => {
    await oauth.startCallbackServer()
    registerIpc()

    Menu.setApplicationMenu(buildMenu({
      isSaas: config.isSaas,
      onChangeServer: showServerPicker,
      onOpenAccount: () => {
        const platform = origins.platformOrigin()
        if (!platform) return
        // The menu item and the app's own "Usage & Plan" link are the same
        // journey and must not land in different windows.
        if (policy.platformFitsMainWindow()) showInMainWindow(platform)
        else openPlatformWindow(platform)
      },
      onCheckForUpdates: (win) => updater.checkManually(win || focusedOrMain()),
      onReload: reloadWindow,
      onBack: (win) => {
        const history = win.webContents.navigationHistory
        if (history.canGoBack()) history.goBack()
      },
      onForward: (win) => {
        const history = win.webContents.navigationHistory
        if (history.canGoForward()) history.goForward()
      },
      onHome: (win) => goHome(win, { isMainWindow: win === mainWindow }),
      onOpenInBrowser: openCurrentInBrowser,
    }))

    deeplink.attach((raw) => {
      // The main window, never the focused one. A deep link resolves to a URL on
      // the app origin, and the server picker is the one window that must never
      // load one: its preload carries `server:use`, which repoints the whole
      // app, and it is deliberately outside the navigation policy because
      // nothing remote was ever supposed to reach it. A link arriving while the
      // picker had focus put a remote page in exactly that window.
      const win = (mainWindow && !mainWindow.isDestroyed()) ? mainWindow : openInitialWindow()
      if (!win) {
        // The picker is up, so there is no app origin to resolve against yet.
        deferredLink = raw
        return
      }
      openDeepLink(win, raw)
    })

    openInitialWindow()
    updater.init(() => mainWindow)

    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) openInitialWindow()
    })
  })
}

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})

// Release the loopback port on the way out, and with it every flow still armed
// on it. The port itself the OS reclaims either way; what this is for is the
// flows, which would otherwise sit on timers in a process that is leaving.
app.on('before-quit', () => {
  oauth.stopCallbackServer()
  updater.stop()
})

// A remote-URL shell loads code it did not build; a stray permission prompt from
// a compromised page is not something to leave to the default.
app.on('web-contents-created', (_event, contents) => {
  const permitted = (permission) => (
    permission === 'clipboard-sanitized-write' || permission === 'fullscreen'
  )
  contents.session.setPermissionRequestHandler((_wc, permission, callback, details) => {
    const allowed = permitted(permission)
    if (!allowed) console.log(`[shell] denied ${permission} for ${details.requestingUrl || '?'}`)
    callback(allowed)
  })
  // The other door. Electron asks the *request* handler for permissions that
  // prompt and the *check* handler for the ones answered synchronously —
  // `navigator.permissions.query`, device enumeration, clipboard reads — and a
  // session with only the first set falls through to the defaults for the
  // second. Same answer from both, or the policy is only half a policy.
  contents.session.setPermissionCheckHandler((_wc, permission) => permitted(permission))
  downloads.attach(contents.session)
})

// Exported for the tests: the verdict path that policy cannot own (it opens the
// system browser), the one store write a completed load makes, and the load
// watcher, whose status/URL pairing is only observable by driving both events.
module.exports = { decide, noteAppReached, adoptServer, watchLoads }

// Surfacing this as a dialog rather than a silent log: an unhandled rejection in
// the main process leaves the window alive but the shell half-dead, which is
// indistinguishable from a hung page.
process.on('unhandledRejection', (reason) => {
  console.error('[shell] unhandled rejection', reason)
  if (app.isReady()) {
    dialog.showErrorBox(app.getName(), `Something went wrong in the app shell:\n\n${reason}`)
  }
})
