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
const { buildMenu, syncNavMenu, syncCommandMenu, syncMenubarItem } = require('./menu')
const { createRemoteControl, registerIpc: registerRemoteControlIpc } = require('./remoteControl')
const { traits: platformTraits } = require('./platformTraits')
const { createLoginItem, HIDDEN_FLAG } = require('./loginItem')
const { createSettingsService } = require('./desktopSettingsService')
const { createShortcut } = require('./shortcut')
const { createLifecycle } = require('./lifecycle')
const { createGlance } = require('./glance')
const { createCommands, commandFromArgv } = require('./commands')
const { alive, appSenderWindow } = require('./sender')
const { recordWindowFacts, windowFactsOf, watchLoads } = require('./pageFacts')

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

const traits = platformTraits()

// Windows files a toast under the process's AppUserModelID and shows none whose
// id has no Start menu shortcut, which the installer makes under the bundle id.
// Set before anything can post one.
if (traits.appUserModelId) app.setAppUserModelId(config.appId)

let mainWindow = null
let platformWindow = null
let setupWindow = null
// A deep link that arrived before this install had a server to resolve it
// against. An OSS first run opens the picker, and the link used to be dropped
// there: the user had to go back to the browser and click again, which a
// single-use magic link may not survive. Held until `server:use` gives it an
// origin, and only one, because the newest link is the one being acted on.
let deferredLink = null
// The window whose page last sent the user to the system browser. An
// integration login or install leaves through the bridge's `openExternal`, so
// this is the window it was started in. Taken from the sender, not from focus
// or blur: those move with every switch between our windows and the browser,
// and a second launch moves focus again before the link it carries is
// delivered.
let lastSentOutWindow = null

const remoteControl = createRemoteControl({ onMenuChange: installMenu })

/**
 * The user's windows that are open: the app, the server picker, the account
 * window. Never the menu bar window, which is the shell's and always there.
 */
const userWindows = () => [mainWindow, setupWindow, platformWindow].filter(alive)

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
function decide(url, win, { isMainWindow, isWindowOpen = false }) {
  // A refused interception falls through to the ordinary policy, and must: the
  // authorize check matches on a pathname suffix, so ANY host can present
  // `/auth/v1/authorize?redirect_to=…` and be recognised. `begin` then declines
  // it, because the redirect_to is not ours — and answering 'allow' there let a
  // path decide what an origin is supposed to, loading the foreign page into
  // this window with the preload bridge. Declining to take a navigation over is
  // not the same as vouching for it.
  if (oauth.isAuthorizeUrl(url) && oauth.begin(url, win)) return 'handled'
  if (isWindowOpen) {
    return policy.classifyWindowOpen(url, { isMainWindow, openerUrl: win.webContents.getURL() })
  }
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

/** The bridge's `openExternal`, remembering which window's page asked. */
function openExternallyFrom(event, url) {
  const opened = openExternally(url)
  const win = BrowserWindow.fromWebContents(event.sender)
  if (opened && win) lastSentOutWindow = win
  return opened
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

    // Never 'allow': a window.open of ours is a request for a second surface,
    // which `classifyWindowOpen` resolves to a window or the browser.
    applyVerdict(decide(url, win, { isMainWindow, isWindowOpen: true }), { url, win, isMainWindow, isTopLevel: false })
    return { action: 'deny' }
  })

  // A remote-URL shell shows a blank window when the network does not answer.
  // This is the screen that replaces it.
  outage.attach(win, {
    onChangeServer: config.isSaas ? null : showServerPicker,
  })

  // The Go menu is built once, so its enablement has to be pushed at it.
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
    },
  })
  recordWindowFacts(mainWindow, { chromeHidden, role: 'main' })

  attachPolicy(mainWindow, { isMainWindow: true })
  watchLoads(mainWindow, { onFinish: (url, status) => glance.onMainLoadedApp(url, status) })
  lifecycle.watchQuitCancel(mainWindow)
  mainWindow.on('closed', () => {
    mainWindow = null
    lifecycle.afterUserWindowClosed()
  })
  commands.attachMainWindow(mainWindow)
  navigate(mainWindow, url)
  glance.applyOverlay()
  return mainWindow
}

/** The app, always in the main window. See the note on `classify`. */
function showInMainWindow(url) {
  if (!alive(mainWindow)) return createMainWindow(url)
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
  if (alive(mainWindow)) {
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
  if (alive(platformWindow)) {
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
    },
  })
  // This window keeps its titlebar unconditionally, so it says so, whatever the
  // console's own flag happens to be.
  recordWindowFacts(platformWindow, { chromeHidden: false })
  platformWindow.on('closed', () => {
    platformWindow = null
    lifecycle.afterUserWindowClosed()
  })
  attachPolicy(platformWindow, { isMainWindow: false })
  lifecycle.watchQuitCancel(platformWindow)
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
// Menu bar, notifications, menu commands, desktop settings: wired to the
// windows main owns. Each lives in its own module (see the layout table in
// AGENTS.md).
// ---------------------------------------------------------------------------

/**
 * Raise the main window, making it if there is none (at `url` when given), and
 * answer it. Without a URL and without a window, a launch's own first window
 * opens instead, which may be the picker or the console's.
 */
function showMain(url) {
  if (!alive(mainWindow)) return url ? showInMainWindow(url) : openInitialWindow()
  if (mainWindow.isMinimized()) mainWindow.restore()
  mainWindow.show()
  mainWindow.focus()
  return mainWindow
}

/**
 * Where a navigation out of the menu bar page goes. A link into the app is an
 * `open` for the main window, which its page carries out in place rather than
 * reloading over a turn that may be streaming there.
 */
function routeFromMenubar(url) {
  if (origins.isApp(url)) {
    const u = new URL(url)
    commands.deliver({ command: 'open', path: `${u.pathname}${u.search}${u.hash}` })
    return
  }
  if (origins.isPlatform(url)) {
    const verdict = policy.classifyNavigation(url, { isMainWindow: true })
    if (verdict === 'platform-window') openPlatformWindow(url)
    else showInMainWindow(url)
    return
  }
  openExternally(url)
}

const commands = createCommands({
  getMainWindow: () => mainWindow,
  showMain,
  loadInMain: (url) => navigate(mainWindow, url),
  appUrl: policy.appPathUrl,
  syncMenu: syncCommandMenu,
})

const lifecycle = createLifecycle({
  traits,
  // Linux's Market Glance window, on screen, is one of the user's windows.
  userWindowOpen: () => userWindows().length > 0 || glance.isOpenAsWindow(),
  keepInTray: () => glance.keepRunningInTray(),
  onQuitCancelled: () => glance.ensure(),
})

const shortcut = createShortcut({ supported: traits.globalShortcut, onPress: () => glance.toggle() })

const loginItem = createLoginItem(traits)

const settings = createSettingsService({ store, traits, loginItem, shortcutError: shortcut.error })

const glance = createGlance({
  traits,
  settings,
  commands,
  lifecycle,
  store,
  host: {
    mainWindow: () => mainWindow,
    showMain,
    openInitialWindow,
    route: routeFromMenubar,
    // Framed as far as the page is concerned: a panel has no window buttons to
    // keep clear of, so it reserves no strip.
    recordFacts: (win) => recordWindowFacts(win, { chromeHidden: false, role: 'menubar' }),
  },
})

settings.onChange((next, prev) => {
  if (prev.shortcut !== next.shortcut) shortcut.apply(next.shortcut)
  if (prev.menubar.enabled !== next.menubar.enabled) syncMenubarItem(next.menubar.enabled)
})

// ---------------------------------------------------------------------------
// OSS server picker
// ---------------------------------------------------------------------------

/**
 * Its own window, with its own preload. Folding this into the main window would
 * mean the remote page's bridge also carried `server:use`, which repoints the
 * whole app, not something to hand to whatever the loaded page becomes.
 */
function showServerPicker() {
  if (alive(setupWindow)) {
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
    if (!store.get('serverUrl') && !alive(mainWindow)) {
      app.quit()
      return
    }
    // With a server, the picker is one of the user's windows like any other,
    // and closing it can be closing the last.
    lifecycle.afterUserWindowClosed()
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

/**
 * A return from the browser, already mapped onto a console page: an
 * integration login or install landing on the console's callback page, or a
 * console page asked to be shown again after a trip to a third party's page.
 * It goes back to the page that started it: the console window that sent the
 * user to the browser, or else any window showing the console. Either can be
 * the one, and both can show it at once: an account window opened before the
 * console reserved the strip stays open after the main window starts showing
 * the console too.
 *
 * With no window showing the console, it goes wherever a navigation to the
 * console goes, decided here and before the load: `navigate` is loadURL and
 * never passes through will-navigate, so a console URL loaded straight into
 * the main window would skip the routing that gives the console a window of
 * its own when the main one cannot show it.
 *
 * Restored as well as raised: the window may have been minimized while the
 * browser had the user, and show() does not bring a minimized window back.
 */
function openConsoleReturn(url) {
  const showsConsole = (w) => alive(w)
    && (w === platformWindow || (w === mainWindow && origins.isPlatform(w.webContents.getURL())))
  let win = [lastSentOutWindow, mainWindow, platformWindow].find(showsConsole)
  if (win) {
    navigate(win, url)
  } else {
    const verdict = policy.classifyNavigation(url, { isMainWindow: true })
    if (verdict === 'allow') win = showInMainWindow(url)
    else if (verdict === 'platform-window') win = openPlatformWindow(url)
    else return null
  }
  if (win.isMinimized()) win.restore()
  win.show()
  win.focus()
  return win
}

// What the log says about a link addressed to the console that this build
// refuses, by the kind `deeplink.toConsole` read it as. Never any of the link:
// its values are the provider's or the third party's answer.
const REFUSED_CONSOLE_LINK = {
  integration: '[shell] ignoring an integration link this build cannot finish',
  console: '[shell] ignoring a console link this build cannot open',
}

/** Where a link the OS handed us goes: every arrival path ends here. */
function onDeepLink(raw) {
  // Settled before any window is picked, because the answer picks it, and never
  // left to fall through: the app's /callback would try to redeem the
  // provider's answer as a sign-in. That covers an integration login or
  // install and any link under the console host, so a console page this build
  // does not know is dropped rather than redeemed, and it includes an edition
  // with no console, which has nowhere to finish one.
  const link = deeplink.toConsole(raw)
  if (link) {
    if (link.url) openConsoleReturn(link.url)
    else console.warn(REFUSED_CONSOLE_LINK[link.kind])
    return
  }

  // The main window, never the focused one. A deep link resolves to a URL on
  // the app origin, and the server picker is the one window that must never
  // load one: its preload carries `server:use`, which repoints the whole
  // app, and it is deliberately outside the navigation policy because
  // nothing remote was ever supposed to reach it. A link arriving while the
  // picker had focus put a remote page in exactly that window.
  const win = alive(mainWindow) ? mainWindow : openInitialWindow()
  if (!win) {
    // The picker is up, so there is no app origin to resolve against yet.
    deferredLink = raw
    return
  }
  openDeepLink(win, raw)
}

/**
 * The windows a launch opens. A link the app was launched by is waiting in
 * deeplink's queue and is delivered by `attach` before anything else opens, so
 * it has already put up the window it belongs in. Opening the entry after it
 * would cover that window, or, when the entry is the console's own window,
 * load sign-in over the page the link just opened.
 */
function openLaunchWindows() {
  deeplink.attach(onDeepLink)
  if (userWindows().length === 0) openInitialWindow()
}

function registerIpc() {
  ipcMain.handle('shell:open-external', openExternallyFrom)
  // Synchronous because the preload needs the answer before the page's first
  // script runs: index.html stamps the frame from it ahead of the bundle. A
  // window the shell did not record gets null, which the preload reads as
  // "framed", the answer that never hides a control under the window buttons.
  ipcMain.on('shell:window-facts', (event) => {
    event.returnValue = windowFactsOf(event.sender)
  })

  oauth.registerIpc()
  theme.registerIpc({ ownsBackground: glance.isMaterial })

  outage.registerIpc({ onRetry: (win, url) => navigate(win, url) })

  ipcMain.handle('server:probe', async (_event, rawUrl) => probe(rawUrl))

  ipcMain.handle('server:use', (_event, rawUrl) => {
    const before = store.get('serverUrl')
    const result = adoptServer(rawUrl)
    if (!result.ok) return result
    const { origin } = result
    // The menu bar page belongs to the server it was loaded from.
    if (origin !== before) glance.reload()
    const win = alive(mainWindow) ? mainWindow : createMainWindow(origin)
    // A link that arrived while the picker was up now has an origin to resolve
    // against, so it lands instead of the bare app root. Cleared either way:
    // one that no longer maps to this server must not outlive this attempt.
    const link = deferredLink
    deferredLink = null
    if (link && openDeepLink(win, link, origin)) {
      if (alive(setupWindow)) setupWindow.close()
      return result
    }
    navigate(win, origin)
    win.show()
    win.focus()
    if (alive(setupWindow)) setupWindow.close()
    return result
  })

  ipcMain.handle('server:current', () => store.get('serverUrl'))

  pdf.registerIpc()

  registerRemoteControlIpc(remoteControl)

  settings.registerIpc((event) => appSenderWindow(event))
  // The main window's page only: it is the one the commands are sent to.
  commands.registerIpc((event) => appSenderWindow(event, { accepts: (w) => w === mainWindow }))
  glance.registerIpc()
}

// ---------------------------------------------------------------------------
// Application menu
// ---------------------------------------------------------------------------

function installMenu() {
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
    remoteControl: remoteControl.menuView(),
    onPauseRemoteControl: remoteControl.pause,
    onResumeRemoteControl: remoteControl.resume,
    onCommand: (command) => commands.deliver({ command }),
    onGlance: () => glance.show(),
    menubarEnabled: settings.get().menubar.enabled,
    onToggleMenubar: (enabled) => settings.update({ menubar: { enabled } }),
  }))
  // A rebuilt menu starts with Back, Forward and every command disabled.
  syncNavMenu(BrowserWindow.getFocusedWindow())
  commands.sync()
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
  app.on('second-instance', (_event, argv) => {
    // A jump list task: the command, carried out in the main window.
    const command = commandFromArgv(argv)
    if (command) {
      commands.deliver({ command })
      return
    }
    // An open-at-login entry run while the app is already up has nothing to add.
    if (Array.isArray(argv) && argv.includes(HIDDEN_FLAG)) return
    const [win] = userWindows()
    if (!win) {
      // Running with no window of the user's (closed to the tray, or launched
      // at login): a second launch is the user asking for the app. A link
      // riding on it is deeplink's to open, in the window it belongs in.
      const scheme = `${config.scheme}:`
      const carriesLink = Array.isArray(argv) && argv.some((a) => String(a).toLowerCase().startsWith(scheme))
      if (!carriesLink) openInitialWindow()
      return
    }
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

    installMenu()
    remoteControl.watch()
    glance.installUserTasks()
    glance.watchPower()
    shortcut.apply(settings.get().shortcut)

    // Opened at login, the app comes up as its menu bar presence only. That
    // needs a presence to come up as: an app reached at least once, and the
    // Dock or else the icon, since without either the window is the only way in.
    const presence = traits.dock || settings.get().menubar.enabled
    if (loginItem.launchedAtLogin() && policy.menubarReachable() && presence) {
      deeplink.attach(onDeepLink)
    } else {
      openLaunchWindows()
    }
    // A jump list task run with the app closed starts it with the command in
    // its own argv, which no `second-instance` reports.
    const command = commandFromArgv(process.argv)
    if (command) commands.deliver({ command })
    glance.ensure()
    updater.init(() => mainWindow)

    // The user's windows, never all of them: the hidden menu bar window is
    // always there, and counting it left a Dock click with nothing to open.
    app.on('activate', () => {
      if (userWindows().length === 0) openInitialWindow()
    })
  })
}

lifecycle.install()
lifecycle.onWillQuit(() => {
  glance.dispose()
  shortcut.dispose()
  // Release the loopback port on the way out, and with it every flow still
  // armed on it. The port itself the OS reclaims either way; what this is for
  // is the flows, which would otherwise sit on timers in a process that is
  // leaving.
  oauth.stopCallbackServer()
  updater.stop()
  remoteControl.unwatch()
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
// system browser), and the launch and the bridge's openExternal, since which
// window a link lands in depends on what a launch opened, in which order, and
// which page sent the user out.
module.exports = { decide, adoptServer, openLaunchWindows, openExternallyFrom }

// Surfacing this as a dialog rather than a silent log: an unhandled rejection in
// the main process leaves the window alive but the shell half-dead, which is
// indistinguishable from a hung page.
process.on('unhandledRejection', (reason) => {
  console.error('[shell] unhandled rejection', reason)
  if (app.isReady()) {
    dialog.showErrorBox(app.getName(), `Something went wrong in the app shell:\n\n${reason}`)
  }
})
