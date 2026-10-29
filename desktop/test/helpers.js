'use strict'

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { EventEmitter } = require('node:events')
const Module = require('node:module')

// The shell's modules require('electron'), which only resolves inside the
// Electron runtime. Swapping it at the loader lets the pure logic (origin
// policy, URL classification, deep-link mapping) be tested in plain node, where
// the cases that matter most, the ones that must be REFUSED, are reachable.
// Those are exactly the cases a live run cannot produce on demand.
const SRC = path.join(__dirname, '..', 'src')
const CONFIG_DIR = path.join(__dirname, '..', 'config')
const BUILD_CONFIG = path.join(CONFIG_DIR, 'build.json')

// The suite writes and deletes the real desktop/config/build.json, because that
// is the path config.js reads and the whole point is to exercise the real
// loader. It is also the file `scripts/write-build-config.mjs` leaves behind
// after packaging a saas build locally, so it is stashed here and put back by
// `cleanup` rather than lost to having run the tests.
// Every test file writes this one build.json, so the suite runs one file at a
// time (package.json's test script, which CI runs as `pnpm test`).
const STASHED_BUILD_CONFIG = fs.existsSync(BUILD_CONFIG) ? fs.readFileSync(BUILD_CONFIG) : null

const opened = []
const errorBoxes = []
const exits = []
const userData = fs.mkdtempSync(path.join(os.tmpdir(), 'la-test-'))

// Mutable so a test can put the machine offline, which is the one input to the
// outage page's copy that cannot be produced on demand any other way.
let online = true
const setOnline = (value) => { online = value }

// What the save dialog answers. Mutable for the same reason: a user who
// dismissed the dialog and a user who chose a path are two different endings of
// the PDF export, and neither is reachable without driving the dialog.
let saveDialog = { canceled: true }
const setSaveDialog = (value) => { saveDialog = value }

// What a message box answers, and every one that was asked. The remote-control
// confirmation is the user's to give, so agreeing and declining are both inputs
// a test has to be able to choose.
const messageBoxes = []
let messageBoxResponse = 0
const setMessageBox = (response) => { messageBoxResponse = response }

// The login item the remote-control helper registers as, in memory. What a
// register turns into is settable, because the answers that matter (macOS
// holding the item for approval, a register that does not take) are the
// machine's to give and none of them can be asked for from a real one here.
const loginItems = { status: 'not-registered', onRegister: 'enabled', calls: [] }
function resetLoginItems() {
  loginItems.status = 'not-registered'
  loginItems.onRegister = 'enabled'
  loginItems.calls.length = 0
}

// What `ipcMain.handle` was given, by channel, so a test can call a handler the
// way a renderer's invoke reaches it.
const ipcHandlers = new Map()

// What Electron falls back to with no `productName`: the package.json `name`.
// Nothing should ever run on it — main sets the edition's name before anything
// asks where userData lives — so it is deliberately neither edition's.
const FALLBACK_NAME = 'langalpha-desktop'
let appName = FALLBACK_NAME

// What the shell registered with `app.on`, so a test can deliver what the OS
// would: `open-url` on macOS, `second-instance` on Windows and Linux. Emptied by
// every `loadShell`, since each one re-requires main and registers afresh.
const appHandlers = new Map()
function emitApp(event, ...args) {
  for (const handler of appHandlers.get(event) || []) handler(...args)
}

// Every window the shell constructed since the last `loadShell`, in order. A
// window is only as much of Electron's as the shell's window code touches, and
// it moves by `loadURL` exactly as the real one does, so `loaded` is the
// history of where it was sent and the last entry is where it is.
const windows = []
const focusOrder = []

class FakeWebContents extends EventEmitter {
  constructor(window) {
    super()
    this.window = window
    this.navigationHistory = { canGoBack: () => false, canGoForward: () => false }
  }

  getURL() { return this.window.loaded.at(-1) || '' }
  setWindowOpenHandler() {}
  send() {}
  async executeJavaScript() { return null }
}

class FakeBrowserWindow extends EventEmitter {
  constructor(options = {}) {
    super()
    this.options = options
    this.loaded = []
    this.destroyed = false
    this.webContents = new FakeWebContents(this)
    windows.push(this)
  }

  static getAllWindows() { return windows.filter((w) => !w.destroyed) }
  static getFocusedWindow() { return null }
  // `fromWebContents` is how the PDF path finds the window it was asked from.
  // The stub reads a back-reference the caller hangs on its own fake contents,
  // rather than keeping a registry: the mapping is Electron's, not ours, and a
  // registry here would be a second implementation of it.
  static fromWebContents(wc) { return (wc && wc.window) || null }

  isDestroyed() { return this.destroyed }
  loadURL(url) { this.loaded.push(url); return Promise.resolve() }
  loadFile(file) { this.loaded.push(`file://${file}`); return Promise.resolve() }
  show() {}
  focus() { focusOrder.push(this) }
  isMinimized() { return false }
  restore() {}
  setBackgroundColor() {}
  close() {
    if (this.destroyed) return
    this.destroyed = true
    this.emit('closed')
  }
}

// What the remote-page preload handed to the page, keyed as the page sees it.
const exposed = {}

const electronStub = {
  net: {
    isOnline: () => online,
    // `probe()` reaches the network through Electron's session rather than
    // Node's, so the stub has to carry it. Refusing by default: no test in this
    // suite is entitled to make a real request, and a silent one would make the
    // outage tests depend on whether the machine running them is online.
    request: () => {
      const emitter = new EventEmitter()
      emitter.end = () => setImmediate(() => emitter.emit('error', new Error('net::ERR_CONNECTION_REFUSED')))
      emitter.abort = () => {}
      return emitter
    },
  },
  app: {
    getName: () => appName,
    setName: (value) => { appName = value },
    // Electron derives userData from `getName()`. Kept fixed here so the store
    // stays readable across a rename; that the derivation holds is proven
    // against a real packaged build, not against a stub that re-implements it.
    getPath: () => userData,
    getVersion: () => '0.0.0-test',
    isPackaged: false,
    setAsDefaultProtocolClient: () => true,
    on: (event, handler) => {
      if (!appHandlers.has(event)) appHandlers.set(event, [])
      appHandlers.get(event).push(handler)
    },
    requestSingleInstanceLock: () => true,
    getLoginItemSettings: () => ({ openAtLogin: loginItems.status === 'enabled', status: loginItems.status }),
    setLoginItemSettings: (settings) => {
      loginItems.calls.push(settings)
      loginItems.status = settings.openAtLogin ? loginItems.onRegister : 'not-registered'
    },
    exit: (code) => { exits.push(code) },
    // Never resolves, so requiring main.js registers its policy and stops there
    // rather than trying to open windows in a runtime that has none.
    whenReady: () => new Promise(() => {}),
  },
  shell: {
    openExternal: async (url) => { opened.push(url) },
  },
  dialog: {
    showErrorBox: (title, content) => { errorBoxes.push({ title, content }) },
    showMessageBox: async (...args) => {
      messageBoxes.push(args)
      return { response: messageBoxResponse }
    },
    showSaveDialog: async () => saveDialog,
  },
  BrowserWindow: FakeBrowserWindow,
  Menu: { buildFromTemplate: (t) => t, setApplicationMenu: () => {} },
  ipcMain: { on: () => {}, handle: (channel, handler) => { ipcHandlers.set(channel, handler) } },
  // The renderer half, for running the real preload rather than reading it.
  contextBridge: { exposeInMainWorld: (key, api) => { exposed[key] = api } },
  ipcRenderer: { invoke: async () => undefined, sendSync: () => undefined },
}

const load = Module._load
Module._load = function (request, ...rest) {
  if (request === 'electron') return electronStub
  return load.call(this, request, ...rest)
}

/**
 * Load the shell modules under a given edition. Config is read at require time,
 * so the cache has to be dropped between editions.
 */
/** Every shell loaded here, so `cleanup` can close what each one opened. */
const SHELLS = []

function loadShell({ edition = 'oss', appOrigin, platformOrigin, serverUrl = null, loginPath, settings } = {}) {
  if (edition === 'saas') {
    fs.writeFileSync(BUILD_CONFIG, JSON.stringify({
      edition: 'saas',
      appOrigin: appOrigin || 'https://app.example.com',
      platformOrigin: platformOrigin || 'https://platform.example.com',
      pairingOrigin: 'https://api.example.com',
      ...(loginPath ? { loginPath } : {}),
    }))
  } else {
    fs.rmSync(BUILD_CONFIG, { force: true })
  }

  for (const key of Object.keys(require.cache)) {
    if (key.startsWith(SRC)) delete require.cache[key]
  }
  appName = FALLBACK_NAME
  appHandlers.clear()
  windows.length = 0
  focusOrder.length = 0
  fs.rmSync(path.join(userData, 'settings.json'), { force: true })
  // Written before the store is required, so a test can hand it the contents of
  // a settings.json rather than a shape the store itself produced. That is the
  // only way to reach a hand-edited file, which is the input that matters here.
  if (settings) fs.writeFileSync(path.join(userData, 'settings.json'), JSON.stringify(settings))

  const store = require(path.join(SRC, 'store.js'))
  if (serverUrl) store.set('serverUrl', serverUrl)

  const shell = {
    store,
    config: require(path.join(SRC, 'config.js')),
    origins: require(path.join(SRC, 'origins.js')),
    policy: require(path.join(SRC, 'policy.js')),
    oauth: require(path.join(SRC, 'oauth.js')),
    deeplink: require(path.join(SRC, 'deeplink.js')),
    theme: require(path.join(SRC, 'theme.js')),
    outage: require(path.join(SRC, 'outage.js')),
    pdf: require(path.join(SRC, 'pdf.js')),
    captive: require(path.join(SRC, 'captive.js')),
    remoteControl: require(path.join(SRC, 'remoteControl.js')),
    main: require(path.join(SRC, 'main.js')),
  }
  // `oauth` is the one module here that can own an OS resource. A callback
  // listener outlives the suite that opened it -- and a suite need not have
  // asked for one, since a sign-in refused for want of a port starts one so the
  // next attempt is not refused for a condition that already cleared. Left open,
  // it holds the runner's event loop past the last test and the file is reported
  // cancelled rather than passed.
  SHELLS.push(shell.oauth)
  return shell
}

/** A saas install that has signed in before, so a launch opens the app. */
function signedIn({ consoleSharesMainWindow }) {
  const shell = loadShell({ edition: 'saas' })
  shell.store.set('reachedApp', true)
  shell.store.set('appChrome', true)
  shell.store.set('platformChrome', consoleSharesMainWindow)
  return shell
}

/** What macOS does with a link to the registered scheme: `open-url`, for the shell to claim. */
const openUrl = (url) => emitApp('open-url', { preventDefault() {} }, url)

/** The console's window of its own, as against the main window. */
const isAccount = (win) => win.options.title === 'Account'

/** What Chromium hands `will-navigate` for a page-initiated main-frame load. */
function navigateFrom(win, url) {
  const event = { isMainFrame: true, defaultPrevented: false, preventDefault() { this.defaultPrevented = true } }
  win.webContents.emit('will-navigate', event, url)
  return event
}

/** Run `fn` with console.warn captured, and answer with every line it wrote. */
function quietly(fn) {
  const warned = []
  const realWarn = console.warn
  console.warn = (...a) => warned.push(a.join(' '))
  try {
    fn()
  } finally {
    console.warn = realWarn
  }
  return warned
}

/**
 * Load the packaged entry rather than main, with a config written to fail
 * validation. The entry's whole job is what happens when a require throws, so it
 * is the one thing that cannot be exercised through `loadShell`.
 */
function loadEntryWith(buildConfig) {
  fs.writeFileSync(BUILD_CONFIG, JSON.stringify(buildConfig))
  for (const key of Object.keys(require.cache)) {
    if (key.startsWith(SRC)) delete require.cache[key]
  }
  errorBoxes.length = 0
  exits.length = 0
  require(path.join(SRC, 'index.js'))
  return { errorBoxes, exits }
}

/**
 * Run the real remote-page preload and answer with what it exposed. It reads
 * `location` to decide whether it is on the outage page, so a remote page is
 * stood in for while it runs.
 */
function loadPreload(argv = []) {
  const file = path.join(SRC, 'preload.js')
  delete require.cache[file]
  for (const key of Object.keys(exposed)) delete exposed[key]
  const had = Object.prototype.hasOwnProperty.call(globalThis, 'location')
  const saved = globalThis.location
  globalThis.location = { protocol: 'https:' }
  const savedArgv = process.argv
  process.argv = [...savedArgv, ...argv]
  try {
    require(file)
  } finally {
    process.argv = savedArgv
    if (had) globalThis.location = saved
    else delete globalThis.location
  }
  return { ...exposed }
}

/** Directories a test made and wants the shared teardown to take away again. */
const TEMP_DIRS = []

/** A temp directory this suite will not leave behind. */
function tempDir(prefix) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix))
  TEMP_DIRS.push(dir)
  return dir
}

function cleanup() {
  for (const oauth of SHELLS.splice(0)) oauth.stopCallbackServer()
  if (STASHED_BUILD_CONFIG) fs.writeFileSync(BUILD_CONFIG, STASHED_BUILD_CONFIG)
  else fs.rmSync(BUILD_CONFIG, { force: true })
  fs.rmSync(userData, { recursive: true, force: true })
  for (const dir of TEMP_DIRS) fs.rmSync(dir, { recursive: true, force: true })
}

module.exports = {
  loadShell, loadEntryWith, loadPreload, cleanup, opened, electronStub, setOnline, setSaveDialog, tempDir,
  emitApp, windows, focusOrder, signedIn, openUrl, isAccount, navigateFrom, quietly,
  loginItems, resetLoginItems, messageBoxes, setMessageBox, ipcHandlers,
}
