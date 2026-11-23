'use strict'

const path = require('node:path')
const { app, BrowserWindow, clipboard, shell } = require('electron')

const backend = require('./backend')
const origins = require('./origins')
const store = require('./store')
const theme = require('./theme')
const { probeWebApp } = require('./probe')
const { alive, gatedIpc } = require('./sender')

const SETUP_DIR = path.join(__dirname, '..', 'setup')

/**
 * The OSS first-run page: the choice, the server form, the stack's progress and
 * its failure. Its own window with its own preload, because `server:use` repoints
 * the whole app and is not something to hand to whatever a remote page becomes.
 *
 * `stack` is the build's bundled stack, or null. `host` is what main owns:
 * `mainWindow()`, `enterApp(origin)` (the main window on an origin just chosen,
 * this window closed behind it), `onBackendUp(result)`, `afterUserWindowClosed()`,
 * `isExiting()` and `installMenu()`.
 */
function createSetupWindow({ stack, host }) {
  let win = null

  function show() {
    if (alive(win)) {
      win.focus()
      return win
    }
    win = new BrowserWindow({
      width: 520,
      // The choice and the failure screen, with its log tail, need the room. A build
      // with no bundled stack shows the connect form alone, the size it always was.
      height: stack ? 640 : 460,
      resizable: false,
      title: stack ? 'LangAlpha' : 'Connect to your server',
      backgroundColor: theme.backgroundFor(store.get('theme')),
      webPreferences: {
        contextIsolation: true,
        nodeIntegration: false,
        preload: path.join(SETUP_DIR, 'setup-preload.js'),
      },
    })
    // The page has no links, and anything that replaced it (a URL dropped on the
    // window) would inherit its channels, which repoint the whole app.
    win.webContents.on('will-navigate', (event) => event.preventDefault())
    win.webContents.setWindowOpenHandler(() => ({ action: 'deny' }))
    win.on('closed', onClosed)
    win.loadFile(path.join(SETUP_DIR, 'setup.html'))
    return win
  }

  function onClosed() {
    win = null
    if (alive(host.mainWindow())) return
    // Closed while the stack is still starting: the app window opens when it is up.
    if (backend.isStarting()) return
    // Nothing configured, or a stack that failed to start, leaves no way back in,
    // so the app has nothing left to do.
    if (!backend.isUp()) {
      app.quit()
      return
    }
    // With a backend up, this is one of the user's windows like any other, and
    // closing it can be closing the last.
    host.afterUserWindowClosed()
  }

  function close() {
    if (alive(win)) win.close()
  }

  /**
   * Run the stack and open the app on it, with this window showing the progress,
   * and on a failure what failed and how to retry or choose a server instead.
   * Started at login (`windowed: false`) it opens no window unless it fails.
   */
  async function runBundled({ windowed = true } = {}) {
    if (windowed) show()
    const starting = backend.useBundled()
    host.installMenu()
    const result = await starting
    // A failure is shown by `onStackChange`, which also covers a window closed
    // while it was starting.
    if (!result.ok) return result
    host.onBackendUp(result)
    if (!windowed) return result
    // Asked for again with the app already on it: a live window is not reloaded
    // for that, since a reload would cost a streaming turn.
    const main = host.mainWindow()
    if (alive(main) && origins.isApp(main.webContents.getURL())) {
      close()
      return result
    }
    host.enterApp(result.origin)
    return result
  }

  /**
   * The stack has stopped restarting a child, or stopped being restartable. The app
   * window keeps what it was showing, since a streaming turn's text is still worth
   * reading, and this window says what is wrong.
   */
  function onStackChange(view) {
    if (alive(win)) win.webContents.send('stack:changed', view)
    // The stack stopping as the app quits can read as a failure, and nothing may open then.
    if (view.state === 'failed' && backend.wantsStack() && !host.isExiting()) show()
  }

  function useServer(rawUrl) {
    const result = backend.useServer(rawUrl)
    if (!result.ok) return result
    host.installMenu()
    host.onBackendUp(result)
    host.enterApp(result.origin)
    return result
  }

  function helpPrompt() {
    // Required here, not at the top: the hosted package leaves out src/stack/.
    const { helpPrompt: build } = require('./stack/helpPrompt')
    return build({
      failure: stack.view().failure,
      logsDir: stack.logsDir,
      dataDir: stack.dataDir,
      source: stack.source(),
      appVersion: app.getVersion(),
      system: {
        platform: process.platform,
        version: typeof process.getSystemVersion === 'function' ? process.getSystemVersion() : '',
        arch: process.arch,
      },
    })
  }

  /** The page's channels, answered for this window only, and the stack's changes sent to it. */
  function install() {
    // The remote preload has no route to these, but that is a property of another
    // file, and the check costs a line.
    const ipc = gatedIpc((event) => alive(win) && event.sender === win.webContents)
    const withStack = (handler) => (...args) => (stack ? handler(...args) : false)

    ipc.handle('server:probe', (rawUrl) => probeWebApp(rawUrl))
    ipc.handle('server:use', useServer, { ok: false })
    // The user's own server, to offer back in the form. Never the stack's origin,
    // which answers only while this install runs it.
    ipc.handle('server:current', () => backend.serverAddress())
    ipc.handle('stack:status', () => ({ available: Boolean(stack), view: stack ? stack.view() : null }))
    ipc.handle('stack:use', () => (stack ? runBundled() : { ok: false }), { ok: false })
    ipc.handle('stack:reveal-logs', withStack(() => {
      shell.openPath(stack.logsDir)
      return true
    }), false)
    // Built here, not on the page, because the paths, the version and the source
    // commit are the main process's to know.
    ipc.handle('stack:copy-help-prompt', withStack(() => {
      clipboard.writeText(helpPrompt())
      return true
    }), false)

    if (stack) stack.on('change', onStackChange)
  }

  return { show, close, window: () => win, runBundled, install }
}

module.exports = { createSetupWindow }
