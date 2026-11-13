'use strict'

const { alive, gatedIpc } = require('./sender')

// Menu commands: what the shell's menus, the Dock menu, a jump list task and a
// notification click ask the main window's page to do. The page declares which
// it handles (`setMenuCommands`) and the menu enables exactly those, so a build
// that predates a command shows it greyed out rather than as a key that does
// nothing.

const COMMANDS = [
  'new-thread', 'new-workspace', 'toggle-sidebar', 'export-pdf', 'open-settings',
  'go-chief-of-staff', 'go-market', 'go-automations', 'go-orders', 'open',
]

// How long a command waits for a page that is still loading to declare it. Long
// enough for a cold start; short enough that a New Thread pressed on a page that
// will never declare it does not fire minutes later.
const PENDING_MS = 20_000

/** The known commands in `raw`, deduplicated. Anything else the page lists is ignored. */
function sanitize(raw) {
  if (!Array.isArray(raw)) return new Set()
  return new Set(raw.filter((c) => typeof c === 'string' && COMMANDS.includes(c)))
}

/**
 * Whether a menu item for `command` is enabled. With no main window every
 * command is: choosing one opens the window and runs once its page declares it.
 * With one, only what its page declared.
 */
function isEnabled(command, { hasMainWindow, declared }) {
  if (!hasMainWindow) return true
  return declared.has(command)
}

/** A command a second launch carries (`--command=new-thread`), or null. `open` takes a path, so it never comes this way. */
function commandFromArgv(argv) {
  if (!Array.isArray(argv)) return null
  for (const arg of argv) {
    const m = /^--command=([a-z-]+)$/.exec(String(arg))
    if (m && m[1] !== 'open' && COMMANDS.includes(m[1])) return m[1]
  }
  return null
}

/**
 * The live half. `showMain(url?)` raises the main window, creating it (at `url`
 * when given) if there is none, and answers it; `appUrl(path)` resolves a path
 * on the app origin or answers null; `syncMenu(isEnabled)` pushes enablement
 * into the application menu (menu.js's `syncCommandMenu`).
 *
 * Only the main window's page declares, so what it declared is one set, emptied
 * whenever that page or that window is replaced.
 */
function createCommands({ getMainWindow, showMain, loadInMain, appUrl, syncMenu, now = Date.now }) {
  const declared = new Set()
  let pending = null

  function send(win, command) {
    win.webContents.send('shell:menu-command', command)
  }

  /** Enable each menu item by what the main window's page declared. */
  function sync() {
    const hasMainWindow = alive(getMainWindow())
    syncMenu((name) => isEnabled(name, { hasMainWindow, declared }))
  }

  function reset() {
    declared.clear()
    sync()
  }

  /** The main window's page says what it handles. A waiting command it now handles goes. */
  function declare(raw) {
    declared.clear()
    for (const c of sanitize(raw)) declared.add(c)
    const win = getMainWindow()
    if (pending && alive(win)) {
      const fresh = now() - pending.at < PENDING_MS
      if (fresh && declared.has(pending.command.command)) send(win, pending.command)
      if (!fresh || declared.has(pending.command.command)) pending = null
    }
    sync()
  }

  /**
   * A new main window has declared nothing, and neither has a new document in
   * it; the page declares again as it boots.
   */
  function attachMainWindow(win) {
    win.webContents.on('did-navigate', reset)
    win.on('closed', reset)
    reset()
  }

  /**
   * Carry out `{ command, path? }`. `open` falls back to loading the path in the
   * main window when its page does not take it; every other command raises the
   * window and waits for a page that will.
   */
  function deliver(command) {
    if (!command || !COMMANDS.includes(command.command)) return
    if (command.command === 'open') {
      const url = appUrl(command.path)
      if (!url) return
      const win = getMainWindow()
      if (!alive(win)) {
        showMain(url)
        return
      }
      showMain()
      if (declared.has('open')) send(win, { command: 'open', path: command.path })
      else loadInMain(url)
      return
    }
    const win = showMain()
    if (alive(win) && declared.has(command.command)) {
      send(win, { command: command.command })
      return
    }
    pending = { command: { command: command.command }, at: now() }
  }

  /** `admit(event)` answers the window asking if it is the main window showing the app. */
  function registerIpc(admit) {
    gatedIpc(admit).on('shell:set-menu-commands', declare)
  }

  return { declare, attachMainWindow, deliver, sync, registerIpc }
}

module.exports = { COMMANDS, sanitize, isEnabled, commandFromArgv, createCommands, PENDING_MS }
