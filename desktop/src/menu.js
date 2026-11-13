'use strict'

const { app, Menu } = require('electron')

const { COMMANDS } = require('./commands')

/**
 * Every handler takes the window the menu event carries, never a module-level
 * "focused or main" lookup. Electron passes the window a menu item was invoked
 * against into `click`, and on a multi-window platform that is the only thing
 * guaranteed to be the one the user meant.
 *
 * The exception is a command (`onCommand`): those are addressed to the main
 * window's page whichever window has focus, since the main window is the only
 * one that runs the app, and they are enabled by what that page declared (see
 * `syncCommandMenu`).
 */
function buildMenu({
  isSaas,
  onChangeServer,
  onOpenAccount,
  onCheckForUpdates,
  onReload,
  onBack,
  onForward,
  onHome,
  onOpenInBrowser,
  remoteControl = null,
  onPauseRemoteControl,
  onResumeRemoteControl,
  onCommand = () => {},
  onGlance = () => {},
  menubarEnabled = true,
  onToggleMenubar = () => {},
}) {
  const isMac = process.platform === 'darwin'

  // A command the main window's page carries out. Disabled until the page says
  // it handles it, so a build that does not shows it greyed rather than as a key
  // that does nothing.
  const command = (label, name, accelerator) => ({
    id: `cmd:${name}`,
    label,
    ...(accelerator ? { accelerator } : {}),
    enabled: false,
    click: () => onCommand(name),
  })

  // The kill switch for remote control, present only while this Mac is paired
  // (`remoteControl` is `menuView` from remoteControl.js). It lives here rather
  // than only in the app because the helper outlives the app's windows, and a
  // signed-out or unreachable app must still leave a way to stop it. The status
  // line is a disabled item, so it is rebuilt rather than relabelled: Electron
  // does not carry a label change to a menu macOS already shows.
  const remoteItems = remoteControl
    ? [
      {
        label: 'Remote Control',
        submenu: [
          { label: remoteControl.line, enabled: false },
          { type: 'separator' },
          remoteControl.paused
            ? { label: 'Resume Remote Control', click: () => onResumeRemoteControl() }
            : { label: 'Pause Remote Control', click: () => onPauseRemoteControl() },
        ],
      },
    ]
    : []

  const settingsItem = command('Settings…', 'open-settings', 'CmdOrCtrl+,')
  const updatesItem = { label: 'Check for Updates…', click: (_item, win) => onCheckForUpdates(win) }
  // The picker is the OSS edition's only server setting, and a user who moved
  // their stack has no other way back to a working window.
  const accountItem = isSaas
    ? { label: 'Account…', click: onOpenAccount }
    : { label: 'Change Server…', click: onChangeServer }

  const appMenu = {
    label: app.name,
    submenu: [
      { role: 'about' },
      { type: 'separator' },
      settingsItem,
      updatesItem,
      { type: 'separator' },
      accountItem,
      ...remoteItems,
      { type: 'separator' },
      { role: 'services' },
      { type: 'separator' },
      { role: 'hide' },
      { role: 'hideOthers' },
      { role: 'unhide' },
      { type: 'separator' },
      { role: 'quit' },
    ],
  }

  const fileMenu = {
    label: 'File',
    submenu: [
      command('New Thread', 'new-thread', 'CmdOrCtrl+N'),
      command('New Workspace…', 'new-workspace', 'Shift+CmdOrCtrl+N'),
      { type: 'separator' },
      { role: 'close', label: 'Close Window' },
      // Everything the macOS app menu carries, on the platforms that have none.
      ...(isMac ? [] : [
        { type: 'separator' },
        settingsItem,
        updatesItem,
        accountItem,
        { type: 'separator' },
        { role: 'quit' },
      ]),
    ],
  }

  const glanceItem = { label: 'Market Glance', click: () => onGlance() }

  const viewMenu = {
    label: 'View',
    submenu: [
      command('Toggle Sidebar', 'toggle-sidebar', isMac ? 'Ctrl+Cmd+S' : 'Ctrl+Shift+S'),
      { type: 'separator' },
      // Not `role: 'reload'`: on the outage page that reloads the local error
      // file, which is the one moment someone actually reaches for ⌘R.
      { label: 'Reload', accelerator: 'CmdOrCtrl+R', click: (_item, win) => win && onReload(win) },
      { role: 'forceReload' },
      { type: 'separator' },
      { role: 'resetZoom' },
      { role: 'zoomIn' },
      { role: 'zoomOut' },
      { type: 'separator' },
      { role: 'togglefullscreen' },
      // macOS keeps it in the Window menu, beside the menu bar switch.
      ...(isMac ? [] : [{ type: 'separator' }, glanceItem]),
      { type: 'separator' },
      // A shipping consumer app should not carry DevTools in its primary menu,
      // but the shell loads a remote page and a support conversation sometimes
      // needs it, so it moves rather than disappears.
      { label: 'Developer', submenu: [{ role: 'toggleDevTools' }] },
    ],
  }

  // The window is frameless on macOS, so the only way back is whatever the page
  // chose to draw. Back and Forward are the OS-level affordance that does not
  // depend on it; their enablement is kept current by the owner of the windows
  // (see syncNavMenu). The places below them are the app's own sections.
  const goMenu = {
    label: 'Go',
    submenu: [
      {
        id: 'nav-back',
        label: 'Back',
        accelerator: isMac ? 'Cmd+[' : 'Alt+Left',
        enabled: false,
        click: (_item, win) => win && onBack(win),
      },
      {
        id: 'nav-forward',
        label: 'Forward',
        accelerator: isMac ? 'Cmd+]' : 'Alt+Right',
        enabled: false,
        click: (_item, win) => win && onForward(win),
      },
      { label: 'Home', accelerator: 'Shift+CmdOrCtrl+H', click: (_item, win) => win && onHome(win) },
      { type: 'separator' },
      command('Chief of Staff', 'go-chief-of-staff', 'CmdOrCtrl+1'),
      command('Market', 'go-market', 'CmdOrCtrl+2'),
      command('Automations', 'go-automations', 'CmdOrCtrl+3'),
      command('Orders', 'go-orders', 'CmdOrCtrl+4'),
      { type: 'separator' },
      { label: 'Open in Browser', click: (_item, win) => win && onOpenInBrowser(win) },
    ],
  }

  // `role: 'window'` with a submenu of our own: macOS still treats it as the
  // Window menu and appends the open windows to it.
  const windowMenu = isMac
    ? {
      label: 'Window',
      role: 'window',
      submenu: [
        { role: 'minimize' },
        { role: 'zoom' },
        { type: 'separator' },
        glanceItem,
        {
          id: 'menubar-enabled',
          label: 'Show in Menu Bar',
          type: 'checkbox',
          checked: menubarEnabled,
          click: (item) => onToggleMenubar(item.checked),
        },
        { type: 'separator' },
        { role: 'front' },
      ],
    }
    : { role: 'windowMenu' }

  // `role: 'help'` is what macOS hangs its menu search field off, which is where
  // a user goes when they cannot find a command. It shipped as a copy of the app
  // menu on the platforms that had one at all, so Help listed Quit.
  const helpMenu = {
    role: 'help',
    submenu: [
      { label: 'Check for Updates…', click: (_item, win) => onCheckForUpdates(win) },
      ...(isMac ? [] : [{ type: 'separator' }, { role: 'about' }]),
    ],
  }

  return Menu.buildFromTemplate([
    isMac ? appMenu : fileMenu,
    ...(isMac ? [fileMenu] : []),
    { role: 'editMenu' },
    viewMenu,
    goMenu,
    windowMenu,
    helpMenu,
  ])
}

/**
 * Enable each command item by `isEnabled(name)`. Pushed in, like Back and
 * Forward, because Electron menus are built once; the `cmd:` ids are the
 * contract between here and the template above.
 */
function syncCommandMenu(isEnabled) {
  const menu = Menu.getApplicationMenu()
  if (!menu) return
  for (const name of COMMANDS) {
    const item = menu.getMenuItemById(`cmd:${name}`)
    if (item) item.enabled = isEnabled(name)
  }
}

/** Tick or clear Window › Show in Menu Bar when the setting moved somewhere else. */
function syncMenubarItem(enabled) {
  const item = Menu.getApplicationMenu()?.getMenuItemById('menubar-enabled')
  if (item) item.checked = enabled
}

/**
 * Point Back/Forward at whatever `win` can currently do. Electron menus are
 * built once, so enablement is pushed in rather than read out, and the ids are
 * the contract between here and the template above.
 */
function syncNavMenu(win) {
  const menu = Menu.getApplicationMenu()
  if (!menu) return
  const back = menu.getMenuItemById('nav-back')
  const forward = menu.getMenuItemById('nav-forward')
  if (!back || !forward) return

  const history = win && !win.isDestroyed() ? win.webContents.navigationHistory : null
  back.enabled = !!history && history.canGoBack()
  forward.enabled = !!history && history.canGoForward()
}

module.exports = { buildMenu, syncNavMenu, syncCommandMenu, syncMenubarItem }
