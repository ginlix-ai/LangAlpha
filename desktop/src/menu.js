'use strict'

const { app, Menu } = require('electron')

/**
 * Every handler takes the window the menu event carries, never a module-level
 * "focused or main" lookup. Electron passes the window a menu item was invoked
 * against into `click`, and on a multi-window platform that is the only thing
 * guaranteed to be the one the user meant.
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
}) {
  const isMac = process.platform === 'darwin'

  // The two app-level commands, shared by the macOS app menu and the File menu
  // every other platform gets instead.
  const appItems = [
    { label: 'Check for Updates…', click: (_item, win) => onCheckForUpdates(win) },
    { type: 'separator' },
    ...(isSaas
      ? [{ label: 'Account…', click: onOpenAccount }]
      // The picker is the OSS edition's only settings surface, and a user who
      // moved their stack has no other way back to a working window.
      : [{ label: 'Change Server…', click: onChangeServer }]),
  ]

  const appMenu = {
    label: app.name,
    submenu: [
      { role: 'about' },
      ...appItems,
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
    submenu: [...appItems, { type: 'separator' }, { role: 'quit' }],
  }

  const viewMenu = {
    label: 'View',
    submenu: [
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
      { type: 'separator' },
      // A shipping consumer app should not carry DevTools in its primary menu,
      // but the shell loads a remote page and a support conversation sometimes
      // needs it, so it moves rather than disappears.
      { label: 'Developer', submenu: [{ role: 'toggleDevTools' }] },
    ],
  }

  // The window is frameless on macOS, so the only way back is whatever the page
  // chose to draw. These are the OS-level affordance that does not depend on it.
  // Enablement is kept current by the owner of the windows; see syncNavMenu.
  const historyMenu = {
    label: 'History',
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
      { type: 'separator' },
      { label: 'Home', accelerator: 'Shift+CmdOrCtrl+H', click: (_item, win) => win && onHome(win) },
      { type: 'separator' },
      { label: 'Open in Browser', click: (_item, win) => win && onOpenInBrowser(win) },
    ],
  }

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
    { role: 'editMenu' },
    viewMenu,
    historyMenu,
    { role: 'windowMenu' },
    helpMenu,
  ])
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

module.exports = { buildMenu, syncNavMenu }
