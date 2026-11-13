'use strict'

const path = require('node:path')
const { app, powerMonitor } = require('electron')

const config = require('./config')
const origins = require('./origins')
const policy = require('./policy')
const theme = require('./theme')
const { createMenubar } = require('./menubar')
const { createTray } = require('./tray')
const { trayMenuTemplate } = require('./trayMenu')
const { createNotifier, isWatching } = require('./notify')
const { createAttention } = require('./attention')
const { alive, appSenderWindow, gatedIpc } = require('./sender')

// The menu bar feature: the hidden menu bar window (the panel, or Market Glance
// as a window), the icon and the lines beside it, the notifications the window's
// page posts, and the badge and Dock menu it reports. The window is the app's
// `/desktop/menubar` page, kept for as long as the app runs once it has an app
// to load; the page asks, and this filters, draws and routes.

/**
 * `host` is what main owns: `mainWindow()`, `showMain(url?)`,
 * `openInitialWindow()`, `route(url)` (a navigation out of the menu bar page)
 * and `recordFacts(win)`. `commands` is commands.js's, `settings` the settings
 * service, `lifecycle` lifecycle.js's.
 */
function createGlance({ traits, settings, commands, lifecycle, store, host, timers }) {
  const tray = createTray({
    traits,
    timers,
    menubar: () => settings.get().menubar,
    onClick: () => toggle(),
    menuTemplate: () => trayMenuTemplate(settings.get(), {
      traits,
      appName: config.appName,
      actions: {
        showGlance: () => show(),
        showMain: () => host.showMain(),
        edit: (patchFor) => settings.update(patchFor(settings.get())),
        command: (name) => commands.deliver({ command: name }),
        quit: () => app.quit(),
      },
    }),
    // Sent whether or not the panel is open: the lines beside the icon are on
    // screen either way.
    onRefresh: () => menubar.window()?.webContents.send('shell:tray-refresh'),
  })

  const menubar = createMenubar({
    traits,
    preload: path.join(__dirname, 'preload.js'),
    recordFacts: host.recordFacts,
    route: host.route,
    backgroundColor: () => theme.backgroundFor(store.get('theme')),
    anchor: tray.bounds,
    isQuitting: lifecycle.isQuitting,
    // With the icon on it is still a way in, so closing Market Glance last
    // (Linux, after a launch at login) leaves the app running behind it.
    onUserClose: () => { if (!settings.get().menubar.enabled) lifecycle.afterUserWindowClosed() },
    onUnproven: () => host.showMain(),
    onPageStart: () => notifier.clearStarted(),
  })

  /**
   * An app path opened from outside the main window: a notification, the Dock
   * menu, the panel. An empty path is the panel's "open the app": the main
   * window raised as it is, or opened at the entry, and never navigated.
   */
  function openAppPath(appPath) {
    menubar.hide()
    if (appPath === '') {
      host.showMain()
      return
    }
    commands.deliver({ command: 'open', path: appPath })
  }

  const notifier = createNotifier({
    traits,
    onOpen: openAppPath,
    onMarkRead: (action) => menubar.window()?.webContents.send('shell:notification-action', action),
  })

  const attention = createAttention({
    traits,
    getMainWindow: host.mainWindow,
    onNewThread: () => commands.deliver({ command: 'new-thread' }),
    onGlance: () => show(),
    onOpen: openAppPath,
  })

  /** The icon follows the setting; the window does not. */
  function setTrayEnabled(enabled) {
    if (enabled && menubar.window()) tray.show()
    else if (!enabled) tray.hide()
  }

  settings.onChange((next, prev) => {
    if (prev.menubar.enabled !== next.menubar.enabled) setTrayEnabled(next.menubar.enabled)
    tray.update()
  })

  /** The menu bar window and, with the setting on, its icon. Only once there is an app to load. */
  function ensure() {
    if (!policy.menubarReachable()) return
    if (!menubar.ensure()) return
    setTrayEnabled(settings.get().menubar.enabled)
  }

  /** Market Glance from a menu: the panel, or its window. Before the app is reached, the app. */
  function show() {
    if (!policy.menubarReachable()) {
      host.openInitialWindow()
      return
    }
    ensure()
    menubar.show()
  }

  function toggle() {
    if (!policy.menubarReachable()) return
    ensure()
    menubar.toggle()
  }

  /** The main window reached the app: the menu bar can exist, and a load of it that failed can try again. */
  function onMainLoadedApp(url, status) {
    if (!origins.isApp(url) || !(status >= 200 && status < 400)) return
    ensure()
    menubar.reloadIfFailed()
  }

  /**
   * Whether closing the last window leaves the app in the notification area:
   * with the icon there and the user wanting it. The first time, it says so.
   */
  function keepRunningInTray() {
    const s = settings.get()
    if (!menubar.window() || !s.menubar.enabled || !s.keepRunningInTray) return false
    if (!store.get('trayNoticeShown')) {
      store.set('trayNoticeShown', true)
      notifier.notice({
        title: `${config.appName} is still running in the tray`,
        body: 'Open it from the icon in the notification area, or quit from that icon\'s menu.',
        onClick: () => host.showMain(),
      })
    }
    return true
  }

  /** Whether the user is looking at thread `id`: the focused main window shows it. */
  function watching(id) {
    const main = host.mainWindow()
    return alive(main) && isWatching({ focused: main.isFocused(), url: main.webContents.getURL(), id })
  }

  /** The menu bar page's methods, which answer that page and nobody else. */
  /** A machine back from sleep or a locked screen gets a fresh menu bar page. */
  function watchPower() {
    const fresh = () => menubar.reloadIfHidden()
    powerMonitor.on('resume', fresh)
    powerMonitor.on('unlock-screen', fresh)
  }

  function registerIpc() {
    const ipc = gatedIpc((event) => {
      const admitted = Boolean(appSenderWindow(event, { accepts: menubar.isMenubar }))
      if (admitted) menubar.markProven()
      return admitted
    })
    ipc.handle('shell:notify', (n) => notifier.notify(n, {
      settings: settings.get(),
      watching: watching(typeof n?.id === 'string' ? n.id : ''),
    }), { shown: false, reason: 'unsupported' })
    ipc.on('shell:clear-notifications', (ids) => notifier.clear(ids))
    ipc.on('shell:set-attention', (value) => attention.set(value))
    ipc.on('shell:set-tray-items', (items, options) => tray.setItems(items, options))
    ipc.on('shell:open-in-main', (appPath) => openAppPath(appPath))
    ipc.on('shell:hide-menubar', () => menubar.hide())
  }

  return {
    ensure,
    show,
    toggle,
    onMainLoadedApp,
    reload: menubar.reload,
    registerIpc,
    watchPower,
    isMaterial: (w) => menubar.isMaterial(w),
    keepRunningInTray,
    isOpenAsWindow: menubar.isOpenAsWindow,
    applyOverlay: attention.applyOverlay,
    installUserTasks: attention.installUserTasks,
    // The quit is certain: the icon goes, and its rotation and refresh with it.
    // Not before: a quit a page can still cancel must not leave a running app
    // without it.
    dispose: tray.hide,
  }
}

module.exports = { createGlance }
