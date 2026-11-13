'use strict'

// The menu bar feature as the shell composes it (glance.js, menubar.js, main):
// who may use the menu bar page's methods and the desktop settings, the menu bar
// window's own rules, and what the bridge hands a page. What is worth pinning is
// what a desk cannot produce on demand: a page that is not the menu bar asking
// to post, a page a step behind the settings, a document running as no origin.

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')

const {
  loadShell, loadPreload, cleanup, electronStub, windows, ipcHandlers, ipcListeners, emitApp, trays, display,
  from, logged, readyShell, menubarWindow, fakeTimers, navigateFrom,
} = require('./helpers')

const geometry = require('../src/panelGeometry.js')
const desktopSettings = require('../src/desktopSettings.js')
const { traits } = require('../src/platformTraits.js')

after(cleanup)

const SIGNED_IN = { reachedApp: true, appChrome: true, platformChrome: true }
const BAR_URL = 'https://app.example.com/desktop/menubar'

describe('the panel material and the native theme', () => {
  test("follows the panel page's choice, auto staying with the OS; no other window moves it", async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN, platform: 'darwin' })
    try {
      const [app] = windows
      const bar = menubarWindow()
      const setTheme = ipcListeners.get('shell:set-theme')
      const report = (win, url, value, preference) => setTheme({ ...from(win, url), returnValue: undefined }, value, preference)
      electronStub.nativeTheme.themeSource = 'system'

      // The resolved theme is not the choice: auto on a dark OS resolves dark,
      // and pinning that would take the page's own auto mode off the OS.
      report(bar, BAR_URL, 'dark', 'auto')
      assert.equal(electronStub.nativeTheme.themeSource, 'system')
      report(bar, BAR_URL, 'light', 'light')
      assert.equal(electronStub.nativeTheme.themeSource, 'light')
      // A web build too old to send its choice leaves the OS in charge.
      report(bar, BAR_URL, 'dark', undefined)
      assert.equal(electronStub.nativeTheme.themeSource, 'system')
      // The main window and the console resolve their own; only the panel's material follows its page.
      report(app, 'https://app.example.com/chat', 'dark', 'dark')
      assert.equal(electronStub.nativeTheme.themeSource, 'system')
    } finally {
      emitApp('will-quit')
    }
  })
})

describe('who may use the desktop bridge', () => {
  test('only the menu bar window posts; the main window is told unsupported, a stranger nothing', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN })
    try {
      const [app] = windows
      const bar = menubarWindow()
      const notifyHandler = ipcHandlers.get('shell:notify')
      const n = { id: 't-1', kind: 'turn', title: 'x', body: 'y', path: '/chat/t/t-1' }

      assert.deepEqual(notifyHandler(from(app, 'https://app.example.com/chat'), n), { shown: false, reason: 'unsupported' })
      const stranger = new electronStub.BrowserWindow()
      assert.deepEqual(
        notifyHandler(from(stranger, 'https://evil.example.com/'), n),
        { shown: false, reason: 'unsupported' },
      )
      // A document at the menu bar's address running as no origin is not the menu bar's page.
      assert.deepEqual(notifyHandler(from(bar, BAR_URL, 'null'), n), { shown: false, reason: 'unsupported' })
    } finally {
      emitApp('will-quit')
    }
  })

  test('settings answer the app\'s pages and nobody else, and a change reaches every app window', async () => {
    const { store } = await readyShell({ edition: 'saas', settings: SIGNED_IN })
    try {
      const [app] = windows
      const bar = menubarWindow()
      const heard = []
      for (const w of [app, bar]) w.webContents.send = (channel, payload) => heard.push([w, channel, payload])

      const get = ipcHandlers.get('shell:get-desktop-settings')
      const set = ipcHandlers.get('shell:set-desktop-settings')
      assert.equal(get(from(new electronStub.BrowserWindow(), 'https://platform.example.com/account')), null)
      assert.equal(set(from(new electronStub.BrowserWindow(), 'https://evil.example.com/'), { openAtLogin: true }), null)

      const view = get(from(app, 'https://app.example.com/settings'))
      assert.equal(view.menubar.enabled, true)
      assert.ok(view.support)

      const next = set(from(app, 'https://app.example.com/settings'), {
        notifications: { started: false }, menubar: { items: [{ kind: 'portfolio' }] },
      })
      assert.equal(next.notifications.started, false)
      assert.deepEqual(next.menubar.items, [{ kind: 'portfolio' }])
      assert.equal(store.get('desktop').notifications.started, false, 'not persisted')
      from(bar, BAR_URL)
      set(from(app, 'https://app.example.com/settings'), { menubar: { items: [{ kind: 'stock', symbol: 'qqq' }] } })
      const broadcasts = heard.filter(([, channel]) => channel === 'shell:desktop-settings')
      assert.ok(broadcasts.some(([w]) => w === bar), 'the menu bar page never heard')
      assert.ok(broadcasts.some(([w]) => w === app), 'the main window never heard')
      assert.deepEqual(broadcasts.at(-1)[2].menubar.items, [{ symbol: 'QQQ', kind: 'stock' }])
    } finally {
      emitApp('will-quit')
    }
  })

  test('menu commands are declared by the main window only, and enable its menu items', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN })
    try {
      const [app] = windows
      const bar = menubarWindow()
      const item = (name) => electronStub.Menu.getApplicationMenu().getMenuItemById(`cmd:${name}`)
      const declare = ipcListeners.get('shell:set-menu-commands')
      assert.equal(item('new-thread').enabled, false)
      declare(from(bar, BAR_URL), ['new-thread'])
      assert.equal(item('new-thread').enabled, false, 'the menu bar window declared for the main one')
      declare(from(app, 'https://app.example.com/chat'), ['new-thread', 'bogus'])
      assert.equal(item('new-thread').enabled, true)
      assert.equal(item('go-orders').enabled, false)
      // A new document has declared nothing.
      app.webContents.emit('did-navigate', {}, 'https://app.example.com/', 200)
      assert.equal(item('new-thread').enabled, false)
    } finally {
      emitApp('will-quit')
    }
  })

  test('a jump list task that starts the app runs its command once the page declares it', async () => {
    process.argv.push('--command=new-thread')
    try {
      await readyShell({ edition: 'saas', settings: SIGNED_IN, platform: 'win32' })
    } finally {
      process.argv.pop()
    }
    try {
      const mains = windows.filter((w) => w !== menubarWindow())
      assert.equal(mains.length, 1, 'the command opened a second window')
      const [app] = mains
      const sent = []
      app.webContents.send = (channel, payload) => sent.push([channel, payload])
      ipcListeners.get('shell:set-menu-commands')(from(app, 'https://app.example.com/chat'), ['new-thread'])
      assert.deepEqual(sent, [['shell:menu-command', { command: 'new-thread' }]])
    } finally {
      emitApp('will-quit')
    }
  })

  test('the menu bar window is told it is one, and keeps its page', async () => {
    const { pageFacts } = await readyShell({ edition: 'saas', settings: SIGNED_IN, platform: 'darwin' })
    try {
      const [app] = windows
      const bar = menubarWindow()
      assert.equal(pageFacts.windowFactsOf(app.webContents).windowRole, 'main')
      assert.equal(pageFacts.windowFactsOf(bar.webContents).windowRole, 'menubar')
      assert.equal(pageFacts.windowFactsOf(bar.webContents).windowChrome, 'native')
      assert.equal(bar.loaded.at(-1), BAR_URL)
      // A click on the icon makes sure of the window before opening it.
      trays.at(-1).emit('click')
      assert.equal(menubarWindow(), bar, 'a second window was made')
      assert.equal(windows.filter((w) => w.loaded.at(-1) === BAR_URL).length, 1)

      // Closing it hides it: it is where notifications come from.
      let prevented = false
      bar.emit('close', { preventDefault() { prevented = true } })
      assert.equal(prevented, true)

      // A link out of it is the main window's to open, never a navigation of the panel.
      assert.equal(navigateFrom(bar, 'https://app.example.com/chat/t/abc').defaultPrevented, true)
      assert.equal(navigateFrom(bar, `${BAR_URL}?x=1`).defaultPrevented, false)
    } finally {
      emitApp('will-quit')
    }
  })
})

describe('the bridge', () => {
  test('the desktop features take no capability word: each has a method to look for', () => {
    const both = loadPreload({ consoleReturns: true }).langalphaDesktop
    assert.deepEqual([...both.capabilities], ['integration-login', 'integration-install', 'console-return'])
    const plain = loadPreload({}).langalphaDesktop
    assert.deepEqual([...plain.capabilities], [])
    for (const method of ['notify', 'setTrayItems', 'setMenuCommands', 'getDesktopSettings']) {
      assert.equal(typeof plain[method], 'function', method)
    }
  })

  test('the window role is one of three or absent', () => {
    assert.equal(loadPreload({ windowRole: 'menubar' }).langalphaDesktop.windowRole, 'menubar')
    assert.equal(loadPreload({ windowRole: 'main' }).langalphaDesktop.windowRole, 'main')
    assert.equal(loadPreload({ windowRole: 'admin' }).langalphaDesktop.windowRole, undefined)
    assert.equal(loadPreload(null).langalphaDesktop.windowRole, undefined)
  })

  test('a listener gets the payload, never the event, and can stop', () => {
    const listeners = new Map()
    const realOn = electronStub.ipcRenderer.on
    const realOff = electronStub.ipcRenderer.removeListener
    electronStub.ipcRenderer.on = (channel, fn) => listeners.set(channel, fn)
    electronStub.ipcRenderer.removeListener = (channel, fn) => { if (listeners.get(channel) === fn) listeners.delete(channel) }
    try {
      const bridge = loadPreload({}).langalphaDesktop
      const got = []
      const stop = bridge.onMenuCommand((c) => got.push(c))
      listeners.get('shell:menu-command')({ sender: 'secret' }, { command: 'new-thread' })
      assert.deepEqual(got, [{ command: 'new-thread' }])
      stop()
      assert.equal(listeners.has('shell:menu-command'), false)
      assert.equal(typeof bridge.onDesktopSettings('not a function'), 'function')
    } finally {
      electronStub.ipcRenderer.on = realOn
      electronStub.ipcRenderer.removeListener = realOff
    }
  })
})

describe('the menu bar page sets the lines', () => {
  test('only the menu bar page, only with something chosen, in the settings\' layout', async () => {
    await readyShell({
      edition: 'saas',
      platform: 'darwin',
      settings: { ...SIGNED_IN, desktop: { menubar: { items: [{ kind: 'portfolio' }], style: 'side' } } },
    })
    try {
      const [app] = windows
      const bar = menubarWindow()
      const icon = trays.at(-1)
      const setItems = ipcListeners.get('shell:set-tray-items')
      const items = [{ text: 'Port ▲0.42%' }, { text: 'NVDA ▼1%' }]
      const options = { refreshMs: 15000 }

      setItems(from(app, 'https://app.example.com/chat'), items, options)
      assert.equal(icon.title ?? '', '', 'the main window set the lines')
      setItems(from(bar, BAR_URL), items, options)
      assert.equal(icon.title, ' Port ▲0.42%  NVDA ▼1%')
      // A layout the page sends is not the shell's to take; anything but options is no options.
      setItems(from(bar, BAR_URL), items, { style: 'rotate', rotateSeconds: 2 })
      assert.equal(icon.title, ' Port ▲0.42%  NVDA ▼1%')
      setItems(from(bar, BAR_URL), items, 'junk')
      assert.equal(icon.title, ' Port ▲0.42%  NVDA ▼1%')

      // The layout changed in Settings shows at once, before the page next reports.
      const set = ipcHandlers.get('shell:set-desktop-settings')
      set(from(app, 'https://app.example.com/settings'), { menubar: { style: 'rotate' } })
      assert.equal(icon.title, ' Port ▲0.42%')

      // Turned off in Settings: the line goes at once, and a page a step behind cannot bring it back.
      set(from(app, 'https://app.example.com/settings'), { menubar: { items: [] } })
      assert.equal(icon.title, '')
      setItems(from(bar, BAR_URL), items, options)
      assert.equal(icon.title, '')
    } finally {
      emitApp('will-quit')
    }
  })

  test('the refresh is sent to the menu bar window', () => {
    const { store } = loadShell({ edition: 'saas', settings: { reachedApp: true } })
    const { createGlance } = require('../src/glance.js')
    const timers = fakeTimers()
    const desktop = desktopSettings.normalize({ menubar: { items: [{ kind: 'portfolio' }] } })
    const glance = createGlance({
      traits: traits('darwin', {}),
      settings: { get: () => desktop, onChange() {}, update() {} },
      commands: { deliver() {} },
      lifecycle: { isQuitting: () => false, afterUserWindowClosed() {} },
      store,
      timers,
      host: { mainWindow: () => null, showMain() {}, openInitialWindow() {}, route() {}, recordFacts() {} },
    })
    glance.registerIpc()
    glance.ensure()
    const bar = menubarWindow()
    const sent = []
    bar.webContents.send = (channel, payload) => sent.push([channel, payload])
    ipcListeners.get('shell:set-tray-items')(from(bar, BAR_URL), [{ text: 'A' }], { refreshMs: 15000 })
    timers.fire(15000)
    assert.deepEqual(sent, [['shell:tray-refresh', undefined]])
    glance.dispose()
  })
})

describe('the icon\'s menu, in the shell', () => {
  test('Linux: the icon\'s only menu follows a settings change', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN, platform: 'linux' })
    try {
      const icon = trays.at(-1)
      assert.equal(icon.menu[0].label, 'Open Market Glance…')
      assert.equal(icon.menu.find((i) => i.label === 'NVDA'), undefined)
      const set = ipcHandlers.get('shell:set-desktop-settings')
      set(from(new electronStub.BrowserWindow(), 'https://app.example.com/settings'), {
        menubar: { items: [{ kind: 'stock', symbol: 'NVDA' }] },
      })
      assert.ok(icon.menu.find((i) => i.label === 'NVDA' && i.type === 'checkbox'), 'the menu was not rebuilt')
    } finally {
      emitApp('will-quit')
    }
  })

  test('macOS: a right-click pops the menu up, built then', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN, platform: 'darwin' })
    try {
      const icon = trays.at(-1)
      icon.emit('right-click')
      const labels = icon.popped.map((i) => (i.type === 'separator' ? '---' : i.label))
      assert.deepEqual(labels.slice(0, 3), ['Open LangAlpha', '---', 'In the menu bar'])
    } finally {
      emitApp('will-quit')
    }
  })
})

describe('the menu bar window', () => {
  function makeMenubar(extra = {}) {
    loadShell({ edition: 'saas', settings: SIGNED_IN })
    const { createMenubar } = require('../src/menubar.js')
    return createMenubar({
      traits: traits('darwin', {}),
      preload: 'preload.js',
      recordFacts() {},
      route() {},
      backgroundColor: () => '#000',
      anchor: () => ({ x: 1000, y: 0, width: 24, height: 33 }),
      isQuitting: () => false,
      ...extra,
    })
  }

  test('a page that has not called in is never shown; the main window stands in', () => {
    let stoodIn = 0
    const menubar = makeMenubar({ onUnproven: () => { stoodIn += 1 } })
    const win = menubar.ensure()
    menubar.show()
    assert.equal(win.isVisible(), false)
    assert.equal(stoodIn, 1)
    menubar.markProven()
    menubar.show()
    assert.equal(win.isVisible(), true)
    // A new load has to prove itself again.
    win.webContents.emit('did-start-loading')
    menubar.hide()
    menubar.show()
    assert.equal(stoodIn, 2)
  })

  test('an older app that routes itself away in place is unloaded', () => {
    const menubar = makeMenubar()
    const win = menubar.ensure()
    logged(() => win.webContents.emit('did-navigate-in-page', {}, 'https://app.example.com/', true))
    assert.equal(win.loaded.at(-1), 'about:blank')
    // An in-page move on its own page, or in a frame, is the page's business.
    win.loadURL(BAR_URL)
    win.webContents.emit('did-navigate-in-page', {}, `${BAR_URL}#x`, true)
    win.webContents.emit('did-navigate-in-page', {}, 'https://elsewhere.example.com/', false)
    assert.equal(win.loaded.at(-1), BAR_URL)
  })

  test('an unloaded page is loaded again when the main window next reaches the app', () => {
    const menubar = makeMenubar()
    const win = menubar.ensure()
    const wc = win.webContents
    logged(() => wc.emit('did-navigate-in-page', {}, 'https://app.example.com/', true))
    assert.equal(win.loaded.at(-1), 'about:blank')
    // What Chromium reports for the blank document the unload commits.
    wc.emit('did-start-loading')
    wc.emit('did-navigate', {}, 'about:blank', -1)
    menubar.reloadIfFailed()
    assert.equal(win.loaded.at(-1), BAR_URL)
  })

  test('a renderer crash reloads the page once; a second within a minute waits for the next retry', () => {
    const menubar = makeMenubar()
    const win = menubar.ensure()
    const wc = win.webContents
    const realNow = Date.now
    let now = 1_000_000
    Date.now = () => now
    try {
      const loads = () => win.loaded.filter((u) => u === BAR_URL).length
      const before = loads()
      logged(() => wc.emit('render-process-gone', {}, { reason: 'crashed' }))
      assert.equal(loads(), before + 1)

      now += 10_000
      logged(() => wc.emit('render-process-gone', {}, { reason: 'crashed' }))
      assert.equal(loads(), before + 1)
      // Left failed, so the main window's next load retries it.
      menubar.reloadIfFailed()
      assert.equal(loads(), before + 2)

      now += 61_000
      logged(() => wc.emit('render-process-gone', {}, { reason: 'oom' }))
      assert.equal(loads(), before + 3)
    } finally {
      Date.now = realNow
    }
  })

  test('a tray click on a failed panel loads it again and still opens the main window', () => {
    let stoodIn = 0
    const menubar = makeMenubar({ onUnproven: () => { stoodIn += 1 } })
    const win = menubar.ensure()
    logged(() => win.webContents.emit('did-navigate-in-page', {}, 'https://app.example.com/', true))
    assert.equal(win.loaded.at(-1), 'about:blank')
    win.webContents.emit('did-start-loading')
    win.webContents.emit('did-navigate', {}, 'about:blank', -1)
    menubar.show()
    assert.equal(win.loaded.at(-1), BAR_URL)
    assert.equal(stoodIn, 1)
    assert.equal(win.isVisible(), false)
  })

  test('the hidden window reads as hidden to its page from the first load', () => {
    for (const platform of ['darwin', 'linux']) {
      const win = makeMenubar({ traits: traits(platform, {}) }).ensure()
      assert.equal(win.options.show, false)
      assert.equal(win.options.paintWhenInitiallyHidden, false, platform)
    }
  })

  test('the panel follows its page\'s height while open, under the icon', () => {
    const menubar = makeMenubar()
    const win = menubar.ensure()
    assert.equal(win.options.webPreferences.enablePreferredSizeMode, true)
    assert.equal(win.options.width, 380)

    // Closed, it only notes the height; the next show sizes it.
    win.webContents.emit('preferred-size-changed', {}, { width: 380, height: 330 })
    assert.equal(win.boundsSet, undefined)
    menubar.markProven()
    menubar.show()
    const icon = { x: 1000, y: 0, width: 24, height: 33 }
    assert.deepEqual(win.getBounds(), {
      x: icon.x + icon.width / 2 - 190, y: display.workArea.y + 6, width: 380, height: 330,
    })

    // Open, it grows in place, its top still under the icon, and stops at the screen.
    win.webContents.emit('preferred-size-changed', {}, { width: 380, height: 520 })
    assert.equal(win.getBounds().height, 520)
    assert.equal(win.getBounds().y, display.workArea.y + 6)
    win.webContents.emit('preferred-size-changed', {}, { width: 380, height: 5000 })
    assert.equal(win.getBounds().height, display.workArea.height - 2 * geometry.MARGIN)
    const placed = win.boundsSet.length
    win.webContents.emit('preferred-size-changed', {}, { width: 380, height: 6000 })
    assert.equal(win.boundsSet.length, placed, 'set the same bounds again')
  })

  test('the menu bar page logs one line per load, with its status only', () => {
    const win = makeMenubar().ensure()
    const wc = win.webContents
    const lines = logged(() => {
      wc.emit('did-start-loading')
      wc.emit('did-navigate', {}, BAR_URL, 200)
      wc.emit('did-finish-load')
      wc.emit('did-start-loading')
      wc.emit('did-fail-load', {}, -3, 'ERR_ABORTED', BAR_URL, true)
      wc.emit('did-fail-load', {}, -102, 'ERR_CONNECTION_REFUSED', BAR_URL, true)
      wc.emit('did-finish-load')
    })
    assert.deepEqual(lines, [
      '[menubar] the page loaded (HTTP 200)',
      '[menubar] the page failed to load (-102)',
    ])
  })
})
