'use strict'

// Quitting and the last window (lifecycle.js). The hidden menu bar window keeps
// `window-all-closed` from firing, and a page's `beforeunload` can cancel a quit
// already under way; both are paths a desk reaches only by accident.

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')

const {
  cleanup, emitApp, windows, trays, quits, sweep, menubarWindow, readyShell, appMenuItem,
} = require('./helpers')

const { createLifecycle } = require('../src/lifecycle.js')
const { traits } = require('../src/platformTraits.js')

after(cleanup)

function fakeApp() {
  const app = new EventEmitter()
  app.quits = 0
  app.quit = () => { app.quits += 1 }
  return app
}

/** A user window, as much of one as lifecycle.js touches. */
const userWindow = () => ({ webContents: new EventEmitter() })

describe('the lifecycle', () => {
  function lifecycleOn(platform, options = {}) {
    const app = fakeApp()
    const updater = new EventEmitter()
    const cancelled = []
    const life = createLifecycle({
      traits: traits(platform, {}),
      userWindowOpen: () => false,
      onQuitCancelled: () => cancelled.push(true),
      app,
      autoUpdater: () => updater,
      ...options,
    })
    life.install()
    return { life, app, updater, cancelled }
  }

  test('a quit is under way from before-quit, and a page that refuses to unload cancels it', () => {
    const { life, app, cancelled } = lifecycleOn('darwin')
    const win = userWindow()
    life.watchQuitCancel(win)
    win.webContents.emit('will-prevent-unload')
    assert.equal(life.isQuitting(), false)
    assert.equal(cancelled.length, 0, 'a refusal outside a quit was read as a cancelled one')

    app.emit('before-quit')
    assert.equal(life.isQuitting(), true)
    win.webContents.emit('will-prevent-unload')
    assert.equal(life.isQuitting(), false, 'still quitting after the quit was cancelled')
    assert.equal(cancelled.length, 1, 'what the sweep closed was not put back')
  })

  test('macOS: an update install marks the quit, since Squirrel closes the windows first', () => {
    const mac = lifecycleOn('darwin')
    mac.updater.emit('before-quit-for-update')
    assert.equal(mac.life.isQuitting(), true)
    const win = lifecycleOn('win32')
    assert.equal(win.updater.listenerCount('before-quit-for-update'), 0)
  })

  test('teardown waits for will-quit, and runs in the order registered', () => {
    const { life, app } = lifecycleOn('darwin')
    const ran = []
    life.onWillQuit(() => ran.push('icon'))
    life.onWillQuit(() => ran.push('updater'))
    app.emit('before-quit')
    assert.deepEqual(ran, [])
    app.emit('will-quit')
    assert.deepEqual(ran, ['icon', 'updater'])
  })

  test('the last user window: macOS stays, Linux quits, Windows stays in the tray when it may', () => {
    let open = false
    const mac = lifecycleOn('darwin', { userWindowOpen: () => open })
    mac.life.afterUserWindowClosed()
    assert.equal(mac.app.quits, 0)

    const linux = lifecycleOn('linux', { userWindowOpen: () => open })
    open = true
    linux.life.afterUserWindowClosed()
    assert.equal(linux.app.quits, 0, 'quit with a user window still open')
    open = false
    linux.life.afterUserWindowClosed()
    assert.equal(linux.app.quits, 1)

    let tray = true
    const win = lifecycleOn('win32', { userWindowOpen: () => false, keepInTray: () => tray })
    win.life.afterUserWindowClosed()
    assert.equal(win.app.quits, 0)
    tray = false
    win.life.afterUserWindowClosed()
    assert.equal(win.app.quits, 1)

    // A window closing in a quit's own sweep decides nothing.
    const quitting = lifecycleOn('linux')
    quitting.app.emit('before-quit')
    quitting.life.afterUserWindowClosed()
    assert.equal(quitting.app.quits, 0)
  })

  test('before the menu bar window exists, window-all-closed quits everywhere but macOS', () => {
    const mac = lifecycleOn('darwin')
    mac.app.emit('window-all-closed')
    assert.equal(mac.app.quits, 0)
    for (const platform of ['win32', 'linux']) {
      const other = lifecycleOn(platform)
      other.app.emit('window-all-closed')
      assert.equal(other.app.quits, 1, platform)
    }
  })
})

// A ready app: the menu bar window and its icon exist, and closing or quitting
// is decided the way it is at a desk. Each test ends the way the app does, with
// `will-quit`, which stops what the ready app started.
describe('quitting and the last window, in the shell', () => {
  const SIGNED_IN = { reachedApp: true, appChrome: true, platformChrome: true }

  test('a page that cancels the quit leaves the menu bar window and its icon in place', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN })
    try {
      const [app] = windows
      const bar = menubarWindow()
      const [tray] = trays
      assert.ok(tray && !tray.isDestroyed(), 'no icon to begin with')

      emitApp('before-quit', {})
      // The sweep reaches the menu bar window first: a quit under way may close it.
      assert.equal(sweep(bar).prevented, false, 'the menu bar window held up the quit')
      assert.equal(tray.isDestroyed(), false, 'the icon went before the quit was certain')

      // Then the app's page refuses to unload over an unsaved edit.
      app.webContents.emit('will-prevent-unload', { preventDefault() {} })
      const rebuilt = menubarWindow()
      assert.ok(rebuilt && rebuilt !== bar, 'the menu bar window was not made again')
      assert.equal(rebuilt.loaded.at(-1), 'https://app.example.com/desktop/menubar')
      assert.equal(trays.length, 1, 'a second icon was made')
      assert.equal(tray.isDestroyed(), false)
      // And it is back to hiding on close.
      assert.equal(sweep(rebuilt).prevented, true, 'still quitting after the quit was cancelled')
    } finally {
      emitApp('will-quit')
    }
  })

  test('a cancel that lands before the sweep reaches the menu bar window keeps that window', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN })
    try {
      const [app] = windows
      const bar = menubarWindow()
      emitApp('before-quit', {})
      app.webContents.emit('will-prevent-unload', { preventDefault() {} })
      assert.equal(sweep(bar).prevented, true)
      assert.equal(menubarWindow(), bar)
    } finally {
      emitApp('will-quit')
    }
  })

  test('a page that refuses to unload outside a quit changes nothing', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN })
    try {
      const bar = menubarWindow()
      windows[0].webContents.emit('will-prevent-unload', { preventDefault() {} })
      assert.equal(menubarWindow(), bar)
      assert.equal(sweep(bar).prevented, true, 'a refusal outside a quit started one')
    } finally {
      emitApp('will-quit')
    }
  })

  test('the menu bar page never holds up a quit, and the icon goes once the quit is certain', async () => {
    await readyShell({ edition: 'saas', settings: SIGNED_IN })
    const bar = menubarWindow()
    let allowed = false
    bar.webContents.emit('will-prevent-unload', { preventDefault() { allowed = true } })
    assert.equal(allowed, true, 'the menu bar page could cancel a quit')
    emitApp('before-quit', {})
    sweep(bar)
    emitApp('will-quit')
    assert.equal(trays[0].isDestroyed(), true)
  })

  /** The server picker, opened the way a user opens it: from the application menu. */
  function openPicker() {
    appMenuItem('Change Server…').click()
    return windows.at(-1)
  }

  test('Linux: closing the server picker last quits', async () => {
    await readyShell({ serverUrl: 'http://127.0.0.1:8000', platform: 'linux' })
    try {
      const [app] = windows
      const picker = openPicker()
      assert.equal(picker.options.title, 'Connect to your server')
      app.close()
      assert.equal(quits.length, 0, 'quit with the picker still open')
      picker.close()
      assert.equal(quits.length, 1, 'the picker was the last window and the app kept running')
    } finally {
      emitApp('will-quit')
    }
  })

  test('Linux: an open Market Glance window is one of the user\'s windows', async () => {
    await readyShell({ serverUrl: 'http://127.0.0.1:8000', platform: 'linux' })
    try {
      const [app] = windows
      trays.at(-1).menu.find((i) => i.label === 'Open Market Glance…').click()
      const glance = menubarWindow()
      assert.equal(glance.isVisible(), true)
      app.close()
      assert.equal(quits.length, 0, 'quit under an open Market Glance window')
      const event = sweep(glance)
      assert.equal(event.prevented, true, 'closing Market Glance took down the notification source')
      // The icon is still there to come back through.
      assert.equal(quits.length, 0, 'closing Market Glance quit an app whose icon is on')
    } finally {
      emitApp('will-quit')
    }
  })

  test('Linux: with the icon off, closing Market Glance last quits', async () => {
    await readyShell({
      serverUrl: 'http://127.0.0.1:8000', platform: 'linux', settings: { desktop: { menubar: { enabled: false } } },
    })
    try {
      const [app] = windows
      const glance = menubarWindow()
      glance.show()
      app.close()
      assert.equal(quits.length, 0, 'quit under an open Market Glance window')
      sweep(glance)
      assert.equal(quits.length, 1, 'Market Glance was the last window and the app kept running')
    } finally {
      emitApp('will-quit')
    }
  })

  test('Windows: a session ending closes the hidden menu bar window', async () => {
    await readyShell({ serverUrl: 'http://127.0.0.1:8000', platform: 'win32' })
    try {
      const bar = menubarWindow()
      assert.equal(sweep(bar).prevented, true, 'the menu bar window closed outside a quit')
      bar.emit('session-end')
      assert.equal(sweep(bar).prevented, false, 'the menu bar window held up the end of the session')
    } finally {
      emitApp('will-quit')
    }
  })

  test('Windows: closing the picker last keeps the app in the tray', async () => {
    await readyShell({ serverUrl: 'http://127.0.0.1:8000', platform: 'win32' })
    try {
      const [app] = windows
      const picker = openPicker()
      app.close()
      picker.close()
      assert.equal(quits.length, 0)
    } finally {
      emitApp('will-quit')
    }
  })
})
