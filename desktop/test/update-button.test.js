'use strict'

// The sidebar's update button: what a page showing the app hears about an
// update, and the native prompt that button stands in for once a page asks.

const { test, describe, beforeEach, afterEach, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const Module = require('node:module')

const helpers = require('./helpers')

const SRC = path.join(__dirname, '..', 'src')
const { app, BrowserWindow } = helpers.electronStub
const APP = 'http://localhost:5173'

function fakeUpdater() {
  const listeners = new Map()
  return {
    channel: null,
    allowDowngrade: false,
    autoInstallOnAppQuit: false,
    installs: 0,
    on(event, fn) { listeners.set(event, fn) },
    emit(event, ...args) { return listeners.get(event)(...args) },
    downloads: 0,
    async checkForUpdates() { return { isUpdateAvailable: false, updateInfo: { version: '0.4.0' } } },
    async downloadUpdate() { this.downloads += 1 },
    quitAndInstall() { this.installs += 1 },
  }
}

/** Runs `fn` while `process.platform` reads `platform`, as the updater reads it per download. */
function onPlatform(platform, fn) {
  const real = Object.getOwnPropertyDescriptor(process, 'platform')
  Object.defineProperty(process, 'platform', { value: platform })
  try {
    return fn()
  } finally {
    Object.defineProperty(process, 'platform', real)
  }
}

let fake = null
let updater = null
const realLoad = Module._load
const realVersion = app.getVersion
const resources = helpers.tempDir('la-update-')
fs.writeFileSync(path.join(resources, 'app-update.yml'), 'provider: generic\nurl: https://feed.example.com\n')

/** The updater under a packaged OSS build on this server, with one window showing the app. */
function start({ version = '0.4.0', buildConfig, stopsServer = () => true, automatic = true } = {}) {
  app.getVersion = () => version
  app.isPackaged = true
  process.resourcesPath = resources
  helpers.loadShell({ serverUrl: APP, buildConfig })
  const store = require(path.join(SRC, 'store.js'))
  store.set('desktop', { ...store.get('desktop'), automaticUpdates: automatic })
  updater = require(path.join(SRC, 'updater.js'))
  const win = new BrowserWindow()
  win.loaded.push(`${APP}/`)
  const sent = []
  win.webContents.send = (channel, payload) => sent.push([channel, payload])
  updater.registerIpc((event) => event.win)
  updater.init(() => win, { stopsServer })
  const ask = () => helpers.ipcHandlers.get('shell:get-update-state')({ win })
  const install = () => helpers.ipcHandlers.get('shell:install-update')({ win })
  const last = () => sent.filter(([channel]) => channel === 'shell:update-state').at(-1)?.[1]
  /** The person flips the setting, as the settings service tells the updater. */
  const setAutomatic = (on) => {
    store.set('desktop', { ...store.get('desktop'), automaticUpdates: on })
    updater.setAutomatic()
  }
  return { win, ask, install, last, setAutomatic }
}

describe('update button', () => {
  beforeEach(() => {
    fake = fakeUpdater()
    Module._load = function (request, ...rest) {
      if (request === 'electron-updater') return { autoUpdater: fake }
      return realLoad.call(this, request, ...rest)
    }
    helpers.messageBoxes.length = 0
  })

  afterEach(() => updater?.stop())

  after(() => {
    Module._load = realLoad
    app.getVersion = realVersion
    app.isPackaged = false
    delete process.resourcesPath
    helpers.cleanup()
  })

  test('a downloaded update reaches the page, which then stands in for the native prompt', async () => {
    const { ask, last } = start()
    assert.equal(ask().status, 'idle')
    fake.emit('update-downloaded', { version: '0.4.1' })
    assert.deepEqual(
      { status: last().status, version: last().version, currentVersion: last().currentVersion, stopsServer: last().stopsServer },
      { status: 'ready', version: '0.4.1', currentVersion: '0.4.0', stopsServer: true },
    )
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.length, 0)
  })

  test('a page that never asks still gets the native prompt', async () => {
    const { last } = start()
    fake.emit('update-downloaded', { version: '0.4.1' })
    assert.equal(last().status, 'ready')
    await new Promise((resolve) => setImmediate(resolve))
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is ready to install/)
  })

  test('a document loaded after the page asked gets the native prompt until it asks too', async () => {
    const { win, ask } = start()
    ask()
    win.webContents.emit('did-navigate')
    fake.emit('update-downloaded', { version: '0.4.1' })
    await new Promise((resolve) => setImmediate(resolve))
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is ready to install/)
  })

  test('a page that takes the button down gets the native prompt again', async () => {
    const { win, ask } = start()
    ask()
    helpers.ipcHandlers.get('shell:hide-updates')({ win })
    fake.emit('update-downloaded', { version: '0.4.1' })
    await new Promise((resolve) => setImmediate(resolve))
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is ready to install/)
    // Shown and taken down again, as a sidebar collapses and expands, leaves
    // nothing waiting on the next document.
    for (let i = 0; i < 20; i += 1) {
      ask()
      helpers.ipcHandlers.get('shell:hide-updates')({ win })
    }
    assert.equal(win.webContents.listenerCount('did-navigate'), 0)
  })

  test('the native prompt stands in once for a ready update whose button the page took down', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const { win, ask } = start()
    ask()
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    await new Promise((resolve) => setImmediate(resolve))
    const shown = helpers.messageBoxes.length
    const hide = () => helpers.ipcHandlers.get('shell:hide-updates')({ win })
    // A sidebar collapsing to its rail mounts the button again at once.
    hide()
    ask()
    t.mock.timers.tick(1000)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.length, shown)
    // The phone layout has no sidebar to put it back.
    hide()
    t.mock.timers.tick(1000)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.length, shown + 1)
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is ready to install/)
    // Back and forth across the layout asks once.
    ask()
    hide()
    t.mock.timers.tick(1000)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.length, shown + 1)
  })

  test('a ready update the page showed is offered natively when the next document does not ask for it', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const { win, ask } = start()
    ask()
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    await new Promise((resolve) => setImmediate(resolve))
    const shown = helpers.messageBoxes.length
    // A reload into a web app with the button asks again as it boots.
    win.webContents.emit('did-navigate')
    t.mock.timers.tick(2000)
    ask()
    t.mock.timers.tick(5000)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.length, shown)
    // One that predates the button never asks.
    win.webContents.emit('did-navigate')
    t.mock.timers.tick(5000)
    await new Promise((resolve) => setImmediate(resolve))
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is ready to install/)
  })

  test('the button restarts only into an update that is ready', async () => {
    const { ask, install } = start()
    ask()
    await install()
    assert.equal(fake.installs, 0)
    fake.emit('update-downloaded', { version: '0.4.1' })
    await install()
    assert.equal(fake.installs, 1)
  })

  test('a server elsewhere is not stopped by the restart, and the page is told so', () => {
    const { ask } = start({ stopsServer: () => false })
    assert.equal(ask().stopsServer, false)
  })

  test('the notes link names the release the update was published as', () => {
    const releasesPage = 'https://github.com/example/app/releases/'
    let page = start({ buildConfig: { releasesPage } })
    fake.emit('update-downloaded', { version: '0.4.1' })
    assert.equal(page.last().notesUrl, 'https://github.com/example/app/releases/tag/desktop-oss-v0.4.1')
    updater.stop()

    page = start({ version: '0.4.1-nightly.202611240717', buildConfig: { releasesPage } })
    fake.emit('update-downloaded', { version: '0.4.1-nightly.202611250717' })
    assert.equal(page.last().notesUrl, 'https://github.com/example/app/releases/tag/desktop-oss-nightly')
    assert.equal(page.last().channel, 'nightly')
    updater.stop()

    page = start()
    fake.emit('update-downloaded', { version: '0.4.1' })
    assert.equal(page.last().notesUrl, null)
  })

  test('with automatic updates off, an update is offered and downloads only when asked', async () => {
    const { ask, install, last } = start({ automatic: false })
    assert.equal(ask().automatic, false)
    assert.equal(fake.autoDownload, false)
    assert.equal(fake.autoInstallOnAppQuit, false)
    await fake.emit('update-available', { version: '0.4.1' })
    assert.equal(last().status, 'available')
    assert.equal(fake.downloads, 0)

    const asked = install()
    assert.equal(last().status, 'downloading')
    await asked
    assert.equal(fake.downloads, 1)
    fake.emit('update-downloaded', { version: '0.4.1' })
    // Asked for, so it installs on quit like any other.
    assert.deepEqual([last().status, last().installsOnQuit], ['ready', true])
    // A newer one found meanwhile waits until the restart into this one.
    await fake.emit('update-available', { version: '0.4.2' })
    assert.deepEqual([last().status, last().version], ['ready', '0.4.1'])
  })

  test('a native offer whose download failed is offered again at the next check', async (t) => {
    t.after(() => helpers.setMessageBox(0))
    const { last } = start({ automatic: false })
    fake.downloadUpdate = async () => { throw new Error('offline') }
    helpers.setMessageBox(1)
    await fake.emit('update-available', { version: '0.4.1' })
    assert.equal(last().status, 'available')
    const offers = helpers.messageBoxes.length
    await fake.emit('update-available', { version: '0.4.1' })
    assert.equal(helpers.messageBoxes.length, offers + 1)
  })

  test('turning automatic updates off keeps one that downloaded by itself from installing on quit', async () => {
    const { win, ask, last, setAutomatic } = start()
    ask()
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    assert.equal(last().installsOnQuit, true)
    setAutomatic(false)
    assert.deepEqual([last().status, last().automatic, last().installsOnQuit], ['ready', false, false])
    assert.equal(fake.autoDownload, false)
    // Check for Updates then offers only the restart.
    fake.checkForUpdates = async () => ({ isUpdateAvailable: true, updateInfo: { version: '0.4.1' } })
    await updater.checkManually(win)
    assert.equal(helpers.messageBoxes.at(-1)[1].detail, 'Restart to install it.')
    onPlatform('linux', () => setAutomatic(true))
    assert.deepEqual([last().automatic, last().installsOnQuit], [true, true])
  })

  test('on a Mac, one that downloaded with automatic updates off stays a restart away when they come back on', () => {
    const { ask, last, setAutomatic } = start()
    ask()
    onPlatform('darwin', () => {
      setAutomatic(false)
      fake.emit('update-downloaded', { version: '0.4.1' })
      setAutomatic(true)
    })
    // Squirrel.Mac never took it, and quitAndInstall() asks for it only with the flag down.
    assert.equal(fake.autoInstallOnAppQuit, false)
    assert.deepEqual([last().status, last().installsOnQuit], ['ready', false])
  })

  test('a download asked for while another transfer runs starts once that one settles', async () => {
    const { ask, install, last } = start({ automatic: false })
    ask()
    let settle
    const running = new Promise((resolve) => { settle = resolve })
    let calls = 0
    // electron-updater hands back a transfer under way, here a nightly from
    // before the person picked stable, whose download is held back.
    fake.downloadUpdate = async () => {
      calls += 1
      if (calls === 1) {
        await running
        return onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1-nightly.202611260000' }))
      }
      onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    }
    await fake.emit('update-available', { version: '0.4.1' })
    const asked = install()
    assert.equal(last().status, 'downloading')
    settle()
    await asked
    assert.equal(calls, 2)
    assert.deepEqual([last().status, last().version], ['ready', '0.4.1'])
  })

  test('on a Mac, one Squirrel already holds still installs on quit when automatic updates go off, and the panel says so', () => {
    const { ask, last, setAutomatic } = start()
    ask()
    onPlatform('darwin', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    setAutomatic(false)
    assert.deepEqual([last().status, last().automatic, last().installsOnQuit], ['ready', false, true])
  })

  test('an update downloaded because automatic updates came on stops installing on quit when they go off', async () => {
    const { ask, last, setAutomatic } = start({ automatic: false })
    ask()
    await fake.emit('update-available', { version: '0.4.1' })
    setAutomatic(true)
    assert.equal(last().status, 'downloading')
    setAutomatic(false)
    fake.emit('update-downloaded', { version: '0.4.1' })
    assert.deepEqual([last().status, last().installsOnQuit], ['ready', false])
  })

  test('a failed download asked for leaves no mark on one automatic updates start later', async () => {
    const { ask, install, last, setAutomatic } = start({ automatic: false })
    ask()
    await fake.emit('update-available', { version: '0.4.1' })
    fake.downloadUpdate = async () => { throw new Error('offline') }
    await install()
    assert.equal(last().status, 'available')
    fake.downloadUpdate = async () => {}
    onPlatform('linux', () => {
      setAutomatic(true)
      setAutomatic(false)
      fake.emit('update-downloaded', { version: '0.4.1' })
    })
    assert.deepEqual([last().status, last().installsOnQuit], ['ready', false])
  })

  test('Later on an update Check for Updates found is not asked again at the next timed check', async (t) => {
    t.after(() => helpers.setMessageBox(0))
    const { win } = start({ automatic: false })
    fake.checkForUpdates = async () => ({ isUpdateAvailable: true, updateInfo: { version: '0.4.1' } })
    helpers.setMessageBox(0)
    await updater.checkManually(win)
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is available/)
    const asked = helpers.messageBoxes.length
    await fake.emit('update-available', { version: '0.4.1' })
    assert.equal(helpers.messageBoxes.length, asked)
  })

  test('off a Mac, one that downloaded with automatic updates off does not claim to install on quit when they come back on', () => {
    const { ask, last, setAutomatic } = start()
    ask()
    onPlatform('linux', () => {
      setAutomatic(false)
      fake.emit('update-downloaded', { version: '0.4.1' })
      setAutomatic(true)
    })
    // electron-updater added no quit handler for it, so a raised flag would install nothing.
    assert.equal(fake.autoInstallOnAppQuit, false)
    assert.deepEqual([last().status, last().installsOnQuit], ['ready', false])
  })

  test('a ready update holds back a newer download until the restart into it', () => {
    const { ask } = start()
    ask()
    assert.equal(fake.autoDownload, true)
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    assert.equal(fake.autoDownload, false)
  })

  test('a failed download is offered again as what the next download fetches', async () => {
    const { ask, install, last } = start({ automatic: false })
    ask()
    await fake.emit('update-available', { version: '0.4.1' })
    fake.downloadUpdate = async () => {
      // A check finds a newer one while this download is out.
      await fake.emit('update-available', { version: '0.4.2' })
      throw new Error('offline')
    }
    await install()
    assert.deepEqual([last().status, last().version], ['available', '0.4.2'])
  })

  test('Download in a native offer a newer one has replaced downloads nothing', async (t) => {
    const { dialog } = helpers.electronStub
    const real = dialog.showMessageBox
    t.after(() => { dialog.showMessageBox = real })
    let answer
    dialog.showMessageBox = (...args) => {
      helpers.messageBoxes.push(args)
      return new Promise((resolve) => { answer = resolve })
    }
    const { last } = start({ automatic: false })
    const first = fake.emit('update-available', { version: '0.4.1' })
    dialog.showMessageBox = real
    helpers.setMessageBox(0)
    await fake.emit('update-available', { version: '0.4.2' })
    answer({ response: 1 })
    await first
    assert.equal(fake.downloads, 0)
    assert.deepEqual([last().status, last().version], ['available', '0.4.2'])
  })

  test('an update that lands while the button is down is prompted for once', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const { win, ask } = start()
    ask()
    const shown = helpers.messageBoxes.length
    helpers.ipcHandlers.get('shell:hide-updates')({ win })
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    await new Promise((resolve) => setImmediate(resolve))
    t.mock.timers.tick(1000)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.length, shown + 1)
  })

  test('turning automatic updates on downloads an update already offered', async () => {
    const { ask, last, setAutomatic } = start({ automatic: false })
    ask()
    await fake.emit('update-available', { version: '0.4.1' })
    setAutomatic(true)
    assert.equal(last().status, 'downloading')
    assert.equal(fake.downloads, 1)
    assert.equal(fake.autoDownload, true)
  })

  test('picking the other channel takes back an update the old one downloaded', async () => {
    const { ask, last, setAutomatic } = start()
    ask()
    // Off a Mac: there Squirrel.Mac already holds it (update-channel.test.js).
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    await updater.pickChannel('nightly', null)
    assert.equal(last().status, 'idle')
    assert.equal(fake.autoInstallOnAppQuit, false)
    // Switching automatic updates does not bring the held-back one back.
    setAutomatic(false)
    setAutomatic(true)
    assert.equal(fake.autoInstallOnAppQuit, false)
  })

  test('with automatic updates off, an update found while no window was open is offered at a later check', async () => {
    const { win } = start({ automatic: false })
    win.destroyed = true
    await fake.emit('update-available', { version: '0.4.1' })
    assert.equal(helpers.messageBoxes.length, 0)
    win.destroyed = false
    await fake.emit('update-available', { version: '0.4.1' })
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is available/)
  })

  test('the native prompt for an update that downloaded as automatic updates went off asks for a restart', async () => {
    const { setAutomatic } = start()
    setAutomatic(false)
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(helpers.messageBoxes.at(-1)[1].detail, 'Restart to install it.')
  })

  test('with automatic updates off, Check for Updates keeps a downloaded update offered over a newer one', async () => {
    const { win, ask, install, last } = start({ automatic: false })
    ask()
    await fake.emit('update-available', { version: '0.4.1' })
    await install()
    fake.emit('update-downloaded', { version: '0.4.1' })
    fake.checkForUpdates = async () => ({ isUpdateAvailable: true, updateInfo: { version: '0.4.2' } })
    await updater.checkManually(win)
    assert.deepEqual([last().status, last().version], ['ready', '0.4.1'])
    assert.match(helpers.messageBoxes.at(-1)[1].message, /0\.4\.1 is ready to install/)
  })
})
