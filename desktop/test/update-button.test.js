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
    async checkForUpdates() { return { isUpdateAvailable: false, updateInfo: { version: '0.4.0' } } },
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
function start({ version = '0.4.0', buildConfig, stopsServer = () => true } = {}) {
  app.getVersion = () => version
  app.isPackaged = true
  process.resourcesPath = resources
  helpers.loadShell({ serverUrl: APP, buildConfig })
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
  return { win, ask, install, last }
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

  test('picking the other channel takes back an update the old one downloaded', async () => {
    const { ask, last } = start()
    ask()
    // Off a Mac: there Squirrel.Mac already holds it (update-channel.test.js).
    onPlatform('linux', () => fake.emit('update-downloaded', { version: '0.4.1' }))
    await updater.pickChannel('nightly', null)
    assert.equal(last().status, 'idle')
    assert.equal(fake.autoInstallOnAppQuit, false)
  })
})
