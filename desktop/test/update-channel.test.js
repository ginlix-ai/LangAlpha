'use strict'

// The update channel an install follows, and the one move it must never make:
// back to an older version, whose backend cannot run on the data a newer one
// has migrated. electron-updater's `channel` setter turns downgrades on, so the
// updater is a fake that does the same.

const { test, describe, beforeEach, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const Module = require('node:module')

const helpers = require('./helpers')

const SRC = path.join(__dirname, '..', 'src')
const { app } = helpers.electronStub

function fakeUpdater(feedVersion) {
  const listeners = new Map()
  const fake = {
    _channel: null,
    allowDowngrade: false,
    get channel() { return this._channel },
    set channel(value) { this._channel = value; this.allowDowngrade = true },
    on(event, fn) { listeners.set(event, fn) },
    emit(event, ...args) { return listeners.get(event)(...args) },
    async checkForUpdates() {
      return { isUpdateAvailable: false, updateInfo: { version: feedVersion } }
    },
  }
  return fake
}

/** Runs `fn` while `process.platform` reads `platform`, as the updater reads it per event. */
function onPlatform(platform, fn) {
  const real = Object.getOwnPropertyDescriptor(process, 'platform')
  Object.defineProperty(process, 'platform', { value: platform })
  try {
    return fn()
  } finally {
    Object.defineProperty(process, 'platform', real)
  }
}

let updater = null
const realLoad = Module._load
const realVersion = app.getVersion
const resources = helpers.tempDir('la-feed-')
fs.writeFileSync(path.join(resources, 'app-update.yml'), 'provider: generic\nurl: https://feed.example.com\n')

function loadUpdater({ edition = 'oss', version, settings } = {}) {
  app.getVersion = () => version
  app.isPackaged = true
  process.resourcesPath = resources
  const shell = helpers.loadShell({ edition, settings })
  return { shell, updater: require(path.join(SRC, 'updater.js')) }
}

describe('update channel', () => {
  beforeEach(() => {
    updater = fakeUpdater('0.4.0')
    Module._load = function (request, ...rest) {
      if (request === 'electron-updater') return { autoUpdater: updater }
      return realLoad.call(this, request, ...rest)
    }
    helpers.messageBoxes.length = 0
  })

  after(() => {
    Module._load = realLoad
    app.getVersion = realVersion
    app.isPackaged = false
    delete process.resourcesPath
    helpers.cleanup()
  })

  test('a nightly follows nightlies, and picking stable never takes it back to an older version', async () => {
    const { shell, updater: channels } = loadUpdater({ version: '0.4.1-nightly.202611250700' })
    assert.equal(channels.canPickChannel(), true)
    assert.equal(channels.currentChannel(), 'nightly')

    await channels.checkManually(null)
    assert.equal(updater.channel, 'nightly')
    assert.equal(updater.allowDowngrade, false)

    await channels.pickChannel('stable', null)
    assert.equal(shell.store.get('updateChannel'), 'stable')
    assert.equal(updater.channel, 'latest')
    assert.equal(updater.allowDowngrade, false)
    // The feed's stable 0.4.0 is older, so the nightly stays and says why.
    const [, box] = helpers.messageBoxes.at(-1)
    assert.match(box.message, /newer than the latest stable release/)
  })

  test('a stable install moves to nightlies only when the person picks them', async () => {
    const { updater: channels } = loadUpdater({ version: '0.4.0' })
    assert.equal(channels.currentChannel(), 'stable')
    await channels.checkManually(null)
    assert.equal(updater.channel, 'latest')

    await channels.pickChannel('nightly', null)
    assert.equal(updater.channel, 'nightly')
    assert.equal(updater.allowDowngrade, false)
    // A stable update downloaded before the pick no longer installs on quit.
    assert.equal(updater.autoInstallOnAppQuit, false)
  })

  test('a pick during a check asks the picked channel, and the left one\'s answer is never shown', async () => {
    const { updater: channels } = loadUpdater({ version: '0.4.0' })
    const asked = []
    let answer
    updater.checkForUpdates = function () {
      asked.push(this.channel)
      if (asked.length === 1) return new Promise((resolve) => { answer = resolve })
      return Promise.resolve({ isUpdateAvailable: true, updateInfo: { version: '0.4.1-nightly.202611260700' } })
    }
    const first = channels.checkManually(null)
    await channels.pickChannel('nightly', null)
    answer({ isUpdateAvailable: false, updateInfo: { version: '0.4.0' } })
    await first
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(asked, ['latest', 'nightly'])
    assert.equal(helpers.messageBoxes.length, 1)
    assert.match(helpers.messageBoxes[0][1].message, /0\.4\.1-nightly\.202611260700 is downloading/)
  })

  test('a download from the channel the person left finishes held back and unoffered', async (t) => {
    const { updater: channels } = loadUpdater({ version: '0.4.1-nightly.202611250700' })
    channels.init(() => null)
    t.after(() => channels.stop())
    await channels.pickChannel('stable', null)
    const boxes = helpers.messageBoxes.length
    updater.emit('update-downloaded', { version: '0.4.1-nightly.202611260700' })
    assert.equal(updater.autoInstallOnAppQuit, false)
    assert.equal(helpers.messageBoxes.length, boxes)
    updater.emit('update-downloaded', { version: '0.4.1' })
    assert.equal(updater.autoInstallOnAppQuit, true)
  })

  test('on a Mac, an update already handed to Squirrel still installs on quit, and the pick says so', async (t) => {
    const { updater: channels } = loadUpdater({ version: '0.4.1-nightly.202611250700' })
    channels.init(() => null)
    t.after(() => channels.stop())
    onPlatform('darwin', () => updater.emit('update-downloaded', { version: '0.4.1-nightly.202611260700' }))
    await channels.pickChannel('stable', null)
    const [, box] = helpers.messageBoxes.at(-1)
    assert.match(box.message, /0\.4\.1-nightly\.202611260700 installs when you quit/)
    assert.match(box.detail, /Updates after it come from Stable/)
  })

  test('a pick made while the timed check is out asks the picked channel once it answers', async (t) => {
    const { updater: channels } = loadUpdater({ version: '0.4.0' })
    const asked = []
    let answer
    const none = { isUpdateAvailable: false, updateInfo: { version: '0.4.0' } }
    updater.checkForUpdates = function () {
      asked.push(this.channel)
      return asked.length === 1 ? new Promise((resolve) => { answer = () => resolve(none) }) : Promise.resolve(none)
    }
    channels.init(() => null)
    t.after(() => channels.stop())
    const picked = channels.pickChannel('nightly', null)
    await new Promise((resolve) => setImmediate(resolve))
    // electron-updater would hand this check the timed one's answer.
    assert.deepEqual(asked, ['latest'])
    answer()
    await picked
    assert.deepEqual(asked, ['latest', 'nightly'])
  })

  test('a release candidate follows its own manifests until the person picks a channel', async () => {
    const { updater: channels } = loadUpdater({ version: '0.4.0-rc.1' })
    assert.equal(channels.currentChannel(), null)
    await channels.checkManually(null)
    assert.equal(updater.channel, null)
    await channels.pickChannel('stable', null)
    assert.equal(updater.channel, 'latest')
  })

  test('a hand-edited channel falls back to the running version\'s', () => {
    const { updater: channels } = loadUpdater({ version: '0.4.0', settings: { updateChannel: 'beta' } })
    assert.equal(channels.currentChannel(), 'stable')
  })

  test('the hosted edition has no channel to pick and keeps the one it was built with', async () => {
    const { updater: channels } = loadUpdater({ edition: 'saas', version: '0.4.0' })
    assert.equal(channels.canPickChannel(), false)
    await channels.checkManually(null)
    assert.equal(updater.channel, null)
  })
})
