'use strict'

/** The shell and the bundled stack together: the choice, the progress, the quit. */

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

const { SRC, fakeTree, control, calls, until, cleanup } = require('./stackFixture')
const { REPO } = require(path.join(SRC, 'helpPrompt.js'))

after(cleanup)

describe('the shell and the stack', () => {
  const helpers = require('./helpers')
  const loaded = () => Object.keys(require.cache).some((key) => key.startsWith(SRC + path.sep) && key.endsWith('index.js'))

  after(() => {
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    helpers.cleanup()
  })

  test('the hosted edition never loads it, even with a tree present', () => {
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = fakeTree()
    helpers.loadShell({ edition: 'saas' })
    assert.equal(loaded(), false)
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
  })

  test('a build with no tree opens the server form, framed as it always was', () => {
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    const { main } = helpers.loadShell({ edition: 'oss' })
    main.openLaunchWindows()
    const [picker] = helpers.windows
    assert.equal(helpers.windows.length, 1)
    assert.equal(picker.options.title, 'Connect to your server')
    assert.equal(picker.options.height, 460)
  })

  /** The app's window: the one on an http page that is not the menu bar's. */
  const appWindow = () => helpers.windows.find((w) => !w.destroyed && w !== helpers.menubarWindow()
    && /^http/.test(w.loaded.at(-1) || ''))
  const quitEvent = () => ({ prevented: false, preventDefault() { this.prevented = true } })
  const LOOPBACK = /^http:\/\/127\.0\.0\.1:\d+$/

  test('upgrading a 0.3.2 install: its server stays the choice and the stack is never started', () => {
    const tree = fakeTree()
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = tree
    const { main, store, backend } = helpers.loadShell({ edition: 'oss', settings: { serverUrl: 'http://stack.example:8080' } })
    main.openLaunchWindows()
    assert.equal(helpers.windows.length, 1)
    assert.equal(helpers.windows[0].loaded.at(-1), 'http://stack.example:8080')
    assert.deepEqual(backend.choice(), { kind: 'server', origin: 'http://stack.example:8080' })
    assert.equal(store.get('stackMode'), null)
    assert.deepEqual(calls(tree), [])
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
  })

  test('with a tree and nothing chosen a first run is offered the choice', () => {
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = fakeTree()
    const { main } = helpers.loadShell({ edition: 'oss' })
    main.openLaunchWindows()
    assert.match(helpers.windows[0].loaded.at(-1), /setup\.html$/)
    assert.equal(helpers.windows[0].options.height, 640)
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
  })

  // What earlier builds of the stack left: its own origin stored as the server.
  const EARLIER_BUNDLED = { stackMode: 'bundled', stackPort: 43123, serverUrl: 'http://127.0.0.1:43123' }

  test('upgrading an earlier stack install: the loopback stops posing as a server', () => {
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = fakeTree()
    const { store, backend } = helpers.loadShell({ edition: 'oss', settings: EARLIER_BUNDLED })
    assert.equal(store.get('serverUrl'), null)
    assert.equal(store.get('stackMode'), 'bundled')
    assert.deepEqual(backend.choice(), { kind: 'bundled' })
    assert.equal(backend.origin(), 'http://127.0.0.1:43123')
    assert.equal(backend.serverAddress(), null)
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
  })

  test('a stack chosen on a build with none shows the picker, never a dead loopback', () => {
    delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    for (const settings of [EARLIER_BUNDLED, { stackMode: 'bundled', stackPort: 43123, serverUrl: 'http://mine.example' }]) {
      const { main, backend } = helpers.loadShell({ edition: 'oss', settings })
      main.openLaunchWindows()
      assert.equal(helpers.windows.length, 1)
      assert.equal(helpers.windows[0].options.title, 'Connect to your server')
      assert.equal(backend.choice(), null)
      // The user's own address, if this install kept one, is what the form offers.
      assert.equal(backend.serverAddress(), settings.serverUrl === 'http://mine.example' ? 'http://mine.example' : null)
    }
  })

  test('a launch on the stack shows progress, opens the app on its origin, and a quit waits for it', async () => {
    const tree = fakeTree()
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = tree
    const { main, store, backend } = helpers.loadShell({ edition: 'oss', settings: { stackMode: 'bundled', stackPort: 0 } })
    try {
      main.openLaunchWindows()
      const [progress] = helpers.windows
      assert.match(progress.loaded.at(-1), /setup\.html$/)
      const app = await until(appWindow)
      assert.match(app.loaded.at(-1), LOOPBACK)
      assert.equal(backend.origin(), app.loaded.at(-1))
      // Its origin is not the user's server, and is never stored as one.
      assert.equal(store.get('serverUrl'), null)
      assert.equal(progress.destroyed, true)

      // A quit the page refuses, over an unsaved edit, leaves the stack up.
      const asked = quitEvent()
      helpers.emitApp('before-quit', asked)
      assert.equal(asked.prevented, false)
      app.webContents.emit('will-prevent-unload', { preventDefault() {} })
      assert.equal(backend.isUp(), true)
      assert.ok(!calls(tree).includes('server stop'))

      // One that goes through closes the windows the ordinary way, then holds the
      // exit until the server and then Postgres have stopped.
      helpers.emitApp('before-quit', quitEvent())
      helpers.sweep(app)
      const held = quitEvent()
      helpers.emitApp('will-quit', held)
      assert.equal(held.prevented, true)
      await until(() => helpers.quits.length > 0)
      const log = calls(tree)
      assert.ok(log.indexOf('server stop') < log.indexOf('postgres fast shutdown'), log.join(', '))
      assert.ok(log.includes('postgres fast shutdown'))
      // Asked again once stopped, and let go.
      const last = quitEvent()
      helpers.emitApp('will-quit', last)
      assert.equal(last.prevented, false)
    } finally {
      await backend.stop()
      delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    }
  })

  test('an update quit stops the stack before the app is replaced', async () => {
    const tree = fakeTree()
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = tree
    const { main, backend } = helpers.loadShell({ edition: 'oss', platform: 'linux', settings: { stackMode: 'bundled', stackPort: 0 } })
    const updater = require(path.join(__dirname, '..', 'src', 'updater.js'))
    try {
      main.openLaunchWindows()
      const app = await until(appWindow)

      // Where the updater installs before it quits, Restart Now waits for the stop.
      let installedAfter = null
      const fake = { quitAndInstall: () => { installedAfter = calls(tree) } }
      await updater.restartToInstall(fake, () => backend.stop(), 'linux')
      assert.ok(installedAfter.includes('postgres fast shutdown'), 'installed before Postgres stopped')
      assert.ok(installedAfter.indexOf('server stop') < installedAfter.indexOf('postgres fast shutdown'))

      // A page that then keeps the app keeps a working one: the stack comes back.
      helpers.emitApp('before-quit', quitEvent())
      app.webContents.emit('will-prevent-unload', { preventDefault() {} })
      await until(() => backend.isUp())
      assert.equal(calls(tree).filter((line) => /^server \d/.test(line)).length, 2)

      // A Mac installs once this process has gone, so the quit's own drain stops it.
      let stoppedFirst = false
      await updater.restartToInstall({ quitAndInstall() {} }, () => { stoppedFirst = true }, 'darwin')
      assert.equal(stoppedFirst, false)
      helpers.emitApp('before-quit', quitEvent())
      helpers.sweep(app)
      const held = quitEvent()
      helpers.emitApp('will-quit', held)
      assert.equal(held.prevented, true)
      await until(() => helpers.quits.length > 0)
      assert.equal(calls(tree).filter((line) => line === 'postgres fast shutdown').length, 2)
    } finally {
      await backend.stop()
      delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    }
  })

  test('from the picker: run here, then a server, then here again, and the server\'s address is kept', async () => {
    const tree = fakeTree()
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = tree
    // A ready app registers the channels and opens the picker itself.
    const { store, backend } = helpers.loadShell({ edition: 'oss', ready: true })
    const invoke = (channel, picker, ...args) => helpers.ipcHandlers.get(channel)({ sender: picker.webContents }, ...args)
    const reopen = () => {
      helpers.appMenuItem('Run on This Computer or a Server…').click()
      return helpers.windows.at(-1)
    }
    try {
      let picker = await until(() => helpers.windows[0])
      const status = invoke('stack:status', picker)
      assert.deepEqual(Object.keys(status).sort(), ['available', 'view'])
      assert.equal(status.available, true)
      assert.equal(status.view.state, 'idle')
      // Refused from anywhere but this page.
      const stranger = { sender: {} }
      assert.equal(helpers.ipcHandlers.get('stack:status')(stranger), null)
      assert.deepEqual(await helpers.ipcHandlers.get('stack:use')(stranger), { ok: false })
      assert.deepEqual(helpers.ipcHandlers.get('server:use')(stranger, 'http://evil.example'), { ok: false })
      assert.equal(helpers.ipcHandlers.get('server:current')(stranger), null)
      // The help prompt is built from what main knows and goes to the clipboard.
      assert.equal(helpers.ipcHandlers.get('stack:copy-help-prompt')(stranger), false)
      helpers.copied.length = 0
      assert.equal(invoke('stack:copy-help-prompt', picker), true)
      assert.equal(helpers.copied.length, 1)
      assert.match(helpers.copied[0], new RegExp(`Source: ${REPO} at commit abc123\\.`))
      assert.ok(helpers.copied[0].includes(path.join('logs')))

      // Run here.
      const first = await invoke('stack:use', picker)
      assert.equal(first.ok, true)
      assert.equal(store.get('stackMode'), 'bundled')
      const app = appWindow()
      assert.equal(app.loaded.at(-1), first.origin)
      assert.equal(picker.destroyed, true)
      assert.equal(store.get('serverUrl'), null)

      // A server instead: the stack stops, its data stays.
      picker = reopen()
      assert.equal(invoke('server:current', picker), null)
      const switched = invoke('server:use', picker, 'http://stack.example:8080')
      assert.equal(switched.ok, true)
      assert.equal(store.get('stackMode'), null)
      assert.equal(store.get('serverUrl'), 'http://stack.example:8080')
      assert.equal(app.loaded.at(-1), 'http://stack.example:8080')
      await until(() => calls(tree).includes('postgres fast shutdown'))

      // And back. The server's address is still the one the form offers.
      picker = reopen()
      const again = await invoke('stack:use', picker)
      assert.equal(again.ok, true)
      assert.equal(app.loaded.at(-1), again.origin)
      assert.equal(store.get('serverUrl'), 'http://stack.example:8080')
      picker = reopen()
      assert.equal(invoke('server:current', picker), 'http://stack.example:8080')
      picker.close()
    } finally {
      await backend.stop()
      helpers.emitApp('will-quit', quitEvent())
      delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    }
  })

  test('switching to the stack keeps the app on its server until the stack is up', async () => {
    const tree = fakeTree()
    process.env.LANGALPHA_DESKTOP_BACKEND_TREE = tree
    const { backend, origins } = helpers.loadShell({ edition: 'oss', serverUrl: 'http://stack.example:8080' })
    try {
      control(tree, { failServer: true })
      const result = await backend.useBundled()
      assert.equal(result.ok, false)
      // A start that failed leaves the server the choice, and its pages the app's.
      assert.deepEqual(backend.choice(), { kind: 'server', origin: 'http://stack.example:8080' })
      assert.equal(origins.isApp('http://stack.example:8080/chat'), true)
      assert.equal(backend.wantsStack(), true, 'the failure is not offered')
    } finally {
      await backend.stop()
      delete process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    }
  })

  test('the menu offers the switch on a build that has a stack, and keeps its old item on one that does not', () => {
    const { buildMenu } = require(path.join(__dirname, '..', 'src', 'menu.js'))
    const items = (options) => buildMenu({ isSaas: false, ...options })[0].submenu.map((i) => i.label).filter(Boolean)
    assert.ok(items({}).includes('Change Server…'))
    assert.ok(items({ hasBundledStack: true }).includes('Run on This Computer or a Server…'))
    assert.ok(!items({ hasBundledStack: true }).includes('Reveal Logs'))
    assert.ok(items({ hasBundledStack: true, onRevealStackLogs: () => {} }).includes('Reveal Logs'))
  })
})

test('the shell reaches src/stack/ only where a stack exists, since the hosted package leaves it out', () => {
  const dir = path.join(__dirname, '..', 'src')
  const topLevel = fs.readdirSync(dir).filter((f) => f.endsWith('.js')).flatMap((file) => fs.readFileSync(path.join(dir, file), 'utf8')
    .split('\n').filter((line) => /^\S.*require\(['"]\.\/stack/.test(line)).map((line) => `${file}: ${line}`))
  // The one allowed: behind config.isSaas, which never requires it on a hosted build.
  assert.deepEqual(topLevel, ["main.js: const stack = config.isSaas ? null : require('./stack').createStack()"])
})
