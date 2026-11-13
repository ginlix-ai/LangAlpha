'use strict'

// Menu commands (commands.js): what the shell's menus ask the main window's
// page to do, enabled by what that page declared.

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')

const { loadShell, cleanup } = require('./helpers')

const commands = require('../src/commands.js')

after(cleanup)

describe('menu commands', () => {
  test('only known commands are declared', () => {
    assert.deepEqual([...commands.sanitize(['new-thread', 'rm -rf', 7, 'open', 'new-thread'])], ['new-thread', 'open'])
    assert.equal(commands.sanitize('new-thread').size, 0)
  })

  test('an item is enabled by the main window\'s declaration, or by there being no main window', () => {
    const declared = new Set(['new-thread'])
    assert.equal(commands.isEnabled('new-thread', { hasMainWindow: true, declared }), true)
    assert.equal(commands.isEnabled('go-orders', { hasMainWindow: true, declared }), false)
    assert.equal(commands.isEnabled('go-orders', { hasMainWindow: false, declared: new Set() }), true)
  })

  test('a second launch carries a command, never an open', () => {
    assert.equal(commands.commandFromArgv(['C:\\LangAlpha.exe', '--command=new-thread']), 'new-thread')
    assert.equal(commands.commandFromArgv(['x', '--command=open']), null)
    assert.equal(commands.commandFromArgv(['x', '--command=format-disk']), null)
    assert.equal(commands.commandFromArgv(undefined), null)
  })

  function fakeMain() {
    const sent = []
    const win = { isDestroyed: () => false, webContents: { send: (channel, payload) => sent.push([channel, payload]) } }
    return { win, sent }
  }

  test('a declared command is sent; an undeclared one waits for the page to declare it', () => {
    const { win, sent } = fakeMain()
    let t = 0
    const c = commands.createCommands({
      getMainWindow: () => win, showMain: () => win, loadInMain() {}, appUrl: () => null, syncMenu() {}, now: () => t,
    })
    c.declare(['toggle-sidebar'])
    c.deliver({ command: 'toggle-sidebar' })
    assert.deepEqual(sent, [['shell:menu-command', { command: 'toggle-sidebar' }]])
    c.deliver({ command: 'new-thread' })
    assert.equal(sent.length, 1)
    t = 1000
    c.declare(['toggle-sidebar', 'new-thread'])
    assert.deepEqual(sent[1], ['shell:menu-command', { command: 'new-thread' }])
    c.declare(['toggle-sidebar', 'new-thread'])
    assert.equal(sent.length, 2, 'the waiting command went twice')
  })

  test('a waiting command expires rather than firing long after it was asked for', () => {
    const { win, sent } = fakeMain()
    let t = 0
    const c = commands.createCommands({
      getMainWindow: () => win, showMain: () => win, loadInMain() {}, appUrl: () => null, syncMenu() {}, now: () => t,
    })
    c.deliver({ command: 'new-thread' })
    t = commands.PENDING_MS + 1
    c.declare(['new-thread'])
    assert.equal(sent.length, 0)
  })

  test('open goes to a page that takes it, and is loaded into one that does not', () => {
    const { win, sent } = fakeMain()
    const loaded = []
    const c = commands.createCommands({
      getMainWindow: () => win,
      showMain: () => win,
      loadInMain: (url) => loaded.push(url),
      appUrl: (p) => (p.startsWith('/') ? `https://app.example.com${p}` : null),
      syncMenu() {},
    })
    c.deliver({ command: 'open', path: '/chat/t/1' })
    assert.deepEqual(loaded, ['https://app.example.com/chat/t/1'])
    c.declare(['open'])
    c.deliver({ command: 'open', path: '/chat/t/2' })
    assert.deepEqual(sent, [['shell:menu-command', { command: 'open', path: '/chat/t/2' }]])
    c.deliver({ command: 'open', path: 'https://evil.example.com/' })
    assert.equal(sent.length, 1)
    assert.equal(loaded.length, 1)
  })

  test('with no main window, open creates it at the path', () => {
    const made = []
    const c = commands.createCommands({
      getMainWindow: () => null,
      showMain: (url) => made.push(url),
      loadInMain() {},
      appUrl: (p) => `https://app.example.com${p}`,
      syncMenu() {},
    })
    c.deliver({ command: 'open', path: '/chat/t/1' })
    assert.deepEqual(made, ['https://app.example.com/chat/t/1'])
  })

  test('a new document or a new main window has declared nothing, and the menu hears it', () => {
    const win = Object.assign(new EventEmitter(), { destroyed: false, isDestroyed() { return this.destroyed } })
    win.webContents = Object.assign(new EventEmitter(), { send() {} })
    let current = win
    let enabled = null
    const c = commands.createCommands({
      getMainWindow: () => current,
      showMain: () => current,
      loadInMain() {},
      appUrl: () => null,
      syncMenu: (isEnabled) => { enabled = commands.COMMANDS.filter(isEnabled) },
    })
    c.attachMainWindow(win)
    assert.deepEqual(enabled, [])
    c.declare(['new-thread'])
    assert.deepEqual(enabled, ['new-thread'])
    win.webContents.emit('did-navigate', {}, 'https://app.example.com/', 200)
    assert.deepEqual(enabled, [], 'a new document kept the old one\'s declaration')
    c.declare(['new-thread'])
    // No main window: every command is enabled, since choosing one opens it.
    win.destroyed = true
    current = null
    win.emit('closed')
    assert.deepEqual(enabled, commands.COMMANDS)
  })

  test('an app path stays on the app origin', () => {
    const { policy } = loadShell({ edition: 'saas' })
    assert.equal(policy.appPathUrl('/chat/t/abc?x=1#y'), 'https://app.example.com/chat/t/abc?x=1#y')
    for (const bad of ['//evil.example.com/x', '/\\evil.example.com/x', 'https://evil.example.com/', 'chat', '', null, 3]) {
      assert.equal(policy.appPathUrl(bad), null, String(bad))
    }
  })
})
