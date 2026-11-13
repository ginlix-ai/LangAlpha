'use strict'

/**
 * Every IPC channel is spelled twice: once where a renderer invokes it and once
 * where the main process handles it. Nothing checks that the two spellings
 * match -- a typo on either side is a bridge method that rejects with "no
 * handler registered" the first time a user reaches for it, in a packaged
 * build, with no signal until then.
 *
 * There are two preloads, and which one carries a channel is a security
 * boundary rather than an arrangement: the remote-page preload is handed to
 * whatever the loaded web app turns out to be, so the channels that repoint the
 * whole app stay on the local setup page. That split is invisible to every
 * other test here, and moving a line between the two files would not break one.
 */

const { test, describe } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

const ROOT = path.join(__dirname, '..')
const SRC = path.join(ROOT, 'src')

function channels(source, pattern) {
  return new Set(Array.from(source.matchAll(pattern), (m) => m[1]))
}
function invokedIn(file) {
  return channels(fs.readFileSync(path.join(ROOT, file), 'utf8'), /ipcRenderer\.invoke\(\s*'([^']+)'/g)
}

function read(file) {
  return fs.readFileSync(path.join(ROOT, file), 'utf8')
}

// What the loaded web app can reach, and what only the local setup page can.
const remote = invokedIn('src/preload.js')
const setup = invokedIn('setup/setup-preload.js')
const invoked = new Set([...remote, ...setup])

// Handlers live in whichever module owns the feature, so gather them from the
// whole tree rather than naming files this test would then have to be
// remembered to update. Registered on `ipcMain` itself or on a gated wrapper
// of it (`gatedIpc` in sender.js, which answers only some windows), so the
// pattern is the method and a channel name, whatever it is called on.
const handled = new Set()
// The other two directions: fire-and-forget messages a page sends (and the
// synchronous ones), heard with `.on`; and what main pushes to a page with
// `webContents.send`, heard with `ipcRenderer.on` through `subscribe`.
const heard = new Set()
const pushed = new Set()
const CHANNEL = "'([a-z]+:[a-z-]+)'"
for (const entry of fs.readdirSync(SRC)) {
  if (!entry.endsWith('.js') || entry === 'preload.js') continue
  const source = fs.readFileSync(path.join(SRC, entry), 'utf8')
  for (const c of channels(source, new RegExp(`\\.handle\\(\\s*${CHANNEL}`, 'g'))) handled.add(c)
  for (const c of channels(source, new RegExp(`\\.on\\(\\s*${CHANNEL}`, 'g'))) heard.add(c)
  for (const c of channels(source, /webContents\.send\(\s*'([^']+)'/g)) pushed.add(c)
}
const preloadSource = read('src/preload.js')
const sent = channels(preloadSource, /ipcRenderer\.send(?:Sync)?\(\s*'([^']+)'/g)
const subscribed = channels(preloadSource, /subscribe\(\s*'([^']+)'/g)

describe('ipc channels', () => {
  test('the source of both halves is still shaped the way this reads it', () => {
    // A refactor to a channel table or a wrapper helper would leave these sets
    // empty and every assertion below vacuously true. The floors are today's
    // counts on purpose: they only ever rise, and deliberately removing a
    // channel should land whoever did it in this file.
    assert.ok(remote.size >= 11, `found only ${remote.size} channels in the remote preload`)
    assert.ok(setup.size >= 3, `found only ${setup.size} channels in the setup preload`)
    assert.ok(handled.size >= 14, `found only ${handled.size} handled channels`)
  })

  test('every message a page sends is heard, and every one main hears is sent', () => {
    assert.ok(sent.size >= 8, `found only ${sent.size} sent channels in the remote preload`)
    assert.ok(heard.size >= 8, `found only ${heard.size} heard channels`)
    assert.ok(sent.has('shell:set-tray-items') && heard.has('shell:set-tray-items'), 'the tray items channel is not wired')
    assert.deepEqual([...sent].filter((c) => !heard.has(c)), [], 'sent with no ipcMain.on')
    assert.deepEqual([...heard].filter((c) => !sent.has(c)), [], 'heard but never sent')
  })

  test('every event main pushes to a page has a listener, and the other way round', () => {
    for (const c of ['shell:menu-command', 'shell:notification-action', 'shell:desktop-settings', 'shell:tray-refresh']) {
      assert.ok(pushed.has(c), `${c} is never sent by main`)
      assert.ok(subscribed.has(c), `${c} has no subscriber in the preload`)
    }
    assert.deepEqual([...subscribed].filter((c) => !pushed.has(c)), [], 'subscribed but never pushed')
  })

  test('every channel a renderer invokes has a handler', () => {
    const missing = [...invoked].filter((c) => !handled.has(c))
    assert.deepEqual(missing, [], `invoked with no ipcMain.handle: ${missing}`)
  })

  test('every handler is reachable from one of the preloads', () => {
    // The other direction is dead code rather than a crash, but a handler no
    // preload names is either a typo or a feature that shipped half-wired.
    const orphans = [...handled].filter((c) => !invoked.has(c))
    assert.deepEqual(orphans, [], `handled but never invoked: ${orphans}`)
  })

  test('the channels that repoint the app stay off the remote page', () => {
    // `server:use` swaps the origin the whole app runs against. It is reachable
    // from the local setup page and from nowhere the loaded page can see.
    for (const c of setup) {
      assert.ok(!remote.has(c), `${c} is exposed to the remote page`)
    }
    assert.ok(setup.has('server:use'), 'the setup bridge no longer carries server:use')
  })

  test('the three the OAuth bridge depends on are among them', () => {
    // Named explicitly because the web app hard-codes these method names behind
    // feature detection: a missing one degrades to "no listener" and sends every
    // desktop connect down the hosted path, silently.
    for (const c of ['shell:mcp-oauth-begin', 'shell:mcp-oauth-bind', 'shell:mcp-oauth-cancel']) {
      assert.ok(remote.has(c), `${c} is not invoked by the remote preload`)
      assert.ok(handled.has(c), `${c} has no handler`)
    }
  })

  test('so are the three remote control rests on', () => {
    // The web feature-detects these by method name and offers "This Mac" only
    // when they answer, so a channel spelled differently on one side is a
    // feature that silently never appears.
    for (const c of ['shell:remote-control-status', 'shell:remote-control-enable', 'shell:remote-control-disable']) {
      assert.ok(remote.has(c), `${c} is not invoked by the remote preload`)
      assert.ok(handled.has(c), `${c} has no handler`)
    }
  })
})
