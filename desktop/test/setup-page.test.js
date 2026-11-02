'use strict'

/**
 * The setup page finds its controls by id, and `getElementById` returns the first
 * match. A stage once shared the id `connect` with the Connect button ahead of
 * it, so the connect handler sat on the whole form: clicking into the address
 * field connected to the default address before anything could be typed.
 */

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')

const SETUP = path.join(__dirname, '..', 'setup')
const html = fs.readFileSync(path.join(SETUP, 'setup.html'), 'utf8')
const script = fs.readFileSync(path.join(SETUP, 'setup.js'), 'utf8')

test('every id on the setup page is unique', () => {
  const ids = [...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1])
  const repeated = ids.filter((id, i) => ids.indexOf(id) !== i)
  assert.deepEqual(repeated, [])
})

test('the ids the script looks up are on the page', () => {
  const ids = new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]))
  const looked = [...script.matchAll(/getElementById\('([^']+)'\)/g)].map((m) => m[1])
  const stages = script.match(/const stages = \[([^\]]+)\]/)[1].match(/'([^']+)'/g).map((s) => s.slice(1, -1))
  assert.deepEqual([...looked, ...stages].filter((id) => !ids.has(id)), [])
})

test('the Connect button is a button', () => {
  assert.match(html, /<button id="connect"[^>]*>Connect<\/button>/)
})

/**
 * The page run against a stand-in DOM and bridge: which stage it opens on, and
 * which stack changes move it.
 */
function runPage(status) {
  const elements = new Map()
  const element = (id) => {
    if (!elements.has(id)) {
      elements.set(id, {
        id, hidden: id === 'back', dataset: {}, listeners: {}, textContent: '', value: '',
        addEventListener(type, fn) { this.listeners[type] = fn },
        replaceChildren() {}, focus() {}, select() {},
      })
    }
    return elements.get(id)
  }
  let onChange = null
  const bridge = {
    stackStatus: () => Promise.resolve(status),
    current: () => Promise.resolve(null),
    onStackChange: (fn) => { onChange = fn },
  }
  vm.runInNewContext(script, {
    document: { getElementById: element, createElement: () => ({ dataset: {} }) },
    window: { langalphaSetup: bridge },
    setTimeout, clearTimeout,
  })
  const shown = () => ['choose', 'server', 'progress'].filter((id) => !element(id).hidden)
  return { element, shown, change: (view) => onChange(view) }
}

const settle = () => new Promise((resolve) => setImmediate(resolve))
const view = (state) => ({ state, steps: [], failure: state === 'failed' ? { message: 'boom' } : null })

test('the page opens on the stage its status asks for', async () => {
  const cases = [
    [null, 'server'],
    [{ available: false, view: null }, 'server'],
    [{ available: true, view: view('idle') }, 'choose'],
    [{ available: true, view: view('starting') }, 'progress'],
    [{ available: true, view: view('failed') }, 'progress'],
  ]
  for (const [status, stage] of cases) {
    const page = runPage(status)
    await settle()
    assert.deepEqual(page.shown(), [stage], JSON.stringify(status))
  }
})

test('a stack change moves the page into progress, and only there', async () => {
  const page = runPage({ available: true, view: view('idle') })
  await settle()
  page.element('use-server').listeners.click()
  assert.deepEqual(page.shown(), ['server'])
  // A stop must not pull the form out from under the user.
  page.change(view('stopped'))
  assert.deepEqual(page.shown(), ['server'])
  page.change(view('failed'))
  assert.deepEqual(page.shown(), ['progress'])
  assert.equal(page.element('failure').hidden, false)
  assert.equal(page.element('failure-message').textContent, 'boom')
  // Both ways to the form are the same way.
  page.element('failed-server').listeners.click()
  assert.deepEqual(page.shown(), ['server'])
  assert.equal(page.element('back').hidden, false)
})
