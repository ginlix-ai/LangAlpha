'use strict'

/** The app's own requests, and only those, carry the bundled server's owner secret. */

const { test, describe } = require('node:test')
const assert = require('node:assert/strict')
const path = require('node:path')

const { SRC } = require('./stackFixture')
const { OWNER_HEADER, withOwner, proveOwner } = require(path.join(SRC, 'owner.js'))
const plan = require(path.join(SRC, 'plan.js'))

const stack = { origin: 'http://127.0.0.1:24454', ownerSecret: 's3cret' }

describe('the owner header', () => {
  test('goes on the server\'s pages, calls and websockets', () => {
    for (const url of ['http://127.0.0.1:24454/', 'http://127.0.0.1:24454/api/v1/workspaces', 'ws://127.0.0.1:24454/ws/v1/x']) {
      assert.deepEqual(withOwner(url, { Accept: '*/*' }, stack), { Accept: '*/*', [OWNER_HEADER]: 's3cret' })
    }
  })

  test('never leaves for another port, host or scheme', () => {
    for (const url of ['http://127.0.0.1:18732/', 'http://localhost:24454/', 'https://127.0.0.1:24454/', 'http://127.0.0.1/', 'not a url']) {
      assert.deepEqual(withOwner(url, { Accept: '*/*' }, stack), { Accept: '*/*' })
    }
  })

  test('goes nowhere while the stack is not up', () => {
    assert.deepEqual(withOwner('http://127.0.0.1:24454/', {}, { origin: null, ownerSecret: 's3cret' }), {})
  })

  test('is added once per session, and not at all without a stack', () => {
    const registered = []
    const session = { webRequest: { onBeforeSendHeaders: (filter, listener) => registered.push({ filter, listener }) } }
    proveOwner(session, null)
    proveOwner(session, stack)
    proveOwner(session, stack)
    assert.equal(registered.length, 1)
    let sent = null
    registered[0].listener({ url: 'http://127.0.0.1:24454/api', requestHeaders: {} }, (answer) => { sent = answer })
    assert.deepEqual(sent, { requestHeaders: { [OWNER_HEADER]: 's3cret' } })
  })

  test('the server is handed the secret and a service token for its calls to itself', () => {
    const env = plan.backendEnv({ layout: plan.layoutOf('/t'), from: {}, db: { host: 'h', port: 1, user: 'u', password: 'p', name: 'n' }, redisUrl: 'r', byokKey: 'k', ownerSecret: 'o', serviceToken: 't', port: 2, pythonMinor: '3.13' })
    assert.equal(env.LANGALPHA_OWNER_SECRET, 'o')
    assert.equal(env.INTERNAL_SERVICE_TOKEN, 't')
  })
})
