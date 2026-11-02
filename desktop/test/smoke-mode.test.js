'use strict'

// The release launch check's entry in main.js. A build with no stack has
// nothing to check, and must say so and exit rather than open the server picker
// and wait for a person who never comes.

const { test, after } = require('node:test')
const assert = require('node:assert/strict')

const { cleanup, loadShell, electronStub, windows } = require('./helpers')

after(cleanup)

test('smoke mode on a build with no stack exits non-zero without opening a window', async () => {
  const codes = []
  const errors = []
  const realExit = electronStub.app.exit
  const realError = console.error
  process.env.LANGALPHA_DESKTOP_SMOKE = '1'
  electronStub.app.exit = (code) => { codes.push(code) }
  console.error = (...args) => { errors.push(args.join(' ')) }
  try {
    const { backend } = loadShell({ edition: 'oss', ready: true })
    assert.equal(backend.hasStack(), false)
    for (let i = 0; i < 50 && codes.length === 0; i += 1) await new Promise((resolve) => setTimeout(resolve, 5))
    assert.deepEqual(codes, [2])
    assert.ok(errors.some((line) => line.includes('no bundled stack')), errors.join('\n'))
    assert.equal(windows.length, 0)
  } finally {
    delete process.env.LANGALPHA_DESKTOP_SMOKE
    electronStub.app.exit = realExit
    console.error = realError
  }
})
