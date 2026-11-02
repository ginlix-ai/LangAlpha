'use strict'

/** The stack's per-process logs: rotated by size, private, with a tail in memory. */

const { test, describe, after, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

const { SRC, tempRoot, cleanup } = require('./stackFixture')
const { createLog } = require(path.join(SRC, 'logs.js'))

after(cleanup)

describe('logs', () => {
  let dir
  beforeEach(() => { dir = tempRoot() })

  test('rotate at their size and keep a bounded number of files', () => {
    const log = createLog(dir, 'x', { maxBytes: 100, keep: 2 })
    for (let i = 0; i < 50; i++) log.write(`line ${i} of some length\n`)
    log.close()
    assert.deepEqual(fs.readdirSync(dir).sort(), ['x.log', 'x.log.1', 'x.log.2'])
    assert.ok(fs.statSync(path.join(dir, 'x.log')).size <= 100)
  })

  test('deleted while running, rotate into a new file and never into another one', () => {
    const log = createLog(dir, 'x', { maxBytes: 100 })
    log.write('before\n')
    fs.rmSync(log.file)
    log.write(`${'a'.repeat(120)}\n`)
    // Opened now, it takes the lowest free number, the one a stale fd would still name.
    const other = path.join(dir, 'other')
    const otherFd = fs.openSync(other, 'a')
    log.write('after\n')
    fs.closeSync(otherFd)
    log.close()
    assert.equal(fs.readFileSync(other, 'utf8'), '')
    assert.match(fs.readFileSync(log.file, 'utf8'), /after\n$/)
  })

  test('are readable by their owner only', () => {
    const log = createLog(dir, 'x')
    log.write('hello\n')
    log.close()
    assert.equal(fs.statSync(path.join(dir, 'x.log')).mode & 0o777, 0o600)
  })

  test('keep a tail in memory, partial last line included', () => {
    const log = createLog(dir, 'x')
    log.write('one\ntwo\nthr')
    log.write('ee')
    assert.equal(log.tail(2), 'two\nthree')
    log.close()
  })
})
