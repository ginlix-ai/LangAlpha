'use strict'

/** A stack child, its guard and its port, as real processes with real signals. */

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const { execFileSync, spawn } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')

const { SRC, node, tempRoot, until, alive, cleanup } = require('./stackFixture')
const { startChild, canBind, stablePort } = require(path.join(SRC, 'children.js'))
const { createLog } = require(path.join(SRC, 'logs.js'))

after(cleanup)

describe('stopping a child', () => {
  test('one that ignores the signal is killed at its deadline', async () => {
    const root = tempRoot()
    const log = createLog(root, 'stubborn')
    const child = startChild({
      name: 'stubborn',
      command: 'sh',
      args: ['-c', 'trap "" TERM; while :; do sleep 0.1; done'],
      env: { PATH: process.env.PATH },
      log,
    })
    await new Promise((r) => setTimeout(r, 150))
    const started = Date.now()
    await child.stop({ deadlineMs: 300 })
    assert.ok(Date.now() - started < 3000)
    assert.equal(child.signal, 'SIGKILL')
    log.close()
  })

  test('a child that ends takes its guard and the guard\'s pipe with it', async () => {
    const root = tempRoot()
    const log = createLog(root, 'loop')
    const childrenOf = (pid, ...filter) => {
      try {
        return execFileSync('pgrep', ['-P', String(pid), ...filter], { encoding: 'utf8' }).trim().split('\n').filter(Boolean).map(Number)
      } catch {
        return []
      }
    }
    const before = new Set(childrenOf(process.pid, '-f', 'langalpha-guard'))
    const cats = []
    for (let round = 0; round < 5; round++) {
      const child = startChild({ name: 'loop', command: 'sleep', args: ['60'], env: { PATH: process.env.PATH }, log, graceSeconds: 1 })
      const guards = await until(() => {
        const found = childrenOf(process.pid, '-f', `langalpha-guard ${child.child.pid} `)
        return found.length ? found : null
      })
      cats.push(...await until(() => {
        const found = childrenOf(guards[0], '-x', 'cat')
        return found.length ? found : null
      }))
      await child.stop()
    }
    assert.equal(cats.length, 5)
    await until(() => cats.every((pid) => !alive(pid)), 3000)
    const left = childrenOf(process.pid, '-f', 'langalpha-guard').filter((pid) => !before.has(pid))
    assert.deepEqual(left, [])
    log.close()
  })

  test('the children go when the parent is killed outright', async () => {
    const root = tempRoot()
    const script = path.join(root, 'parent.js')
    fs.writeFileSync(script, `
      const { startChild } = require(${JSON.stringify(path.join(SRC, 'children.js'))});
      const { createLog } = require(${JSON.stringify(path.join(SRC, 'logs.js'))});
      const child = startChild({ name: 'c', command: 'sleep', args: ['60'], env: { PATH: process.env.PATH }, log: createLog(${JSON.stringify(root)}, 'c') });
      console.log(child.child.pid);
      setInterval(() => {}, 1000);
    `)
    const parent = spawn(node, [script], { stdio: ['ignore', 'pipe', 'inherit'] })
    const childPid = await new Promise((resolve) => parent.stdout.once('data', (d) => resolve(Number(String(d).trim()))))
    assert.ok(alive(childPid))
    parent.kill('SIGKILL')
    await until(() => !alive(childPid), 5000)
  })
})

describe('a child\'s output', () => {
  test('keeps a character whose bytes arrive in two writes', async () => {
    const root = tempRoot()
    const log = createLog(root, 'split')
    const child = startChild({
      name: 'split',
      command: 'sh',
      // The euro sign's three bytes, two now and one after a pause.
      args: ['-c', "printf '\\342\\202'; sleep 0.3; printf '\\254\\n'"],
      env: { PATH: process.env.PATH },
      log,
    })
    await child.done
    log.close()
    assert.equal(log.tail(1), '\u20ac')
    assert.equal(fs.readFileSync(log.file, 'utf8'), '\u20ac\n')
  })

  test('is all in the log by the time the child is done, its last line included', async () => {
    const root = tempRoot()
    const log = createLog(root, 'loud')
    const child = startChild({
      name: 'loud',
      command: 'sh',
      // A long run of output, then the line a caller judges the failure by.
      args: ['-c', 'i=0; while [ $i -lt 4000 ]; do echo "line $i of noise to fill the pipe" >&2; i=$((i+1)); done; echo "Address already in use" >&2; exit 1'],
      env: { PATH: process.env.PATH },
      log,
    })
    await child.done
    assert.equal(child.code, 1)
    assert.equal(log.tail(1), 'Address already in use')
    log.close()
  })

  test('a process the child left holding its output does not hold up its end', async () => {
    const root = tempRoot()
    const log = createLog(root, 'leaver')
    const started = Date.now()
    const child = startChild({
      name: 'leaver',
      command: 'sh',
      args: ['-c', 'sleep 5 & echo gone'],
      env: { PATH: process.env.PATH },
      log,
    })
    await child.done
    assert.ok(Date.now() - started < 3000)
    assert.equal(child.code, 0)
    log.close()
  })
})

describe('the backend port', () => {
  test('a free saved port is kept, and a new one avoids the ephemeral ranges', async () => {
    const free = await stablePort(null, { random: () => 0.5 })
    assert.equal(free, 25000)
    assert.equal(await stablePort(free), free)
    assert.equal(await canBind(free), true)
  })
})
