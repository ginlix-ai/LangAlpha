'use strict'

/** The supervisor against the stand-in tree in `stackFixture`: start, failures, staying up, stopping. */

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const { spawn } = require('node:child_process')
const fs = require('node:fs')
const net = require('node:net')
const path = require('node:path')

const { SRC, tempRoot, program, fakeTree, control, calls, steps, supervisor, until, alive, cleanup } = require('./stackFixture')
const plan = require(path.join(SRC, 'plan.js'))

after(cleanup)

describe('starting', () => {
  test('a first run creates the data folder, then starts everything in order', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const sup = supervisor(tree, userData)
    const seen = []
    sup.on('change', (view) => {
      const active = view.steps.find((s) => s.status === 'running')
      if (active && seen.at(-1) !== active.id) seen.push(active.id)
    })
    const result = await sup.start()
    assert.equal(result.ok, true)
    assert.deepEqual(seen, ['prepare', 'init', 'database', 'cache', 'migrate', 'server'])
    assert.deepEqual(calls(tree).map(steps), ['initdb', 'postgres', 'valkey', 'migrate', 'server'])
    assert.match(sup.origin, /^http:\/\/127\.0\.0\.1:\d+$/)
    await sup.stop()
  })

  test('a later launch goes straight to starting, keeps its keys and its port', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData, { port: 23456 })
    await first.start()
    const secrets = fs.readFileSync(path.join(userData, 'stack', 'secrets.json'), 'utf8')
    assert.equal(fs.statSync(path.join(userData, 'stack', 'secrets.json')).mode & 0o777, 0o600)
    await first.stop()

    const second = supervisor(tree, userData, { port: 23456 })
    const result = await second.start()
    assert.equal(result.origin, 'http://127.0.0.1:23456')
    assert.equal(calls(tree).filter((c) => c === 'initdb').length, 1)
    assert.equal(fs.readFileSync(path.join(userData, 'stack', 'secrets.json'), 'utf8'), secrets)
    await second.stop()
  })

  test('the backend gets exactly the environment built for it', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const sup = supervisor(tree, userData, { env: { DAYTONA_API_KEY: 'from-the-shell', OPENAI_API_KEY: 'from-the-shell' } })
    await sup.start()
    const env = JSON.parse(fs.readFileSync(path.join(tree, 'server-env.json'), 'utf8'))
    assert.equal(env.DAYTONA_API_KEY, undefined)
    assert.equal(env.OPENAI_API_KEY, undefined)
    assert.equal(env.HOST_MODE, 'oss')
    assert.equal(env.LANGALPHA_LOCAL_COMPUTER, '1')
    assert.equal(env.LANGALPHA_DESKTOP_APP, '1')
    assert.equal(env.PYTHON_DOTENV_DISABLED, '1')
    assert.equal(env.LANGSMITH_TRACING, 'false')
    assert.equal(env.PUBLIC_APP_URL, sup.origin)
    assert.equal(env.SERVER_BASE_URL, sup.origin)
    // The hand-offs and report-backs that used to reach whatever listened on 8000.
    for (const name of ['GINLIXFLOW_BASE_URL', 'PDF_RENDER_INTERNAL_BASE', 'EGRESS_RELAY_LOOPBACK_URL']) {
      assert.equal(env[name], sup.origin, name)
    }
    const provided = Object.keys(plan.backendEnv({ layout: plan.layoutOf(tree), from: {}, db: { host: 'h', port: 1, user: 'u', password: 'p', name: 'n' }, redisUrl: 'r', byokKey: 'k', port: 2, pythonMinor: '3.13' }))
    assert.ok(provided.every((name) => env[name] !== undefined && !/:8000\b/.test(env[name])), 'no value points at the default port')
    assert.equal(env.DB_HOST, '127.0.0.1')
    assert.match(env.BYOK_ENCRYPTION_KEY, /^[0-9a-f]{64}$/)
    assert.match(env.REDIS_URL, /^redis:\/\/:[0-9a-f]+@127\.0\.0\.1:\d+\/0$/)
    await sup.stop()
  })

  test('a database named by URL starts no Postgres', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot(), { env: { LANGALPHA_DESKTOP_PG_URL: 'postgres://u:p%40ss@127.0.0.1:5999/db1' } })
    const result = await sup.start()
    assert.equal(result.ok, true)
    assert.deepEqual(calls(tree).map(steps), ['valkey', 'check', 'migrate', 'server'])
    const env = JSON.parse(fs.readFileSync(path.join(tree, 'server-env.json'), 'utf8'))
    assert.equal(env.DB_PASSWORD, 'p@ss')
    assert.equal(env.DB_PORT, '5999')
    assert.equal(env.DB_NAME, 'db1')
    await sup.stop()
  })

  test('a server that answers 503 is up: that is a remote MCP server being down', async () => {
    const tree = fakeTree()
    control(tree, { health: 503 })
    const sup = supervisor(tree, tempRoot())
    assert.equal((await sup.start()).ok, true)
    await sup.stop()
  })
})

describe('a data folder from an earlier launch', () => {
  test('a folder from another Postgres major is refused and left alone', async () => {
    const tree = fakeTree({ major: 18 })
    const userData = tempRoot()
    const data = path.join(userData, 'stack', 'pg', 'data')
    fs.mkdirSync(data, { recursive: true })
    fs.writeFileSync(path.join(data, 'PG_VERSION'), '17\n')
    fs.writeFileSync(path.join(data, 'precious'), 'yes')
    fs.writeFileSync(path.join(userData, 'stack', 'secrets.json'), JSON.stringify({ byokKey: 'k', pgPassword: 'p', valkeyPassword: 'v' }))
    const sup = supervisor(tree, userData)
    const result = await sup.start()
    assert.equal(result.ok, false)
    assert.equal(result.failure.step, 'database')
    assert.match(result.failure.message, /PostgreSQL 17/)
    assert.match(result.failure.dataNote, /not changed/)
    assert.deepEqual(calls(tree), [])
    assert.equal(fs.readFileSync(path.join(data, 'precious'), 'utf8'), 'yes')
    await sup.stop()
  })

  test('an interrupted initdb goes away and never looks like a data folder', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const half = path.join(userData, 'stack', 'pg', 'data.init')
    fs.mkdirSync(half, { recursive: true })
    fs.writeFileSync(path.join(half, 'junk'), 'x')
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, true)
    assert.equal(fs.existsSync(path.join(userData, 'stack', 'pg', 'data', 'junk')), false)
    assert.equal(fs.existsSync(half), false)
    await sup.stop()
  })

  test('a failed initdb leaves no data folder, and says so with its log', async () => {
    const tree = fakeTree()
    control(tree, { failInitdb: true })
    const userData = tempRoot()
    const result = await supervisor(tree, userData).start()
    assert.equal(result.failure.step, 'init')
    assert.match(result.failure.tail, /initdb: refused/)
    assert.equal(fs.existsSync(path.join(userData, 'stack', 'pg', 'data')), false)
  })

  test('a postmaster.pid whose number now belongs to something else is cleared', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    // This test's own pid: alive, and not a postgres on this data folder.
    const pidFile = path.join(userData, 'stack', 'pg', 'data', 'postmaster.pid')
    fs.writeFileSync(pidFile, `${process.pid}\n/elsewhere\n\n1\n\n\n\nready\n`)
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, true)
    assert.ok(alive(process.pid))
    await sup.stop()
  })

  test('a postmaster.pid whose process is gone is left for postgres to judge', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    const gone = spawn(process.execPath, ['-e', ''])
    await new Promise((resolve) => gone.once('exit', resolve))
    const data = path.join(userData, 'stack', 'pg', 'data')
    fs.writeFileSync(path.join(data, 'postmaster.pid'), `${gone.pid}\n${data}\n\n1\n\n\n\nready\n`)
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, true)
    assert.doesNotMatch(fs.readFileSync(path.join(userData, 'logs', 'postgres.log'), 'utf8'), /removing a postmaster\.pid/)
    await sup.stop()
  })

  test('a database with no secrets file is refused, and no new passwords are made', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    const secrets = path.join(userData, 'stack', 'secrets.json')
    fs.rmSync(secrets)
    const result = await supervisor(tree, userData).start()
    assert.equal(result.ok, false)
    assert.equal(result.failure.step, 'prepare')
    assert.match(result.failure.message, /is missing\. Your saved keys cannot be read until it is restored/)
    assert.match(result.failure.dataNote, /not changed/)
    assert.equal(fs.existsSync(secrets), false)
  })

  test('keys from an earlier install gain a relay secret once and keep the rest', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const file = path.join(userData, 'stack', 'secrets.json')
    fs.mkdirSync(path.dirname(file), { recursive: true })
    fs.writeFileSync(file, JSON.stringify({ byokKey: 'k', pgPassword: 'p', valkeyPassword: 'v' }))
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    const saved = JSON.parse(fs.readFileSync(file, 'utf8'))
    assert.deepEqual({ ...saved, relaySecret: undefined }, { byokKey: 'k', pgPassword: 'p', valkeyPassword: 'v', relaySecret: undefined })
    assert.match(saved.relaySecret, /^[0-9a-f]{64}$/)
    assert.equal(fs.statSync(file).mode & 0o777, 0o600)
    const second = supervisor(tree, userData)
    await second.start()
    await second.stop()
    assert.deepEqual(JSON.parse(fs.readFileSync(file, 'utf8')), saved)
  })

  test('a logs folder that cannot be made still ends the start in a failure it shows', async () => {
    const userData = tempRoot()
    fs.writeFileSync(path.join(userData, 'logs'), 'a file where the folder goes')
    const result = await supervisor(fakeTree(), userData).start()
    assert.equal(result.ok, false)
    assert.equal(result.failure.step, 'prepare')
    assert.equal(result.failure.log, null)
  })

  test('our postgres still running after the wait keeps its lock, and the step says so', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    const data = path.join(userData, 'stack', 'pg', 'data')
    // A postgres on this very folder that takes no notice of a fast shutdown.
    const stubborn = path.join(tree, 'stubborn', 'bin', 'postgres')
    program(stubborn, `process.on('SIGINT',()=>{});process.stdout.write('up\\n');setInterval(()=>{},1000);`)
    // Run from its data folder, as a postmaster does.
    const held = spawn(stubborn, ['-D', data], { cwd: data, stdio: ['ignore', 'pipe', 'ignore'] })
    await new Promise((resolve) => held.stdout.once('data', resolve))
    const pidFile = path.join(data, 'postmaster.pid')
    fs.writeFileSync(pidFile, `${held.pid}\n${data}\n\n1\n\n\n\nready\n`)
    try {
      const result = await supervisor(tree, userData, { lockWaitTries: 2 }).start()
      assert.equal(result.ok, false)
      assert.equal(result.failure.step, 'database')
      assert.match(result.failure.message, new RegExp(`\\(${held.pid}\\) is still running`))
      assert.equal(fs.existsSync(pidFile), true)
      assert.ok(alive(held.pid))
    } finally {
      held.kill('SIGKILL')
    }
  })

  test('an upgrade that was cut off goes back to its snapshot first', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    const pg = path.join(userData, 'stack', 'pg')
    fs.cpSync(path.join(pg, 'data'), path.join(pg, 'data.before-upgrade'), { recursive: true })
    fs.writeFileSync(path.join(pg, 'data', 'half-migrated'), 'x')
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, true)
    assert.equal(fs.existsSync(path.join(pg, 'data', 'half-migrated')), false)
    assert.equal(fs.existsSync(path.join(pg, 'data.before-upgrade')), false)
    await sup.stop()
  })
})

describe('the copy before an upgrade', () => {
  /** A `cp` on the stack's PATH that does as the test says, and otherwise the real one. */
  function fakeCp(behaviour) {
    const bin = tempRoot()
    fs.writeFileSync(path.join(bin, 'cp'), `#!/bin/sh\n${behaviour}\n`, { mode: 0o755 })
    return { PATH: `${bin}:${process.env.PATH}` }
  }

  async function installed() {
    const userData = tempRoot()
    const first = supervisor(fakeTree({ digest: 'one' }), userData)
    await first.start()
    await first.stop()
    const pg = path.join(userData, 'stack', 'pg')
    fs.writeFileSync(path.join(pg, 'data', 'users-data'), 'precious')
    return { userData, pg, tree: fakeTree({ digest: 'two' }) }
  }

  test('one a crash cut short is thrown away, and the live data stands', async () => {
    const { userData, pg, tree } = await installed()
    const partial = path.join(pg, 'data.before-upgrade.tmp')
    fs.mkdirSync(partial)
    fs.writeFileSync(path.join(partial, 'PG_VERSION'), '18\n')
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, true)
    assert.equal(fs.readFileSync(path.join(pg, 'data', 'users-data'), 'utf8'), 'precious')
    assert.equal(fs.existsSync(partial), false)
    assert.equal(fs.existsSync(path.join(pg, 'data.before-upgrade')), false)
    await sup.stop()
  })

  test('one that fails never takes the name a launch restores from', async () => {
    const { userData, pg, tree } = await installed()
    const env = fakeCp('echo "cp: no space left" >&2\nmkdir -p "$4"\nexit 1')
    const result = await supervisor(tree, userData, { env, platform: 'linux' }).start()
    assert.equal(result.ok, false)
    assert.equal(result.failure.step, 'database')
    assert.match(result.failure.message, /Could not copy your data folder/)
    assert.equal(fs.existsSync(path.join(pg, 'data.before-upgrade')), false)
    assert.equal(fs.existsSync(path.join(pg, 'data.before-upgrade.tmp')), false)
    assert.equal(fs.readFileSync(path.join(pg, 'data', 'users-data'), 'utf8'), 'precious')
  })

  test('a quit ends it, leaves no copy to restore, and starts nothing after', async () => {
    const { userData, pg, tree } = await installed()
    const env = fakeCp('mkdir -p "$4"\necho $$ > "$4/cp.pid"\nexec sleep 60')
    const sup = supervisor(tree, userData, { env, platform: 'linux' })
    const started = sup.start()
    const pidFile = path.join(pg, 'data.before-upgrade.tmp', 'cp.pid')
    await until(() => fs.existsSync(pidFile) && fs.readFileSync(pidFile, 'utf8').trim())
    const pid = Number(fs.readFileSync(pidFile, 'utf8'))
    assert.ok(alive(pid))
    await sup.stop()
    assert.equal(alive(pid), false)
    await started
    assert.equal(fs.existsSync(path.join(pg, 'data.before-upgrade')), false)
    assert.equal(fs.existsSync(path.join(pg, 'data.before-upgrade.tmp')), false)
    assert.deepEqual(calls(tree), [])
    assert.equal(fs.readFileSync(path.join(pg, 'data', 'users-data'), 'utf8'), 'precious')
  })
})

describe('a failed migration', () => {
  async function installedThenUpgraded() {
    const userData = tempRoot()
    const old = fakeTree({ digest: 'one' })
    const first = supervisor(old, userData)
    await first.start()
    await first.stop()
    const data = path.join(userData, 'stack', 'pg', 'data')
    fs.writeFileSync(path.join(data, 'users-data'), 'precious')
    return { userData, data, tree: fakeTree({ digest: 'two' }) }
  }

  test('puts the data folder back, starts no server, and says so', async () => {
    const { userData, data, tree } = await installedThenUpgraded()
    control(tree, { failMigrate: true, scribble: path.join(data, 'half-migrated') })
    const sup = supervisor(tree, userData)
    const result = await sup.start()
    assert.equal(result.ok, false)
    assert.equal(result.failure.step, 'migrate')
    assert.match(result.failure.dataNote, /put back/)
    assert.match(result.failure.tail, /migration failed/)
    assert.equal(calls(tree).some((c) => c.startsWith('server')), false)
    assert.equal(fs.existsSync(path.join(data, 'half-migrated')), false)
    assert.equal(fs.readFileSync(path.join(data, 'users-data'), 'utf8'), 'precious')
    assert.equal(fs.existsSync(`${data}.before-upgrade`), false)
    assert.deepEqual(Object.keys(sup.pids()), [])
  })

  test('can be retried, and a good run keeps its work and drops the copy', async () => {
    const { userData, data, tree } = await installedThenUpgraded()
    control(tree, { failMigrate: true })
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, false)
    control(tree, { scribble: path.join(data, 'migrated-ok') })
    assert.equal((await sup.start()).ok, true)
    assert.equal(fs.existsSync(path.join(data, 'migrated-ok')), true)
    assert.equal(fs.existsSync(`${data}.before-upgrade`), false)
    await sup.stop()
  })

  test('with the same migrations as last time there is no copy to take', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const first = supervisor(tree, userData)
    await first.start()
    await first.stop()
    control(tree, { failMigrate: true })
    const result = await supervisor(tree, userData).start()
    assert.match(result.failure.dataNote, /not changed/)
    assert.equal(calls(tree).includes('check'), false)
  })
})

describe('the release launch check', () => {
  const { runSmoke } = require(path.join(SRC, 'smoke.js'))
  const quiet = () => {}

  test('passes on a stack that starts and answers, and leaves nothing running', async () => {
    const sup = supervisor(fakeTree(), tempRoot())
    assert.equal(await runSmoke(sup, { log: quiet }), true)
    assert.deepEqual(Object.keys(sup.pids()), [])
  })

  test('fails on a stack that does not start', async () => {
    const tree = fakeTree()
    control(tree, { failServer: true })
    assert.equal(await runSmoke(supervisor(tree, tempRoot()), { log: quiet }), false)
  })

  test('fails on a server that starts but serves no app', async () => {
    const tree = fakeTree()
    control(tree, { blankShell: true })
    const sup = supervisor(tree, tempRoot())
    assert.equal(await runSmoke(sup, { log: quiet }), false)
    assert.deepEqual(Object.keys(sup.pids()), [])
  })
})

describe('data a newer version migrated', () => {
  test('stops before migrating, changes nothing, and says which version to install', async () => {
    const userData = tempRoot()
    const newer = fakeTree({ digest: 'two' })
    const first = supervisor(newer, userData)
    await first.start()
    await first.stop()
    const data = path.join(userData, 'stack', 'pg', 'data')
    fs.writeFileSync(path.join(data, 'users-data'), 'precious')

    const older = fakeTree({ digest: 'one' })
    control(older, { newerData: true })
    const sup = supervisor(older, userData)
    const result = await sup.start()
    assert.equal(result.ok, false)
    assert.equal(result.failure.step, 'migrate')
    assert.match(result.failure.message, /newer version of LangAlpha/)
    assert.match(result.failure.dataNote, /not changed/)
    assert.deepEqual(calls(older).map(steps).filter((s) => s !== 'postgres' && s !== 'valkey'), ['check'])
    assert.equal(fs.readFileSync(path.join(data, 'users-data'), 'utf8'), 'precious')
    assert.equal(fs.existsSync(`${data}.before-upgrade`), false)
    assert.deepEqual(Object.keys(sup.pids()), [])
  })

  test('a check that fails migrates nothing', async () => {
    const userData = tempRoot()
    const first = supervisor(fakeTree({ digest: 'one' }), userData)
    await first.start()
    await first.stop()
    const tree = fakeTree({ digest: 'two' })
    control(tree, { failCheck: true })
    const sup = supervisor(tree, userData)
    const result = await sup.start()
    assert.equal(result.ok, false)
    assert.match(result.failure.message, /could not be checked \(exit 1\)/)
    assert.match(result.failure.dataNote, /not changed/)
    assert.deepEqual(calls(tree).map(steps).filter((s) => s !== 'postgres' && s !== 'valkey'), ['check'])
    assert.deepEqual(Object.keys(sup.pids()), [])
  })

  test('an older revision this version knows migrates as usual', async () => {
    const userData = tempRoot()
    const first = supervisor(fakeTree({ digest: 'one' }), userData)
    await first.start()
    await first.stop()
    const tree = fakeTree({ digest: 'two' })
    const sup = supervisor(tree, userData)
    assert.equal((await sup.start()).ok, true)
    assert.deepEqual(calls(tree).map(steps), ['postgres', 'valkey', 'check', 'migrate', 'server'])
    await sup.stop()
  })
})

describe('staying up', () => {
  test('a server that dies is restarted on the same port', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot())
    await sup.start()
    const { server } = sup.pids()
    const origin = sup.origin
    process.kill(server, 'SIGKILL')
    await until(() => sup.pids().server && sup.pids().server !== server)
    await until(async () => (await fetch(`${origin}/health`).catch(() => null))?.status === 200)
    await sup.stop()
  })

  test('Postgres and Valkey come back on their own ports', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot())
    await sup.start()
    const before = sup.ports()
    const pids = sup.pids()
    process.kill(pids.postgres, 'SIGKILL')
    process.kill(pids.valkey, 'SIGKILL')
    await until(() => {
      const now = sup.pids()
      return now.postgres && now.postgres !== pids.postgres && now.valkey && now.valkey !== pids.valkey
    })
    assert.equal(sup.ports().pg, before.pg)
    assert.equal(sup.ports().valkey, before.valkey)
    assert.equal(sup.state, 'ready')
    await sup.stop()
  })

  test('a child that keeps dying ends in a failure that names it', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot(), { restartPolicy: { baseMs: 10, maxMs: 20, limit: 2 } })
    await sup.start()
    // Each new server exits at once from here on, which is what "keeps dying" looks like.
    control(tree, { failServer: true })
    process.kill(sup.pids().server, 'SIGKILL')
    await until(() => sup.state === 'failed')
    const { failure } = sup.view()
    assert.equal(failure.step, 'server')
    assert.match(failure.message, /keeps stopping/)
    await sup.stop()
    assert.deepEqual(sup.pids(), {})
  })


  test('a server that stops answering after a wake is restarted', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot(), { wakeSettleMs: 1, wakeRetryMs: 1 })
    await sup.start()
    const { server } = sup.pids()
    // A live process that no longer answers, which is what a stuck wake looks like.
    process.kill(server, 'SIGSTOP')
    await sup.afterWake()
    await until(() => sup.pids().server && sup.pids().server !== server)
    await sup.stop()
  })

  test('a server that dies during the wake check leaves its replacement starting', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot(), { wakeSettleMs: 300, wakeRetryMs: 20, restartPolicy: { baseMs: 10, maxMs: 20 } })
    await sup.start()
    const { server } = sup.pids()
    // The replacement is up but not answering yet, so every probe of the check fails.
    control(tree, { health: 500 })
    const woke = sup.afterWake()
    process.kill(server, 'SIGKILL')
    await woke
    const replacement = sup.pids().server
    assert.ok(replacement && replacement !== server)
    control(tree, {})
    await until(async () => (await fetch(`${sup.origin}/health`).catch(() => null))?.status === 200)
    assert.equal(sup.pids().server, replacement)
    assert.equal(alive(replacement), true)
    await sup.stop()
  })

  test('a server that still answers after a wake is left alone', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot(), { wakeSettleMs: 1, wakeRetryMs: 1 })
    await sup.start()
    const { server } = sup.pids()
    await sup.afterWake()
    assert.equal(sup.pids().server, server)
    await sup.stop()
  })
})

describe('ports', () => {
  test('a saved port someone else took moves, and the new one is saved', async () => {
    const taken = net.createServer()
    await new Promise((r) => taken.listen(0, '127.0.0.1', r))
    const port = taken.address().port
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot(), { port })
    const result = await sup.start()
    assert.equal(result.ok, true)
    assert.notEqual(new URL(result.origin).port, String(port))
    await sup.stop()
    await new Promise((r) => taken.close(r))
  })
})

describe('stopping', () => {
  test('in order, with Postgres told to shut down rather than killed', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot())
    await sup.start()
    const pids = Object.values(sup.pids())
    await sup.stop()
    const log = calls(tree)
    assert.deepEqual(log.slice(-3), ['server stop', 'valkey stop', 'postgres fast shutdown'])
    for (const pid of pids) assert.equal(alive(pid), false)
    assert.equal(sup.state, 'stopped')
  })
})

describe('the health probe', () => {
  const { httpStatus } = require(path.join(SRC, 'supervisor.js'))

  // A server that sends its status line, then a header every 100 ms, and never finishes them.
  async function trickling() {
    const sockets = new Set()
    const server = net.createServer((socket) => {
      sockets.add(socket)
      socket.on('error', () => {})
      socket.write('HTTP/1.1 200 OK\r\n')
      const timer = setInterval(() => socket.write('X-Slow: 1\r\n'), 100)
      socket.on('close', () => { clearInterval(timer); sockets.delete(socket) })
    })
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    const close = () => { for (const s of sockets) s.destroy(); return new Promise((resolve) => server.close(resolve)) }
    return { url: `http://127.0.0.1:${server.address().port}/health`, close }
  }

  test('gives up at its deadline on a server that keeps sending headers', async (t) => {
    const server = await trickling()
    t.after(server.close)
    const started = Date.now()
    assert.equal(await httpStatus(server.url, { timeoutMs: 300 }), null)
    assert.ok(Date.now() - started < 1000)
  })

  test('ends at once when its run is stopped', async (t) => {
    const server = await trickling()
    t.after(server.close)
    const run = new AbortController()
    const started = Date.now()
    const probe = httpStatus(server.url, { timeoutMs: 5000, signal: run.signal })
    setTimeout(() => run.abort(), 50)
    assert.equal(await probe, null)
    assert.ok(Date.now() - started < 1000)
  })
})

describe('a start cut short', () => {
  test('a server that exits while starting says so, rather than that it was slow', async () => {
    const tree = fakeTree()
    control(tree, { failServer: true })
    const result = await supervisor(tree, tempRoot()).start()
    assert.equal(result.failure.step, 'server')
    assert.equal(result.failure.message, 'The server stopped while starting.')
  })

  test('a stop while a port is being chosen waits for the start, and nothing launches after it', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot())
    let stopped = null
    // Queued, so it lands once the cache step is awaiting its port rather than before it begins.
    sup.on('change', (view) => {
      if (!stopped && view.steps.some((s) => s.id === 'cache' && s.status === 'running')) {
        queueMicrotask(() => { stopped = sup.stop() })
      }
    })
    let startEnded = false
    const started = sup.start().then((result) => { startEnded = true; return result })
    await until(() => stopped !== null)
    await stopped
    assert.equal(startEnded, true, 'the stop resolved before the start it cancelled')
    assert.equal((await started).ok, false)
    assert.deepEqual(calls(tree).map(steps), ['initdb', 'postgres', 'postgres'])
    assert.equal(calls(tree).at(-1), 'postgres fast shutdown')
    assert.deepEqual(sup.pids(), {})
    assert.equal(sup.state, 'stopped')
  })

  test('a start while a stop is still going comes up once the stop is done', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot())
    const first = sup.start()
    const stopped = sup.stop()
    const second = sup.start()
    assert.equal((await first).ok, false)
    await stopped
    const result = await second
    assert.equal(result.ok, true)
    assert.equal(sup.state, 'ready')
    assert.match(result.origin, /^http:\/\/127\.0\.0\.1:\d+$/)
    await sup.stop()
  })

  test('a start waiting on a stop is cancelled by a later stop, and nothing launches', async () => {
    const tree = fakeTree()
    const sup = supervisor(tree, tempRoot())
    const first = sup.start()
    const stopped = sup.stop()
    const queued = sup.start()
    const again = sup.stop()
    assert.equal((await first).ok, false)
    await stopped
    await again
    assert.equal((await queued).ok, false)
    assert.equal(sup.state, 'stopped')
    assert.deepEqual(sup.pids(), {})
  })

  test('a stop that lands as the server first answers leaves the stack stopped, not ready', async () => {
    const tree = fakeTree()
    // The answer is a 200 that arrives after the stop, from a server slow to go.
    control(tree, { healthDelayMs: 300, lingerMs: 1000 })
    const sup = supervisor(tree, tempRoot())
    const started = sup.start()
    await until(() => calls(tree).includes('health'))
    const stopped = sup.stop()
    const result = await started
    await stopped
    assert.equal(result.ok, false)
    assert.equal(sup.state, 'stopped')
    assert.equal(sup.view().failure, null)
  })

  test('a restart a stop cuts short is not logged as a restart', async () => {
    const tree = fakeTree()
    const userData = tempRoot()
    const sup = supervisor(tree, userData, { restartPolicy: { baseMs: 10, maxMs: 20 } })
    await sup.start()
    const { server } = sup.pids()
    control(tree, { healthDelayMs: 300, lingerMs: 1000 })
    process.kill(server, 'SIGKILL')
    await until(() => calls(tree).includes('health'))
    await sup.stop()
    // Past when the cancelled restart's health answer arrives.
    await new Promise((r) => setTimeout(r, 500))
    const log = fs.readFileSync(path.join(userData, 'logs', 'supervisor.log'), 'utf8')
    assert.match(log, /server ended unexpectedly/)
    assert.doesNotMatch(log, /server restarted/)
  })
})
