'use strict'

/** What the bundled stack decides, apart from anything it runs. */

const { test, describe } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

const { SRC } = require('./stackFixture')
const plan = require(path.join(SRC, 'plan.js'))

describe('what the plan decides', () => {
  test('a lock counts as ours only for a postgres on this very data folder', () => {
    const dataDir = '/data/stack/pg/data'
    const cases = [
      [{ pid: null, command: null, dataDir }, 'stale'],
      [{ pid: 5, command: null, dataDir }, 'dead'],
      // Running, but `ps` could not say what it is.
      [{ pid: 5, command: null, alive: true, dataDir }, 'unknown'],
      [{ pid: 5, command: '/Applications/Slack.app/Contents/MacOS/Slack', dataDir }, 'stale'],
      [{ pid: 5, command: '/opt/homebrew/bin/postgres -D /opt/homebrew/var/postgresql@14', cwd: '/opt/homebrew/var/postgresql@14', dataDir }, 'stale'],
      // `ps` shows `-D …/data backup` as `-D …/data` and one more argument.
      [{ pid: 5, command: `/app/pg/bin/postgres -D ${dataDir} backup -p 5000`, cwd: `${dataDir} backup`, dataDir }, 'stale'],
      [{ pid: 5, command: `/app/pg/bin/postgres -D ${dataDir}.backup`, cwd: `${dataDir}.backup`, dataDir }, 'stale'],
      [{ pid: 5, command: `/app/pg/bin/postgres -D ${dataDir} -p 5000`, cwd: dataDir, dataDir }, 'ours'],
      // Where it runs could not be read: it may be on this folder, so nothing may replace it.
      [{ pid: 5, command: `/app/pg/bin/postgres -D ${dataDir}`, cwd: null, dataDir }, 'unknown'],
    ]
    for (const [input, verdict] of cases) assert.equal(plan.classifyLock(input), verdict, JSON.stringify(input))
  })

  test('postmaster.pid is read by line', () => {
    assert.deepEqual(plan.parsePidFile('123\n/d\n1700\n5432\n\n127.0.0.1\n 1 2\nready   \n'), { pid: 123, dataDir: '/d', status: 'ready' })
    assert.deepEqual(plan.parsePidFile('garbage'), { pid: null, dataDir: null, status: null })
  })

  test('a data folder is judged by its stamped major', () => {
    assert.equal(plan.dataDirState(null, 18).state, 'missing')
    assert.equal(plan.dataDirState('18\n', 18).state, 'current')
    assert.deepEqual(plan.dataDirState('17\n', 18), { state: 'other-major', found: 17 })
    assert.deepEqual(plan.dataDirState('19\n', 18), { state: 'other-major', found: 19 })
    assert.equal(plan.dataDirState('nonsense', 18).state, 'unreadable')
  })

  test('both directions of a major mismatch say something true', () => {
    assert.match(plan.majorMismatchMessage(17, 18), /cannot open it yet/)
    assert.match(plan.majorMismatchMessage(19, 18), /older PostgreSQL 18/)
  })

  test('initdb asks for the builtin provider only where it exists', () => {
    const args = (major) => plan.initdbArgs({ dataDir: '/d', pwFile: '/p', major })
    assert.ok(args(18).includes('--locale-provider=builtin'))
    assert.ok(!args(14).includes('--locale-provider=builtin'))
  })

  test('Postgres is reachable on loopback only, with no socket file', () => {
    const args = plan.postgresArgs({ dataDir: '/d', port: 5 })
    assert.equal(args[args.indexOf('-h') + 1], '127.0.0.1')
    assert.equal(args[args.indexOf('-k') + 1], '')
  })

  test('the cache config survives a path with a space in it', () => {
    const conf = plan.valkeyConf({ port: 7, password: 'pw', dir: '/Users/a/Library/Application Support/x' })
    assert.match(conf, /^dir "\/Users\/a\/Library\/Application Support\/x"$/m)
    assert.match(conf, /^bind 127\.0\.0\.1$/m)
    assert.match(conf, /^save ""$/m)
  })

  test('what is inherited is a short allowlist, with the usual tool folders added', () => {
    const env = plan.baseEnv({ HOME: '/h', PATH: '/custom:/usr/bin', AWS_SECRET_ACCESS_KEY: 'x', DAYTONA_API_KEY: 'y' })
    assert.deepEqual(Object.keys(env).sort(), ['HOME', 'PATH'])
    assert.ok(env.PATH.startsWith('/custom:/usr/bin:'))
    assert.ok(env.PATH.includes('/opt/homebrew/bin'))
    assert.equal(env.PATH.split(':').filter((p) => p === '/usr/bin').length, 1)
  })

  test('every .env above a folder is found, for the log', () => {
    const found = plan.dotenvsAbove('/a/b/c', (file) => file === '/a/.env' || file === '/a/b/c/.env')
    assert.deepEqual(found, ['/a/b/c/.env', '/a/.env'])
  })

  test('the server is given longer to stop than its streams and its runs may hold its shutdown', () => {
    const env = plan.backendEnv({ layout: plan.layoutOf('/t'), from: {}, db: { host: 'h', port: 1, user: 'u', password: 'p', name: 'n' }, redisUrl: 'r', byokKey: 'k', port: 2, pythonMinor: '3.13' })
    assert.equal(env.LANGALPHA_GRACEFUL_SHUTDOWN_SECONDS, '8')
    const config = fs.readFileSync(path.join(__dirname, '..', '..', 'config.yaml'), 'utf8')
    const runsSettle = Number(config.match(/^\s*shutdown_timeout:\s*(\d+)/m)[1])
    assert.ok(plan.SERVER_STOP_GRACE_SECONDS >= Number(env.LANGALPHA_GRACEFUL_SHUTDOWN_SECONDS) + runsSettle + 10)
  })

  test('the backend is handed this app as the Node its computer runs `la` with', () => {
    const env = plan.backendEnv({ layout: plan.layoutOf('/t'), from: {}, db: { host: 'h', port: 1, user: 'u', password: 'p', name: 'n' }, redisUrl: 'r', byokKey: 'k', port: 2, pythonMinor: '3.13', node: '/A.app/Contents/MacOS/A' })
    assert.equal(env.LANGALPHA_NODE, '/A.app/Contents/MacOS/A')
  })

  test('Docker computers are off only where a container cannot reach loopback', () => {
    const args = { layout: plan.layoutOf('/t'), from: {}, db: { host: 'h', port: 1, user: 'u', password: 'p', name: 'n' }, redisUrl: 'r', byokKey: 'k', port: 2, pythonMinor: '3.13' }
    assert.equal(plan.backendEnv({ ...args, platform: 'linux' }).LANGALPHA_DOCKER_COMPUTERS, '0')
    assert.equal(plan.backendEnv({ ...args, platform: 'darwin' }).LANGALPHA_DOCKER_COMPUTERS, undefined)
    assert.equal(plan.backendEnv({ ...args, platform: 'darwin' }).DOCKER_SANDBOX_IMAGE, 'ghcr.io/ginlix-ai/langalpha/sandbox:latest')
  })

  test('a bad database override is refused rather than guessed at', () => {
    assert.throws(() => plan.parseDbUrl('mysql://x'), /postgres/)
  })

  test('a database override keeps the TLS it asks for, and the bundled one goes without', () => {
    const args = { layout: plan.layoutOf('/t'), from: {}, redisUrl: 'r', byokKey: 'k', port: 2, pythonMinor: '3.13' }
    const required = plan.parseDbUrl('postgresql://u:p@db.example.com:5433/app?sslmode=require')
    assert.equal(plan.backendEnv({ ...args, db: required }).DB_SSLMODE, 'require')
    assert.equal(plan.parseDbUrl('postgres://u:p@db.example.com/app').sslmode, 'prefer')
    assert.throws(() => plan.parseDbUrl('postgres://u:p@db.example.com/app?sslmode=verify-full&sslrootcert=/ca.pem'), /sslrootcert.*only sslmode/)
    assert.equal(plan.backendEnv({ ...args, db: { host: 'h', port: 1, user: 'u', password: 'p', name: 'n' } }).DB_SSLMODE, 'disable')
  })

  test('a database override that names no database gets the one named after its user, as Postgres does', () => {
    assert.equal(plan.parseDbUrl('postgres://alice:pw@db.example.com').name, 'alice')
    assert.equal(plan.parseDbUrl('postgres://alice:pw@db.example.com/my%20app').name, 'my app')
    assert.throws(() => plan.parseDbUrl('postgres://db.example.com'), /names no user/)
    assert.throws(() => plan.parseDbUrl('postgres://u:p@db.example.com/app%3Fapplication_name%3Ddesktop'), /\? or % is not supported/)
    assert.throws(() => plan.parseDbUrl('postgres://a%25b:p@db.example.com'), /"a%b".*not supported/)
  })

  test('a database override on an IPv6 address, or naming a setting twice', () => {
    assert.equal(plan.parseDbUrl('postgresql://u:p@[::1]:5433/app').host, '[::1]')
    assert.match(plan.REVISION_CHECK, /DB_HOST"\]\.strip\("\[\]"\)/)
    assert.throws(() => plan.parseDbUrl('postgresql://u:p@%2Fvar%2Frun%2Fpostgresql/app'), /socket folder/)
    assert.equal(plan.parseDbUrl('postgres://u:p@db%2Eexample.com/app').host, 'db.example.com')
    assert.equal(plan.parseDbUrl('  postgres://alice:secret@db.example.com/app \n').name, 'app')
    assert.equal(plan.parseDbUrl('postgres://alice:pw@db.example.com/%2e%2e').name, '..')
    assert.equal(plan.parseDbUrl('postgres://alice:pw@db.example.com/test%20db?sslmode=require').name, 'test db')
    assert.equal(plan.parseDbUrl('postgres://alice:pw@db.example.com/').name, 'alice')
    assert.equal(plan.parseDbUrl('postgresql://u:p@db.example.com/app').host, 'db.example.com')
    assert.throws(() => plan.parseDbUrl('postgresql://u:p@db.example.com/app?sslmode=disable&sslmode=require'), /sslmode more than once/)
  })

  test('the restart budget widens, caps, and forgets old deaths', () => {
    let history = []
    const now = 1_000_000
    const delays = []
    for (let i = 0; i < 6; i++) {
      const decision = plan.nextRestart(history, now + i)
      history = decision.history
      delays.push(decision.delay)
    }
    assert.deepEqual(delays, [1000, 2000, 4000, 8000, 16000, null])
    assert.equal(plan.nextRestart(history, now + 200_000).delay, 1000, 'old deaths do not count forever')
  })

  test('what a failure says about the data follows from what the failed migration left', () => {
    const after = (facts) => plan.dataAfterFailedMigration({ firstRun: false, snapshotTaken: false, externalDb: false, ...facts })
    assert.equal(after({ firstRun: true }), 'new-install')
    assert.equal(after({ snapshotTaken: true }), 'restored')
    assert.equal(after({ externalDb: true }), 'external')
    assert.equal(after({}), 'untouched')
    assert.equal(plan.dataNoteText('restored'), 'Your data has been put back as it was before the update.')
    assert.equal(plan.dataNoteText(null), null)
  })
})
