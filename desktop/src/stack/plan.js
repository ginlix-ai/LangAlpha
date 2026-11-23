'use strict'

/**
 * What the bundled stack decides, separated from what it does about it, the same
 * split `policy` and `main` keep. Nothing here starts a process or writes a file.
 */

const path = require('node:path')

/** Where things are in the tree the package carries. */
function layoutOf(root, env = {}) {
  const pg = env.LANGALPHA_DESKTOP_PG_PREFIX || path.join(root, 'pg')
  return {
    root,
    manifest: path.join(root, 'manifest.json'),
    python: path.join(root, 'python', 'bin', 'python3'),
    app: path.join(root, 'app'),
    web: path.join(root, 'web'),
    chartRender: path.join(root, 'chart-render'),
    initdb: path.join(pg, 'bin', 'initdb'),
    postgres: path.join(pg, 'bin', 'postgres'),
    valkey: env.LANGALPHA_DESKTOP_VALKEY_BIN || path.join(root, 'valkey', 'bin', 'valkey-server'),
  }
}

/** Where things are in userData. Everything a launch needs to keep, and the logs. */
function dataPathsOf(userData) {
  const stack = path.join(userData, 'stack')
  return {
    stack,
    pgData: path.join(stack, 'pg', 'data'),
    pgInit: path.join(stack, 'pg', 'data.init'),
    pgSnapshot: path.join(stack, 'pg', 'data.before-upgrade'),
    pgSnapshotPartial: path.join(stack, 'pg', 'data.before-upgrade.tmp'),
    migrated: path.join(stack, 'migrated.json'),
    secrets: path.join(stack, 'secrets.json'),
    valkeyDir: path.join(stack, 'valkey'),
    valkeyConf: path.join(stack, 'valkey', 'valkey.conf'),
    logs: path.join(userData, 'logs'),
  }
}

const PG_USER = 'langalpha'
const PG_DB = 'postgres'

/**
 * How long an open event stream may hold the server's shutdown, and how long the
 * supervisor then waits before a kill. The hook that cancels runs and ends their
 * jobs runs only after the streams let go, then the runs get up to config.yaml's
 * `shutdown_timeout` (50s) to settle, and only then is this computer stopped. A
 * kill anywhere in that leaves commands running with no owner after the app has
 * quit, so the second holds all of it. A quit with nothing running takes seconds.
 */
const SERVER_STREAM_GRACE_SECONDS = 8
const SERVER_STOP_GRACE_SECONDS = 75

/** What .github/workflows/sandbox-image.yml publishes: pulled once, where Dockerfile.sandbox would take a long first run. */
const DOCKER_SANDBOX_IMAGE = 'ghcr.io/ginlix-ai/langalpha/sandbox:latest'

/**
 * The steps a launch shows, in order. `init` only when there is no data folder
 * yet: it is the slow one on a first run and meaningless on any other.
 */
function stepsFor({ firstRun, externalDb }) {
  return [
    { id: 'prepare', label: 'Preparing your data folder' },
    ...(firstRun && !externalDb ? [{ id: 'init', label: 'Creating the database' }] : []),
    ...(externalDb ? [] : [{ id: 'database', label: 'Starting the database' }]),
    { id: 'cache', label: 'Starting the cache' },
    { id: 'migrate', label: 'Updating the database' },
    { id: 'server', label: 'Starting LangAlpha' },
  ]
}

/** initdb's arguments. Postgres 17's builtin provider gives proper Unicode case handling with no ICU. */
function initdbArgs({ dataDir, pwFile, major }) {
  return [
    '-D', dataDir,
    '-U', PG_USER,
    `--pwfile=${pwFile}`,
    '-A', 'scram-sha-256',
    '-E', 'UTF8',
    '--locale=C',
    ...(major >= 17 ? ['--locale-provider=builtin', '--builtin-locale=C.UTF-8'] : []),
    '--no-instructions',
  ]
}

/**
 * Postgres in the foreground, so it is a child this process can watch rather
 * than a daemon `pg_ctl` detached. TCP on loopback only: the socket directory is
 * turned off because a Unix socket path is limited to about 100 characters and
 * `Application Support` eats most of that, which would fail for some users by
 * the length of their name.
 */
function postgresArgs({ dataDir, port }) {
  return [
    '-D', dataDir,
    '-p', String(port),
    '-h', '127.0.0.1',
    '-k', '',
    '-c', 'logging_collector=off',
  ]
}

/** Quoted, because `Application Support` has a space in it and this file's parser splits on spaces. */
function valkeyConf({ port, password, dir }) {
  return [
    'bind 127.0.0.1',
    `port ${port}`,
    `requirepass ${password}`,
    'protected-mode yes',
    'daemonize no',
    'save ""',
    'appendonly no',
    `dir "${dir.replace(/(["\\])/g, '\\$1')}"`,
    '',
  ].join('\n')
}

function serverArgs({ port }) {
  return ['server.py', '--host', '127.0.0.1', '--port', String(port), '--log-level', 'info']
}

/** Only what the server names to be useful on this machine reaches it, as the local computer provider does for its own commands. */
const PASSED_ENV = [
  'HOME', 'USER', 'LOGNAME', 'SHELL', 'TMPDIR', 'LANG', 'LC_ALL', 'LC_CTYPE',
  'XDG_CACHE_HOME', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_RUNTIME_DIR', 'XDG_STATE_HOME',
  'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy',
  'DOCKER_HOST', 'DOCKER_CONTEXT',
]

/** What a login's PATH holds and an app started from the Dock's does not: Docker's CLI, Homebrew. */
const USUAL_PATH = ['/opt/homebrew/bin', '/usr/local/bin', '/usr/bin', '/bin', '/usr/sbin', '/sbin']

/**
 * The whole environment of a stack process, built up rather than inherited with
 * some of it removed. A backend that inherits picks up whatever the shell that
 * launched the app had exported, including provider keys; the only keys this
 * backend sees are the ones its owner saves in setup.
 */
function baseEnv(from) {
  const env = {}
  for (const name of PASSED_ENV) if (from[name]) env[name] = from[name]
  const path_ = (from.PATH || '').split(':').filter(Boolean)
  env.PATH = [...new Set([...path_, ...USUAL_PATH])].join(':')
  return env
}

/**
 * The database a `postgres://` URI names, read from the text as libpq reads it:
 * `URL` resolves dot segments, which would turn a database named `..` into none.
 */
function rawDatabase(raw) {
  const path = raw.replace(/^[a-z]+:\/\/[^/?#]*/i, '').split(/[?#]/)[0]
  return decodeURIComponent(path.replace(/^\//, ''))
}

/**
 * A `postgres://` override as the backend's DB_* variables. Its `sslmode` is
 * kept, so a server that must be reached over TLS is, and with none named the
 * connection asks for TLS where the server offers it, as libpq does. Any other
 * parameter is refused, since the backend takes only DB_* variables and would
 * otherwise connect without it.
 */
function parseDbUrl(given) {
  // Trimmed once, so `URL` and the database read below see the same text.
  const raw = given.trim()
  const url = new URL(raw)
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new Error('LANGALPHA_DESKTOP_PG_URL is not a postgres:// URL')
  }
  // Named, since with none Postgres takes this Mac's account name for the user
  // and the database, which is not the server's environment to guess at.
  if (!url.username) throw new Error('LANGALPHA_DESKTOP_PG_URL names no user')
  // Decoded as libpq decodes it, since `URL` leaves escapes in a host it does not know.
  let host
  try {
    host = decodeURIComponent(url.hostname)
  } catch {
    throw new Error('LANGALPHA_DESKTOP_PG_URL names a host that cannot be read')
  }
  // libpq's way of naming a Unix socket, which DB_HOST cannot carry to the server.
  if (host.startsWith('/')) throw new Error('LANGALPHA_DESKTOP_PG_URL names a socket folder; only a host name or address is supported')
  const keys = [...url.searchParams.keys()]
  // Named twice, libpq takes the last and this would read the first.
  const repeated = keys.find((key, i) => keys.indexOf(key) !== i)
  if (repeated) throw new Error(`LANGALPHA_DESKTOP_PG_URL sets ${repeated} more than once`)
  const unsupported = keys.filter((key) => key !== 'sslmode')
  if (unsupported.length > 0) {
    throw new Error(`LANGALPHA_DESKTOP_PG_URL sets ${unsupported.join(', ')}, which the app cannot pass on; only sslmode is supported`)
  }
  // With no database named, Postgres connects to the one named after the user.
  const name = rawDatabase(raw) || decodeURIComponent(url.username)
  // The server and its migrations put the name into connection URLs as it is,
  // where `?` ends it and `%` starts an escape, so they would reach another
  // database than the one the revision check reads.
  if (/[?%]/.test(name)) throw new Error(`LANGALPHA_DESKTOP_PG_URL names the database "${name}"; a name with ? or % is not supported`)
  return {
    // An IPv6 address keeps its brackets: the server builds connection URLs from
    // DB_HOST, and the revision check, which passes it alone, takes them off.
    host,
    port: Number(url.port) || 5432,
    user: decodeURIComponent(url.username),
    password: decodeURIComponent(url.password),
    name,
    sslmode: url.searchParams.get('sslmode') || 'prefer',
  }
}

/**
 * The backend's environment. `HOST_MODE` set rather than left to its default,
 * since `local_computer_allowed` reads the literal variable. `PYTHON_DOTENV_DISABLED`
 * because `load_dotenv` walks up from the source file looking for a `.env`, and an
 * app dropped into a project folder would otherwise pick up that project's keys.
 * The rest of `PYTHON*` are for a tree that cannot be written to: a signed bundle
 * or an AppImage mount. Docker computers are off on Linux because a container
 * there reaches the host over the bridge, and this backend listens on loopback.
 */
function backendEnv({ layout, from, db, redisUrl, byokKey, relaySecret, ownerSecret, serviceToken, port, pythonMinor, platform = process.platform, node = process.execPath }) {
  const origin = `http://127.0.0.1:${port}`
  return {
    ...baseEnv(from),
    HOST_MODE: 'oss',
    LANGALPHA_LOCAL_COMPUTER: '1',
    LANGALPHA_DESKTOP_APP: '1',
    ...(platform === 'linux' ? { LANGALPHA_DOCKER_COMPUTERS: '0' } : {}),
    DOCKER_SANDBOX_IMAGE,
    LANGALPHA_WEB_DIST: layout.web,
    CHARTKIT_RENDER_DIR: layout.chartRender,
    LANGALPHA_STDIO_OWN_PYTHON: '1',
    // This app's own binary, which runs as Node: the `la` CLI on This computer
    // needs one, and a Mac comes with none.
    LANGALPHA_NODE: node,
    DB_TYPE: 'postgres',
    DB_HOST: db.host,
    DB_PORT: String(db.port),
    DB_USER: db.user,
    DB_PASSWORD: db.password,
    DB_NAME: db.name,
    // The bundled Postgres listens on loopback only, with no certificate.
    DB_SSLMODE: db.sslmode || 'disable',
    REDIS_URL: redisUrl,
    BYOK_ENCRYPTION_KEY: byokKey,
    // Without it the relay is off, and with it the connectors the user signs in to.
    EGRESS_RELAY_SECRET: relaySecret,
    // Only a request carrying it is the owner (`owner.js`), and the server's
    // calls to itself carry the service token instead.
    LANGALPHA_OWNER_SECRET: ownerSecret,
    INTERNAL_SERVICE_TOKEN: serviceToken,
    SERVER_BASE_URL: origin,
    PUBLIC_APP_URL: origin,
    // The server's calls to itself, each defaulting to a port this one is not on.
    GINLIXFLOW_BASE_URL: origin,
    PDF_RENDER_INTERNAL_BASE: origin,
    EGRESS_RELAY_LOOPBACK_URL: origin,
    LANGALPHA_GRACEFUL_SHUTDOWN_SECONDS: String(SERVER_STREAM_GRACE_SECONDS),
    LANGSMITH_TRACING: 'false',
    SSE_EVENT_LOG_LEVEL: 'WARNING',
    PYTHONPATH: `${path.join(layout.app, 'src')}:${layout.app}`,
    PYTHONDONTWRITEBYTECODE: '1',
    // Only the bundled packages, never ones installed for the user under the same Python.
    PYTHONNOUSERSITE: '1',
    PYTHONUNBUFFERED: '1',
    PYTHON_DOTENV_DISABLED: '1',
    // python-build-standalone's OpenSSL has no system trust store to find.
    SSL_CERT_FILE: path.join(layout.root, 'python', 'lib', `python${pythonMinor}`, 'site-packages', 'certifi', 'cacert.pem'),
  }
}

/** The fields `postmaster.pid` carries, by line. */
function parsePidFile(text) {
  const lines = String(text || '').split('\n')
  const pid = Number.parseInt(lines[0], 10)
  return {
    pid: Number.isInteger(pid) && pid > 0 ? pid : null,
    dataDir: lines[1] || null,
    status: (lines[7] || '').trim() || null,
  }
}

/**
 * What a leftover `postmaster.pid` means. While the pid in it is dead the file is
 * Postgres's own to judge (`dead`): it reads the shared memory the file names and
 * refuses to start while backends of the old one are still attached, which a file
 * removed under them would hide. After a reboot that number can belong to anything,
 * and Postgres then refuses to start for as long as that process lives, so that
 * file, and one too damaged to name a pid, is `stale` and goes. A process counts as
 * ours only if it is a postgres running on this very data folder, which it is when
 * its working directory, `cwd`, is that folder: a postmaster runs from its data
 * folder, and `ps` flattens a path with spaces into the arguments around it, so
 * the command line cannot say. Another postgres on this machine, a Homebrew one
 * say, is someone else's and must not be signalled. A postgres whose folder could
 * not be read is left to Postgres too, since removing a live one's lock would let
 * a second open the same data.
 */
function classifyLock({ pid, command, cwd, dataDir, alive = false }) {
  if (pid === null) return 'stale'
  if (command === null) return alive ? 'unknown' : 'dead'
  if (!/\bpostgres\b/.test(command)) return 'stale'
  // A postgres whose folder could not be read may be on this one.
  if (cwd === null) return 'unknown'
  return cwd === dataDir ? 'ours' : 'stale'
}

/**
 * Whether this build's Postgres can open a data folder, off the major version
 * stamped in `PG_VERSION`. Opening another major's folder is not something it
 * can do at all, and refusing here is what turns that into a sentence rather
 * than a tail of `FATAL` lines.
 */
function dataDirState(versionText, bundledMajor) {
  if (versionText === null || versionText === undefined) return { state: 'missing' }
  const found = Number.parseInt(String(versionText).trim(), 10)
  if (!Number.isInteger(found)) return { state: 'unreadable' }
  return found === bundledMajor ? { state: 'current', found } : { state: 'other-major', found }
}

function majorMismatchMessage(found, bundled) {
  const direction = found < bundled
    ? `This version of the app carries PostgreSQL ${bundled} and cannot open it yet.`
    : `This version of the app carries the older PostgreSQL ${bundled}, which cannot open it.`
  return `Your data folder was created by PostgreSQL ${found}. ${direction} Nothing was changed. Install the app version you had before to keep using this data.`
}

/**
 * Exits 3, naming them, when the database stands at a revision this tree's
 * migrations do not have. That is data a newer release migrated, and `alembic
 * upgrade` would only fail on it partway with "Can't locate revision".
 */
const REVISION_CHECK = `
import os, sys
import psycopg
from alembic.config import Config
from alembic.script import ScriptDirectory

known = {r.revision for r in ScriptDirectory.from_config(Config("alembic.ini")).walk_revisions()}
with psycopg.connect(
    host=os.environ["DB_HOST"].strip("[]"), port=os.environ["DB_PORT"], user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"], dbname=os.environ["DB_NAME"],
    sslmode=os.environ.get("DB_SSLMODE", "disable"),
) as conn:
    if conn.execute("select to_regclass('alembic_version')").fetchone()[0] is None:
        sys.exit(0)
    found = [row[0] for row in conn.execute("select version_num from alembic_version")]
unknown = [rev for rev in found if rev not in known]
if unknown:
    print(" ".join(unknown))
    sys.exit(3)
`
const UNKNOWN_REVISION_EXIT = 3

const NEWER_DATA_MESSAGE = 'Your data was last opened by a newer version of LangAlpha, which updated it in ways this version does not know. Install that version or a later one to keep using it.'

/** What a failure says about the user's data, by the note its step left. */
const DATA_NOTES = {
  untouched: 'Your data was not changed.',
  restored: 'Your data has been put back as it was before the update.',
  'new-install': 'This is a new install, so there was no data to lose.',
  external: 'The database is yours, so it was left as the failure left it.',
}

function dataNoteText(note) {
  return DATA_NOTES[note] || null
}

/**
 * What a migration that did not finish leaves of the data. With no snapshot and
 * no new install, the migrations were the ones that had already run, so the run
 * had nothing to change.
 */
function dataAfterFailedMigration({ firstRun, snapshotTaken, externalDb }) {
  if (firstRun) return 'new-install'
  if (snapshotTaken) return 'restored'
  if (externalDb) return 'external'
  return 'untouched'
}

/** The failure a process leaves when its port was taken between choosing it and binding. */
function looksLikeBindFailure(text) {
  return /address already in use|could not bind|errno 48|errno 98/i.test(text || '')
}

/** A socket's or server's answer that means the backend is up, including its "up, with a remote MCP server down" 503. */
function healthAnswered(status) {
  return status === 200 || status === 503
}

/**
 * The delay before restarting a child that died, or null once it has died too
 * often to keep trying. Five times in two minutes is a thing that does not work,
 * and restarting it forever would hide that behind a window that never quite
 * comes back.
 */
function nextRestart(history, now, { windowMs = 120_000, limit = 5, baseMs = 1000, maxMs = 30_000 } = {}) {
  const recent = history.filter((t) => now - t < windowMs)
  if (recent.length >= limit) return { delay: null, history: recent }
  return { delay: Math.min(baseMs * 2 ** recent.length, maxMs), history: [...recent, now] }
}

/** The `.env` files `load_dotenv` would find walking up from `start`, for the log. */
function dotenvsAbove(start, exists) {
  const found = []
  let dir = start
  for (;;) {
    const candidate = path.join(dir, '.env')
    if (exists(candidate)) found.push(candidate)
    const parent = path.dirname(dir)
    if (parent === dir) return found
    dir = parent
  }
}

module.exports = {
  PG_USER, PG_DB, USUAL_PATH, SERVER_STOP_GRACE_SECONDS,
  layoutOf, dataPathsOf, stepsFor, initdbArgs, postgresArgs, valkeyConf, serverArgs,
  baseEnv, backendEnv, parseDbUrl, parsePidFile, classifyLock, dataDirState, majorMismatchMessage,
  REVISION_CHECK, UNKNOWN_REVISION_EXIT, NEWER_DATA_MESSAGE, dataNoteText, dataAfterFailedMigration,
  looksLikeBindFailure, healthAnswered, nextRestart, dotenvsAbove,
}
