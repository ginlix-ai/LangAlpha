'use strict'

const { execFile } = require('node:child_process')
const { randomBytes } = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')

const plan = require('./plan')
const { runToEnd, pollUntil } = require('./children')
const { StepError } = require('./stepError')

const INIT_MS = 5 * 60_000

function readText(file) {
  try {
    return fs.readFileSync(file, 'utf8')
  } catch (err) {
    if (err.code === 'ENOENT') return null
    throw err
  }
}

function commandOf(pid) {
  return new Promise((resolve) => {
    execFile('ps', ['-o', 'command=', '-p', String(pid)], (err, stdout) => {
      resolve(err ? null : stdout.trim() || null)
    })
  })
}

/** The folder `pid` runs from, read off the system rather than its command line, or null. */
function cwdOf(pid, platform) {
  if (platform === 'linux') {
    try {
      return Promise.resolve(fs.readlinkSync(`/proc/${pid}/cwd`))
    } catch {
      return Promise.resolve(null)
    }
  }
  return new Promise((resolve) => {
    execFile('/usr/sbin/lsof', ['-a', '-p', String(pid), '-d', 'cwd', '-Fn'], (err, stdout) => {
      const line = err ? null : stdout.split('\n').find((l) => l.startsWith('n'))
      resolve(line ? line.slice(1) : null)
    })
  })
}

function isRunning(pid) {
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    // Another user's process, which this one may not signal, still runs.
    return err.code === 'EPERM'
  }
}

/**
 * The stack's data folder and the rules that keep it whole. A database is only
 * ever created or copied under a temporary name and renamed once whole, so a
 * launch cut off anywhere finds either the folder as it was or a finished one,
 * and a snapshot's name always means a complete copy a launch may restore.
 */
function createDataFolder({ data, env, platform, logFor, lockWaitTries }) {
  const pidFile = path.join(data.pgData, 'postmaster.pid')
  const hasDatabase = (dir) => fs.existsSync(path.join(dir, 'PG_VERSION'))

  /** Generated once, and never regenerated: the encryption key is what makes saved provider keys readable. */
  function loadSecrets() {
    fs.mkdirSync(data.stack, { recursive: true, mode: 0o700 })
    const existing = readText(data.secrets)
    if (existing === null && (hasDatabase(data.pgData) || hasDatabase(data.pgSnapshot))) {
      // New passwords would not open the database that is already here, and a
      // new key could never read what it holds.
      throw new StepError('prepare', `${data.secrets} is missing. Your saved keys cannot be read until it is restored.`, { dataNote: 'untouched' })
    }
    if (existing !== null) {
      let parsed = null
      try {
        parsed = JSON.parse(existing)
      } catch { /* the check below answers for this too */ }
      if (parsed?.byokKey && parsed.pgPassword && parsed.valkeyPassword) {
        // Later than the file itself on an earlier install. It signs the relay's
        // short-lived grants and encrypts nothing kept, so one made now loses nothing.
        if (!parsed.relaySecret) {
          parsed = { ...parsed, relaySecret: randomBytes(32).toString('hex') }
          save(parsed)
        }
        return parsed
      }
      // A key file present but unusable is not a first run. Writing a new key
      // here would make everything already encrypted unreadable with no way back.
      throw new StepError('prepare', `${data.secrets} is damaged. Your saved keys cannot be read until it is restored.`, { dataNote: 'untouched' })
    }
    const fresh = {
      byokKey: randomBytes(32).toString('hex'),
      pgPassword: randomBytes(24).toString('hex'),
      valkeyPassword: randomBytes(24).toString('hex'),
      relaySecret: randomBytes(32).toString('hex'),
    }
    save(fresh)
    return fresh
  }

  function save(secrets) {
    const tmp = `${data.secrets}.tmp`
    fs.writeFileSync(tmp, JSON.stringify(secrets, null, 2), { mode: 0o600 })
    fs.renameSync(tmp, data.secrets)
  }

  /**
   * Finishes what a previous launch left half done: an interrupted first creation
   * or copy goes away, and an interrupted upgrade goes back to its snapshot, since
   * the data folder beside that snapshot may be half migrated.
   */
  async function recoverInterrupted(signal) {
    fs.mkdirSync(path.dirname(data.pgData), { recursive: true, mode: 0o700 })
    fs.rmSync(data.pgInit, { recursive: true, force: true })
    fs.rmSync(data.pgSnapshotPartial, { recursive: true, force: true })
    if (!fs.existsSync(data.pgSnapshot)) return
    logFor('supervisor').note('an upgrade did not finish; restoring the data folder from before it')
    await clearLockOrStop(signal)
    restore()
  }

  /**
   * Whether this is a first run, after recovering what a launch left half done.
   * A folder from another Postgres major is refused before anything touches it.
   */
  async function prepare(major, signal) {
    await recoverInterrupted(signal)
    const { state, found } = plan.dataDirState(readText(path.join(data.pgData, 'PG_VERSION')), major)
    if (state === 'other-major') {
      throw new StepError('database', plan.majorMismatchMessage(found, major), { dataNote: 'untouched' })
    }
    if (state === 'unreadable') {
      throw new StepError('database', `${path.join(data.pgData, 'PG_VERSION')} cannot be read.`, { dataNote: 'untouched' })
    }
    // Before a launch may copy the folder for an upgrade, which is no copy at all
    // while a postgres still writes to it.
    if (state !== 'missing') await clearLockOrStop(signal)
    return state === 'missing'
  }

  /** Into a temporary name and renamed once whole, so an interrupted initdb can never look like a data folder. */
  async function initialize({ initdb, major, password, signal }) {
    const pwFile = path.join(data.stack, 'pg', '.pwfile')
    fs.writeFileSync(pwFile, password, { mode: 0o600 })
    let handle
    try {
      handle = await runToEnd({
        name: 'initdb',
        command: initdb,
        args: plan.initdbArgs({ dataDir: data.pgInit, pwFile, major }),
        env: { ...plan.baseEnv(env), LC_ALL: 'C' },
        cwd: data.stack,
        log: logFor('postgres'),
        graceSeconds: 5,
      }, { signal, timeoutMs: INIT_MS })
    } finally {
      fs.rmSync(pwFile, { force: true })
    }
    if (handle.error) throw new StepError('init', `Could not start initdb: ${handle.error.message}`, { log: 'postgres' })
    if (handle.code !== 0) throw new StepError('init', 'Creating the database failed.', { log: 'postgres' })
    fs.renameSync(data.pgInit, data.pgData)
  }

  /** The stale lock cleared, or the launch stopped with the folder untouched when its postgres cannot be checked. */
  async function clearLockOrStop(signal) {
    if (await clearStaleLock(signal) === 'unknown') {
      throw new StepError('database', 'A database process is still running and could not be checked, so your data folder was left as it was. Quit it or restart this computer, then try again.', { log: 'postgres', dataNote: 'untouched' })
    }
  }

  /** What `postmaster.pid` says now: whose lock it is, and whether that postgres is ready. */
  const postmaster = () => plan.parsePidFile(readText(pidFile))

  async function clearStaleLock(signal) {
    const contents = readText(pidFile)
    if (contents === null) return
    const { pid } = plan.parsePidFile(contents)
    const command = pid ? await commandOf(pid) : null
    const cwd = command ? await cwdOf(pid, platform) : null
    // As the system reports a working directory: with every link resolved.
    const verdict = plan.classifyLock({ pid, command, cwd, alive: pid ? isRunning(pid) : false, dataDir: fs.realpathSync(data.pgData) })
    const log = logFor('postgres')
    // Left for Postgres, whose own check refuses a lock a live postmaster holds.
    if (verdict === 'dead' || verdict === 'unknown') return verdict
    if (verdict === 'ours') {
      // Left from a launch whose guard has not finished with it yet, or never
      // ran. Fast shutdown is the same request `pg_ctl stop -m fast` sends.
      log.note(`stopping postgres ${pid}, left running on this data folder`)
      try { process.kill(pid, 'SIGINT') } catch { /* gone already */ }
      const released = await pollUntil(() => readText(pidFile) === null || !isRunning(pid), { tries: lockWaitTries, every: 500, signal })
      if (readText(pidFile) === null) return
      // The lock is all that stops a second postgres opening this folder while
      // this one still writes to it.
      if (!released) {
        throw new StepError('database', `An earlier database process (${pid}) is still running on your data folder and did not stop. Quit it or restart this computer, then try again.`, { log: 'postgres', dataNote: 'untouched' })
      }
      // Gone without removing its file, as after a kill: Postgres's own to judge.
      return
    }
    // Only the lock judged above: a stop during the lookups may have let a new
    // start write its own, which is all that keeps a second postgres off.
    signal?.throwIfAborted()
    if (readText(pidFile) !== contents) return
    log.note('removing a postmaster.pid no process stands behind')
    fs.rmSync(pidFile, { force: true })
  }

  /** The migrations that last ran to the end on this data, or null. */
  function migratedDigest() {
    try {
      return JSON.parse(readText(data.migrated) || 'null')?.digest ?? null
    } catch {
      return null
    }
  }

  function markMigrated(digest) {
    fs.writeFileSync(data.migrated, JSON.stringify({ digest }))
  }

  function clearMigrated() {
    fs.rmSync(data.migrated, { force: true })
  }

  /**
   * A copy of the data folder before an upgrade's migrations. Some of them commit
   * outside the run's transaction (`autocommit_block`), so a failure partway
   * cannot simply roll back, and "your data is as it was" has to be made true
   * some other way. Under the partial name until whole, since a launch restores
   * whatever holds the snapshot's name over the live data.
   */
  async function takeSnapshot(signal) {
    logFor('supervisor').note('new migrations; copying the data folder first')
    try {
      await copyTree(data.pgData, data.pgSnapshotPartial, signal)
      fs.renameSync(data.pgSnapshotPartial, data.pgSnapshot)
    } catch (err) {
      fs.rmSync(data.pgSnapshotPartial, { recursive: true, force: true })
      if (signal.aborted) throw err
      throw new StepError('database', `Could not copy your data folder before updating it: ${err.message}.`, { dataNote: 'untouched' })
    }
  }

  /**
   * A cheap copy where the filesystem can do one: clones on APFS, reflinks where
   * Linux has them. A child like any other, so a quit ends it and so does the
   * app's death: an orphan would go on writing into the next launch's copy.
   */
  async function copyTree(from, to, signal) {
    const cp = (args) => runToEnd({
      name: 'snapshot',
      command: 'cp',
      args,
      env: plan.baseEnv(env),
      cwd: data.stack,
      log: logFor('supervisor'),
      graceSeconds: 5,
    }, { signal })
    const failed = (handle) => handle.error || handle.code !== 0
    const onMac = platform === 'darwin'
    let result = await cp(onMac ? ['-cR', from, to] : ['-a', '--reflink=auto', from, to])
    if (failed(result) && onMac) {
      // `cp -c` refuses on a volume with no clones. A plain copy still works, slower.
      fs.rmSync(to, { recursive: true, force: true })
      result = await cp(['-R', from, to])
    }
    if (failed(result)) throw new Error(result.error?.message || `cp exited with ${result.code ?? result.signal}`)
  }

  /** Renamed first, so a delete cut off partway leaves nothing a launch would restore. */
  function dropSnapshot() {
    if (!fs.existsSync(data.pgSnapshot)) return
    fs.rmSync(data.pgSnapshotPartial, { recursive: true, force: true })
    fs.renameSync(data.pgSnapshot, data.pgSnapshotPartial)
    fs.rmSync(data.pgSnapshotPartial, { recursive: true, force: true })
  }

  /** The data folder put back as the snapshot has it. Postgres must not have it open. */
  function restore() {
    fs.rmSync(data.pgData, { recursive: true, force: true })
    fs.renameSync(data.pgSnapshot, data.pgData)
  }

  return {
    loadSecrets,
    recoverInterrupted,
    prepare,
    initialize,
    postmaster,
    clearStaleLock,
    migratedDigest,
    markMigrated,
    clearMigrated,
    takeSnapshot,
    dropSnapshot,
    restore,
  }
}

module.exports = { createDataFolder, readText }
