'use strict'

const { EventEmitter } = require('node:events')
const fs = require('node:fs')
const http = require('node:http')
const { setTimeout: sleep } = require('node:timers/promises')

const plan = require('./plan')
const { startChild, runToEnd, pollUntil, canConnect, freePort, stablePort } = require('./children')
const { createDataFolder, readText } = require('./dataFolder')
const { createLog } = require('./logs')
const { StepError } = require('./stepError')
const owner = require('./owner')

const MIGRATE_MS = 15 * 60_000
const BIND_RETRIES = 3

/**
 * The status a GET gets, or null. Bounded in elapsed time as well as idleness,
 * which is all `timeout` measures, and ended at once by `signal`.
 */
function httpStatus(url, { timeoutMs = 3000, signal } = {}) {
  return new Promise((resolve) => {
    const req = http.get(url, { timeout: timeoutMs, signal }, (res) => {
      clearTimeout(deadline)
      res.resume()
      resolve(res.statusCode)
    })
    const giveUp = () => { clearTimeout(deadline); req.destroy(); resolve(null) }
    const deadline = setTimeout(giveUp, timeoutMs)
    req.once('timeout', giveUp)
    req.once('error', giveUp)
  })
}

/**
 * Starts the stack in order and keeps it up: Postgres, Valkey, migrations, the
 * server. Knows nothing about windows. It reports through `change` events and
 * `view()`, and `main` draws them.
 *
 * Every child's end is observed here, which is what restarts it, and the failure
 * of a step always says which step and why: a user who sees "Starting the
 * database" and a tail of its log can do something about it, and one who sees a
 * spinner cannot.
 *
 * Each start is one run with its own AbortController. A stop aborts it, so every
 * wait in it ends at once and it launches nothing after, and then waits for it.
 */
function createSupervisor({
  treeRoot,
  userData,
  env = process.env,
  platform = process.platform,
  loadPort = () => null,
  savePort = () => {},
  // Shorter in the tests, which have real processes to wait for.
  restartPolicy = undefined,
  wakeSettleMs = 2000,
  wakeRetryMs = 5000,
  lockWaitTries = 40,
}) {
  const events = new EventEmitter()
  const layout = plan.layoutOf(treeRoot, env)
  const data = plan.dataPathsOf(userData)
  const externalDbUrl = env.LANGALPHA_DESKTOP_PG_URL || null

  let state = 'idle'
  let steps = []
  let reached = 0 // the running step's index; steps.length once all are done
  let failedStep = null
  let failure = null
  let origin = null
  // New at each launch, so nothing outside this run of the app holds them.
  const ownerSecret = owner.newSecret()
  const serviceToken = owner.newSecret()
  let manifest = null
  let secrets = null
  let db = null
  let current = null // this run's AbortController
  let starting = null
  let stopping = null
  // Counts stop calls, so a start queued behind a stop knows whether a later one cancelled it.
  let stops = 0
  const children = new Map() // service name -> handle
  const logs = new Map()
  const restartHistory = new Map()
  const ports = {}

  const logFor = (name) => {
    if (!logs.has(name)) logs.set(name, createLog(data.logs, name))
    return logs.get(name)
  }
  const folder = createDataFolder({ data, env, platform, logFor, lockWaitTries })

  /**
   * The long-running children, in the order they start; they stop in reverse,
   * the server first because it needs the other two for its own shutdown. Each
   * one's log is named after it.
   */
  const SERVICES = {
    postgres: {
      step: 'database',
      noun: 'database',
      port: 'pg',
      readyMs: 5 * 60_000,
      every: 150,
      choosePort: () => freePort(),
      launch: (port) => ({
        command: layout.postgres,
        args: plan.postgresArgs({ dataDir: data.pgData, port }),
        env: { ...plan.baseEnv(env), LC_ALL: 'C' },
        cwd: data.stack,
        // SIGINT is Postgres's fast shutdown: what `pg_ctl stop -m fast` sends.
        stopSignal: 'SIGINT',
        graceSeconds: 20,
      }),
      // `ready` in postmaster.pid is how `pg_ctl -w` itself knows. A TCP accept is
      // not enough, since Postgres accepts during crash recovery and answers "the
      // database system is starting up" until it is done.
      ready: (handle) => {
        const { pid, status } = folder.postmaster()
        return pid === handle.child.pid && status === 'ready'
      },
      notReady: () => 'The database did not start.',
      // A postgres that died leaves its lock behind.
      beforeRestart: (signal) => folder.clearStaleLock(signal),
    },
    valkey: {
      step: 'cache',
      noun: 'cache',
      port: 'valkey',
      readyMs: 30_000,
      every: 100,
      choosePort: () => freePort(),
      launch: (port) => {
        fs.mkdirSync(data.valkeyDir, { recursive: true, mode: 0o700 })
        // A file rather than `--requirepass`, which any local account can read off `ps`.
        fs.writeFileSync(data.valkeyConf, plan.valkeyConf({ port, password: secrets.valkeyPassword, dir: data.valkeyDir }), { mode: 0o600 })
        return { command: layout.valkey, args: [data.valkeyConf], env: plan.baseEnv(env), cwd: data.valkeyDir, graceSeconds: 5 }
      },
      ready: (handle, port) => canConnect(port),
      notReady: () => 'The cache did not start.',
    },
    server: {
      step: 'server',
      noun: 'server',
      port: 'server',
      readyMs: 5 * 60_000,
      every: 250,
      choosePort: async () => {
        const port = await stablePort(loadPort())
        savePort(port)
        return port
      },
      portTaken: () => savePort(0),
      launch: (port) => ({
        command: layout.python,
        args: plan.serverArgs({ port }),
        cwd: layout.app,
        env: serverEnv(),
        graceSeconds: plan.SERVER_STOP_GRACE_SECONDS,
      }),
      ready: async (handle, port, signal) => plan.healthAnswered(await httpStatus(`http://127.0.0.1:${port}/health`, { signal })),
      notReady: (ended) => (ended ? 'The server stopped while starting.' : 'The server did not answer in time.'),
    },
  }
  const labelOf = (name) => `The ${SERVICES[name].noun}`

  const view = () => ({
    state,
    steps: steps.map((s, i) => ({ ...s, status: statusOf(s, i) })),
    failure,
    origin,
    logsDir: data.logs,
  })
  const changed = () => events.emit('change', view())

  function statusOf(step, i) {
    if (step.id === failedStep) return 'failed'
    if (i < reached) return 'done'
    if (i === reached) return state === 'failed' ? 'pending' : 'running'
    return 'pending'
  }

  function activate(id) {
    reached = steps.findIndex((s) => s.id === id)
    logFor('supervisor').note(`step: ${id}`)
    changed()
  }

  function fail(err) {
    state = 'failed'
    failedStep = err.step || 'prepare'
    const logName = err.log || 'supervisor'
    failure = {
      step: failedStep,
      label: steps.find((s) => s.id === failedStep)?.label || failedStep,
      message: err.message,
      dataNote: plan.dataNoteText(err.dataNote),
      log: null,
      tail: '',
    }
    // The logs folder can be what failed, and the failure still has to be shown.
    try {
      failure.log = logFor(logName).file
      failure.tail = logFor(logName).tail()
      logFor('supervisor').note(`failed at ${failedStep}: ${err.message}`)
    } catch {}
    changed()
  }

  // -------------------------------------------------------------------------
  // The bundled tree
  // -------------------------------------------------------------------------

  function manifestFile() {
    const raw = readText(layout.manifest)
    return raw === null ? null : JSON.parse(raw)
  }

  function readManifest() {
    const parsed = manifestFile()
    if (parsed === null) throw new StepError('prepare', `This install is missing its bundled parts (${layout.manifest}).`)
    // A package built for the other architecture fails inside Python's dynamic
    // loader with a message that names a .so. Say what actually went wrong.
    if (parsed.arch !== process.arch) {
      throw new StepError('prepare', `This install was built for ${parsed.arch} and this Mac runs ${process.arch}. Download the ${process.arch} build.`)
    }
    return parsed
  }

  function majorOf() {
    if (env.LANGALPHA_DESKTOP_PG_MAJOR) return Number(env.LANGALPHA_DESKTOP_PG_MAJOR)
    return manifest.postgres.major
  }

  function serverEnv() {
    return plan.backendEnv({
      layout,
      from: env,
      db,
      redisUrl: `redis://:${secrets.valkeyPassword}@127.0.0.1:${ports.valkey}/0`,
      byokKey: secrets.byokKey,
      relaySecret: secrets.relaySecret,
      ownerSecret,
      serviceToken,
      port: ports.server,
      pythonMinor: manifest.python.minor,
      platform,
    })
  }

  // -------------------------------------------------------------------------
  // Children
  // -------------------------------------------------------------------------

  /**
   * One service up and answering, for a boot and a restart alike. A boot picks a
   * port and moves on from one someone else took between choosing and binding; a
   * restart keeps the port, since the others hold connections to it.
   */
  async function startService(name, signal, { restart = false } = {}) {
    const service = SERVICES[name]
    const log = logFor(name)
    for (let attempt = 0; attempt < (restart ? 1 : BIND_RETRIES); attempt++) {
      signal.throwIfAborted()
      if (!restart) ports[service.port] = await service.choosePort()
      signal.throwIfAborted()
      const port = ports[service.port]
      const handle = startChild({ name, log, ...service.launch(port) })
      // Registered before it is ready, so a stop meanwhile finds it.
      children.set(name, handle)
      const ready = await pollUntil(() => service.ready(handle, port, signal), {
        ms: service.readyMs, every: service.every, signal, alive: () => !handle.ended,
      })
      // A server that answered and died before the answer was read is not up.
      if (ready && !handle.ended) return handle
      // Read before the stop below, which marks the handle as stopped by us.
      const endedOnItsOwn = handle.ended
      children.delete(name)
      await handle.stop()
      if (handle.error) throw new StepError(service.step, `Could not start the ${service.noun}: ${handle.error.message}`, { log: name })
      if (!plan.looksLikeBindFailure(log.tail())) throw new StepError(service.step, service.notReady(endedOnItsOwn), { log: name })
      if (restart) break
      log.note(`port ${port} was taken; trying another`)
      service.portTaken?.()
    }
    throw new StepError(service.step, `${labelOf(name)} could not get a free port.`, { log: name })
  }

  /** A one-off command, stopped by its deadline or the run's abort. */
  const runTask = (options, signal) => runToEnd({ cwd: layout.app, env: serverEnv(), ...options }, { signal, timeoutMs: MIGRATE_MS })

  /** Everything this run started, in reverse of starting. */
  async function sweep() {
    for (const name of Object.keys(SERVICES).reverse()) {
      const handle = children.get(name)
      if (!handle) continue
      children.delete(name)
      await handle.stop()
    }
  }

  // -------------------------------------------------------------------------
  // Migrations
  // -------------------------------------------------------------------------

  /**
   * Before migrations that are new to this data, or to a database this app does
   * not manage: an older release opened on data a newer one migrated stops here,
   * with nothing changed, rather than partway through `alembic upgrade`.
   */
  async function refuseNewerData({ snapshotTaken, signal }) {
    const log = logFor('migrate')
    log.note('checking the database was not migrated by a newer version')
    const handle = await runTask({ name: 'migrate-check', command: layout.python, args: ['-c', plan.REVISION_CHECK], log, graceSeconds: 5 }, signal)
    if (handle.code === 0 && !handle.error) return
    // The copy is of data nothing has changed, and a launch would restore it.
    if (snapshotTaken) folder.dropSnapshot()
    const dataNote = externalDbUrl ? 'external' : 'untouched'
    if (handle.code === plan.UNKNOWN_REVISION_EXIT) throw new StepError('migrate', plan.NEWER_DATA_MESSAGE, { log: 'migrate', dataNote })
    // A check that did not answer proves nothing, and a database the app does not
    // manage has no snapshot to fall back on, so nothing is migrated.
    const why = handle.error ? handle.error.message : `exit ${handle.code ?? handle.signal}`
    throw new StepError('migrate', `The database's version could not be checked (${why}).`, { log: 'migrate', dataNote })
  }

  async function migrate({ snapshotTaken, firstRun, signal }) {
    if (!firstRun && (snapshotTaken || externalDbUrl)) await refuseNewerData({ snapshotTaken, signal })
    const log = logFor('migrate')
    log.note('alembic upgrade head')
    const handle = await runTask({ name: 'migrate', command: layout.python, args: ['-m', 'alembic', 'upgrade', 'head'], log, graceSeconds: 10 }, signal)

    if (handle.code === 0 && !handle.error) {
      if (!externalDbUrl) {
        // In this order: a crash anywhere between then only repeats the snapshot,
        // never rolls back to data a launch thinks is up to date, and never leaves
        // an older release's digest standing on data without its copy.
        folder.clearMigrated()
        folder.dropSnapshot()
        folder.markMigrated(manifest.migrationsDigest)
      }
      return
    }

    const dataNote = plan.dataAfterFailedMigration({ firstRun, snapshotTaken, externalDb: Boolean(externalDbUrl) })
    if (dataNote === 'restored') {
      // Postgres has the folder open, so it stops before the folder goes back.
      const pg = children.get('postgres')
      children.delete('postgres')
      await pg?.stop()
      folder.restore()
    }
    const why = handle.error ? handle.error.message : `exit ${handle.code ?? handle.signal}`
    throw new StepError('migrate', `A database migration did not finish (${why}).`, { log: 'migrate', dataNote })
  }

  // -------------------------------------------------------------------------
  // Staying up
  // -------------------------------------------------------------------------

  /**
   * Attached only once a child is up, so a death before that is the failure of
   * the step that started it, and one after it is restarted, on a widening delay.
   */
  function watch(handle, run) {
    handle.done.then(() => {
      if (handle.stopping || run.signal.aborted) return
      children.delete(handle.name)
      logFor('supervisor').note(`${handle.name} ended unexpectedly (${handle.code ?? handle.signal})`)
      restart(handle.name, run)
    })
  }

  async function restart(name, run) {
    const { signal } = run
    for (;;) {
      // Elapsed time, which a change to the clock does not move.
      const decision = plan.nextRestart(restartHistory.get(name) || [], performance.now(), restartPolicy)
      restartHistory.set(name, decision.history)
      if (decision.delay === null) {
        fail(new StepError(SERVICES[name].step, `${labelOf(name)} keeps stopping, so the app has stopped restarting it.`, { log: name }))
        // The run is over: the others are left up for the failure page's logs, but
        // nothing more is restarted until the next start.
        run.abort()
        return
      }
      try {
        await sleep(decision.delay, undefined, { signal, ref: false })
        await SERVICES[name].beforeRestart?.(signal)
        watch(await startService(name, signal, { restart: true }), run)
        logFor('supervisor').note(`${name} restarted`)
        return
      } catch (err) {
        if (signal.aborted) return
        logFor('supervisor').note(`restarting ${name} failed: ${err.message}`)
      }
    }
  }

  /**
   * After sleep the processes are all still there, with their connections
   * possibly not. Postgres and Valkey on loopback come through it fine, so the one
   * worth checking is whether the server still answers. One that does not is
   * killed, which `watch` then treats like any other death.
   */
  async function afterWake() {
    if (state !== 'ready') return
    const { signal } = current
    // The server this check is about. One that dies meanwhile is replaced, and
    // the replacement, not ready yet, must not pay for the failed probes.
    const server = children.get('server')
    try {
      await sleep(wakeSettleMs, undefined, { signal, ref: false })
      const answered = await pollUntil(async () => plan.healthAnswered(await httpStatus(`${origin}/health`, { timeoutMs: 5000, signal })), {
        tries: 4, every: wakeRetryMs, signal, alive: () => state === 'ready',
      })
      if (answered || state !== 'ready') return
    } catch (err) {
      if (signal.aborted) return
      throw err
    }
    if (!server || server.ended || signal.aborted || children.get('server') !== server) return
    logFor('supervisor').note('the server stopped answering after a wake; restarting it')
    server.child.kill('SIGKILL')
  }

  // -------------------------------------------------------------------------
  // Start and stop
  // -------------------------------------------------------------------------

  /** The stack up, returning its services' handles to watch. */
  async function boot(signal) {
    logFor('supervisor').note(`starting from ${treeRoot}`)
    for (const found of plan.dotenvsAbove(layout.app, fs.existsSync)) {
      logFor('supervisor').note(`ignoring ${found}: this backend is configured by the app, not by .env files`)
    }

    const externalDb = Boolean(externalDbUrl)
    // A guess until the data folder has been looked at, so a failure on the way
    // there still has a step to name.
    steps = plan.stepsFor({ firstRun: false, externalDb })
    activate('prepare')
    manifest = readManifest()
    secrets = folder.loadSecrets()
    const firstRun = !externalDb && await folder.prepare(majorOf(), signal)
    steps = plan.stepsFor({ firstRun, externalDb })
    changed()
    signal.throwIfAborted()

    const up = []
    let snapshotTaken = false
    if (externalDb) {
      db = plan.parseDbUrl(externalDbUrl)
    } else {
      if (firstRun) {
        activate('init')
        await folder.initialize({ initdb: layout.initdb, major: majorOf(), password: secrets.pgPassword, signal })
      } else if (folder.migratedDigest() !== manifest.migrationsDigest) {
        // Skipped on a launch with the same migrations as last time, which have nothing to run.
        await folder.takeSnapshot(signal)
        snapshotTaken = true
      }
      activate('database')
      up.push(await startService('postgres', signal))
      db = { host: '127.0.0.1', port: ports.pg, user: plan.PG_USER, password: secrets.pgPassword, name: plan.PG_DB }
    }

    activate('cache')
    up.push(await startService('valkey', signal))

    activate('migrate')
    await migrate({ snapshotTaken, firstRun, signal })

    activate('server')
    up.push(await startService('server', signal))
    origin = `http://127.0.0.1:${ports.server}`

    // Nothing watches a child until the whole stack is up, since a step's own
    // failure reports it, and a child that died after its step finished reported nothing.
    for (const handle of up) {
      if (handle.ended) throw new StepError(SERVICES[handle.name].step, `${labelOf(handle.name)} stopped while the rest was starting.`, { log: handle.name })
    }
    return up
  }

  function start() {
    if (state === 'ready') return Promise.resolve({ ok: true, origin })
    // The start a stop is waiting on is the one it cancelled, so this one begins
    // after the stop rather than answering with that start's result, unless a
    // later stop cancels it too.
    if (stopping) {
      const asked = stops
      return stopping.then(() => (stops === asked ? start() : { ok: false, failure }))
    }
    if (starting) return starting
    current?.abort()
    const run = new AbortController()
    current = run
    starting = (async () => {
      try {
        if (stopping) await stopping
        run.signal.throwIfAborted()
        state = 'starting'
        steps = []
        reached = 0
        failedStep = null
        failure = null
        origin = null
        ports.pg = null
        ports.valkey = null
        restartHistory.clear()
        changed()
        // After a failure at runtime the survivors are still up, and one of them
        // holds the data folder.
        await sweep()
        // A quit or a switch to a server during that wait cancelled this run.
        run.signal.throwIfAborted()
        const up = await boot(run.signal)
        run.signal.throwIfAborted()
        state = 'ready'
        reached = steps.length
        for (const handle of up) watch(handle, run)
        logFor('supervisor').note(`ready at ${origin}`)
        changed()
        return { ok: true, origin }
      } catch (err) {
        await sweep()
        if (!run.signal.aborted) fail(err)
        return { ok: false, failure }
      } finally {
        starting = null
      }
    })()
    return starting
  }

  /**
   * Everything stopped, in order and within a deadline. Safe to call in any state.
   * A start in flight is aborted and waited for, since it may be about to launch
   * a child the sweep would otherwise miss.
   */
  function stop() {
    stops++
    current?.abort()
    if (stopping) return stopping
    if (state !== 'failed') state = 'stopped'
    const inFlight = starting
    stopping = (async () => {
      await inFlight
      await sweep()
    })().finally(() => {
      stopping = null
      changed()
    })
    return stopping
  }

  return {
    start,
    stop,
    view,
    afterWake,
    on: (name, fn) => events.on(name, fn),
    get state() { return state },
    get origin() { return origin },
    get ownerSecret() { return ownerSecret },
    /** Add the owner's secret to `session`'s requests to this server (`owner.js`). */
    proveOwner(session) { owner.proveOwner(session, this) },
    get logsDir() { return data.logs },
    get dataDir() { return data.stack },
    /** What the bundled tree was built from, or null when its manifest cannot say. */
    source() {
      try {
        const parsed = manifestFile()
        return parsed && parsed.source && typeof parsed.source.commit === 'string' ? parsed.source : null
      } catch {
        return null
      }
    },
    /** For tests and the log: which service runs as which pid. */
    pids: () => Object.fromEntries([...children].map(([name, handle]) => [name, handle.child.pid])),
    ports: () => ({ ...ports }),
  }
}

module.exports = { createSupervisor, StepError, httpStatus }
