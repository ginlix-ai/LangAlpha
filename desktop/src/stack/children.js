'use strict'

const { spawn } = require('node:child_process')
const net = require('node:net')
const { setTimeout: sleep } = require('node:timers/promises')

/**
 * Sits on a pipe only this process holds, and when the pipe closes the parent is
 * gone. So a child outlives its parent by a few seconds at most, however the
 * parent went: a kill -9, a crash, an update that quit past the shutdown path.
 * A shell rather than an Electron process, at about a megabyte each, because
 * Postgres and Valkey read nothing from stdin and cannot be told to watch a pipe
 * themselves, and macOS has no parent-death signal to ask the kernel for.
 *
 * The write end cannot leak into another child: libuv marks its pipes close-on-exec.
 */
const GUARD_SCRIPT = `
cat >/dev/null
kill -0 "$1" 2>/dev/null || exit 0
kill -"$2" "$1" 2>/dev/null
i=0
while [ "$i" -lt "$3" ]; do
  kill -0 "$1" 2>/dev/null || exit 0
  sleep 1
  i=$((i + 1))
done
kill -KILL "$1" 2>/dev/null
`

function guard(pid, signal, graceSeconds, onFailed) {
  const sidecar = spawn('/bin/sh', ['-c', GUARD_SCRIPT, 'langalpha-guard', String(pid), signal.replace(/^SIG/, ''), String(graceSeconds)], {
    stdio: ['pipe', 'ignore', 'ignore'],
  })
  // A guard that never started (a process limit reached between the two spawns)
  // leaves a child nothing would stop after a crash, so that child goes too.
  sidecar.on('error', onFailed)
  sidecar.stdin.on('error', () => {})
  sidecar.unref()
  sidecar.stdin.unref?.()
  return sidecar
}

/**
 * A child process with the sidecar above and its output in `log`.
 *
 * Returned as a small handle because the supervisor asks only three things of a
 * child: has it ended, what did it say, and stop it within a deadline.
 */
const OUTPUT_DRAIN_MS = 1000

function startChild({ name, command, args, cwd, env, log, stopSignal = 'SIGTERM', graceSeconds = 15 }) {
  const child = spawn(command, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] })
  const handle = { name, child, ended: false, code: null, signal: null, stopping: false }

  let sidecar = null
  let guardError = null
  if (child.pid) {
    sidecar = guard(child.pid, stopSignal, graceSeconds, (error) => {
      if (handle.ended) return
      guardError = new Error(`its guard could not start: ${error.message}`)
      child.kill('SIGKILL')
    })
  }

  // Decoded on the stream, which holds back a character split across two
  // chunks, rather than per chunk, which turns its halves into U+FFFD.
  const pipe = (stream) => {
    stream.setEncoding('utf8')
    stream.on('data', (chunk) => log.write(chunk))
  }
  pipe(child.stdout)
  pipe(child.stderr)

  handle.done = new Promise((resolve) => {
    let settled = false
    const finish = (code, signal, error) => {
      if (settled) return
      settled = true
      handle.ended = true
      handle.code = code
      handle.signal = signal
      handle.error = error || null
      if (sidecar) sidecar.kill('SIGKILL')
      resolve(handle)
    }
    child.on('error', (error) => finish(null, null, error))
    // A child's last words can arrive after its exit, and a caller reads them to
    // judge the failure, so the end waits for the pipes to close. Bounded, since a
    // process the child left behind can hold them open.
    const closed = new Promise((resolve) => child.on('close', resolve))
    child.on('exit', (code, signal) => {
      handle.ended = true
      if (sidecar) sidecar.kill('SIGKILL')
      Promise.race([closed, sleep(OUTPUT_DRAIN_MS, undefined, { ref: false })]).then(() => finish(code, signal, guardError))
    })
  })

  /** Ask, wait, then insist. Resolves once it has really ended. */
  handle.stop = async ({ signal = stopSignal, deadlineMs = graceSeconds * 1000 } = {}) => {
    handle.stopping = true
    if (handle.ended) return handle.done
    child.kill(signal)
    const timer = setTimeout(() => { if (!handle.ended) child.kill('SIGKILL') }, deadlineMs)
    timer.unref?.()
    await handle.done
    clearTimeout(timer)
    return handle
  }

  return handle
}

/**
 * A command run to its end, stopped at `timeoutMs` or as soon as `signal` aborts.
 * Resolves the ended handle, or rejects with the abort's reason, so a quit is
 * never read as the command's own failure.
 */
async function runToEnd(options, { signal, timeoutMs = 0 } = {}) {
  signal?.throwIfAborted()
  const handle = startChild(options)
  const stop = () => { handle.stop() }
  signal?.addEventListener('abort', stop, { once: true })
  const timer = timeoutMs ? setTimeout(stop, timeoutMs) : null
  try {
    await handle.done
  } finally {
    clearTimeout(timer)
    signal?.removeEventListener('abort', stop)
  }
  signal?.throwIfAborted()
  return handle
}

/**
 * Asks `check` every `every` ms until it answers yes, for at most `ms` or `tries`,
 * and gives up early once `alive` says no. An abort rejects, even right after a
 * yes, so a run cancelled while it waited never counts as ready.
 */
async function pollUntil(check, { ms = Infinity, tries = Infinity, every, signal, alive = () => true }) {
  // A monotonic clock, which on a Mac stands still while it sleeps, so a lid closed
  // mid-start does not spend the wait.
  const deadline = performance.now() + ms
  for (let i = 0; i < tries && performance.now() < deadline && alive(); i++) {
    signal?.throwIfAborted()
    const yes = await check()
    signal?.throwIfAborted()
    if (yes) return true
    if (i + 1 < tries) await sleep(every, undefined, { signal, ref: false })
  }
  return false
}

/** Does anything accept a TCP connection there. */
function canConnect(port, host = '127.0.0.1', timeoutMs = 1000) {
  return new Promise((resolve) => {
    const socket = net.connect({ port, host })
    const finish = (ok) => { socket.destroy(); resolve(ok) }
    socket.setTimeout(timeoutMs, () => finish(false))
    socket.once('connect', () => finish(true))
    socket.once('error', () => finish(false))
  })
}

/** A port free right now, from the OS. Whoever uses it still has to survive losing the race for it. */
function freePort(host = '127.0.0.1') {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.once('error', reject)
    server.listen(0, host, () => {
      const { port } = server.address()
      server.close(() => resolve(port))
    })
  })
}

function canBind(port, host = '127.0.0.1') {
  return new Promise((resolve) => {
    const server = net.createServer()
    server.once('error', () => resolve(false))
    server.listen(port, host, () => server.close(() => resolve(true)))
  })
}

/**
 * The backend's port, the same one launch after launch whenever it is free.
 * Postgres and Valkey can move every launch, but this one is the app's origin,
 * and a browser keys its storage by origin: a new port is a signed-out app with
 * its drafts and preferences gone. Below the OS's ephemeral ranges (32768 on
 * Linux, 49152 on macOS), so an outgoing connection is unlikely to be holding it
 * when the app comes back.
 */
async function stablePort(saved, { random = Math.random } = {}) {
  if (saved && await canBind(saved)) return saved
  for (let tries = 0; tries < 30; tries++) {
    const port = 20000 + Math.floor(random() * 10000)
    if (await canBind(port)) return port
  }
  return freePort()
}

module.exports = { startChild, runToEnd, pollUntil, canConnect, freePort, canBind, stablePort, GUARD_SCRIPT }
