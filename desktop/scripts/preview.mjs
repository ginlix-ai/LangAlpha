#!/usr/bin/env node
/**
 * Run the shell against a web build that has not deployed yet.
 *
 * A remote-URL shell shows whatever is deployed, so working on the two of them
 * together has a blind spot: the packaged app loads the *live* bundle, and every
 * frame decision the shell makes is a reaction to that bundle rather than to the
 * source sitting in this worktree. That is how the window buttons ended up on the
 * app's own logo and the window stopped being draggable: both were the shell
 * correctly reacting to a deployed build that predates the drag strip.
 *
 * This builds `web/` and serves it on loopback so the shell can be pointed at it.
 * What you see is what the next deploy looks like.
 *
 *   node scripts/preview.mjs --backend http://127.0.0.1:8050
 *   node scripts/preview.mjs --no-build           # reuse web/dist
 *
 * The frontend is built the way the environment says, so a hosted preview is a
 * matter of pointing at that environment's web env file and at a backend running
 * the same mode:
 *
 *   node scripts/preview.mjs --web-env ../../web/.env \
 *     --platform https://platform.example.com --backend http://127.0.0.1:8000
 */
import { spawn, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import https from 'node:https'
import net from 'node:net'
import tls from 'node:tls'
import os from 'node:os'
import path from 'node:path'
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'

const require = createRequire(import.meta.url)
const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.join(here, '..')
const web = path.join(root, '..', 'web')
const dist = path.join(web, 'dist')
const buildConfig = path.join(root, 'config', 'build.json')

const argv = process.argv.slice(2)
const flag = (name, fallback) => {
  const at = argv.indexOf(`--${name}`)
  return at === -1 ? fallback : argv[at + 1]
}
const port = Number(flag('port', 5399))
// `--port` with nothing after it is `undefined`, and `Number(undefined)` is NaN,
// which `listen` reads as "any free port": the preview then runs somewhere other
// than the address it just printed and pointed the shell at.
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  console.error(`[preview] --port takes a number between 1 and 65535, got '${flag('port', '') ?? ''}'`)
  process.exit(1)
}
// Worth a flag: a console on the same hostname shares cookies with the preview
// across ports, which is what lets a locally-served pair behave like the two
// subdomains do in production.
const host = flag('host', '127.0.0.1')
const backend = flag('backend', defaultBackend())
// A hosted backend is a normal thing to preview against, and the proxy below
// has to speak whichever protocol this one is.
const upstreamUrl = new URL(backend)
const upstream = upstreamUrl.protocol === 'https:' ? https : http
const upstreamPort = upstreamUrl.port || (upstreamUrl.protocol === 'https:' ? 443 : 80)
const platformOrigin = flag('platform', null)
const webEnvFile = flag('web-env', null)
const shouldBuild = !argv.includes('--no-build')

/**
 * VITE_* from another environment's env file, for building the frontend the way
 * that environment builds it. Inline variables still win, which is also Vite's
 * own precedence, so a single override does not mean restating the whole file.
 */
function webEnv() {
  if (!webEnvFile) return {}
  const text = fs.readFileSync(path.resolve(webEnvFile), 'utf8')
  const vars = {}
  for (const line of text.split('\n')) {
    const match = line.match(/^\s*(VITE_[A-Z0-9_]+)\s*=\s*(.*)$/)
    if (match) vars[match[1]] = match[2].trim().replace(/^["']|["']$/g, '')
  }
  return vars
}

/** The worktree's own stack, which is the honest default even when it is down. */
function defaultBackend() {
  try {
    const env = fs.readFileSync(path.join(root, '..', '.env'), 'utf8')
    const match = env.match(/^BACKEND_PORT=(\d+)/m)
    if (match) return `http://127.0.0.1:${match[1]}`
  } catch {
    // No worktree .env: the base stack's port is the only sensible guess.
  }
  return 'http://127.0.0.1:8000'
}

// ---------------------------------------------------------------------------

if (shouldBuild) {
  const env = { ...webEnv(), ...process.env }
  const mode = env.VITE_HOST_MODE || 'oss'
  console.log(`[preview] building web/ (${mode} mode)…`)
  const built = spawnSync('pnpm', ['build'], { cwd: web, stdio: 'inherit', shell: true, env })
  if (built.status !== 0) process.exit(built.status ?? 1)
}
if (!fs.existsSync(path.join(dist, 'index.html'))) {
  console.error(`[preview] no build at ${path.relative(process.cwd(), dist)}; drop --no-build`)
  process.exit(1)
}

if (!(await reachable(backend))) {
  console.error(`[preview] nothing is answering at ${backend}`)
  console.error('[preview] start a stack, or pass --backend http://127.0.0.1:<port>')
  process.exit(1)
}

const origin = `http://${host}:${port}`

const TYPES = {
  '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png',
  '.json': 'application/json', '.html': 'text/html; charset=utf-8', '.woff2': 'font/woff2',
  '.ico': 'image/x-icon', '.map': 'application/json',
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, origin)
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws/')) return proxy(req, res)

  const file = path.join(dist, path.normalize(url.pathname))
  if (file.startsWith(dist) && fs.existsSync(file) && fs.statSync(file).isFile()) {
    res.writeHead(200, { 'content-type': TYPES[path.extname(file)] || 'application/octet-stream' })
    fs.createReadStream(file).pipe(res)
    return
  }
  // A missing build file 404s instead of falling back to the shell, which is what
  // the edge is being changed to do. Serving HTML here would hide the exact class
  // of bug this preview exists to catch, one layer earlier than production does.
  if (url.pathname.startsWith('/assets/')) {
    res.writeHead(404, { 'content-type': 'text/plain', 'cache-control': 'no-store' })
    res.end('not found')
    return
  }
  res.writeHead(200, { 'content-type': TYPES['.html'] })
  fs.createReadStream(path.join(dist, 'index.html')).pipe(res)
})

// Streamed rather than buffered: a turn arrives as SSE, and a proxy that waits
// for the response to finish is a preview in which the agent never answers.
function proxy(req, res) {
  const out = upstream.request({
    host: upstreamUrl.hostname, port: upstreamPort, path: req.url, method: req.method,
    headers: { ...req.headers, host: upstreamUrl.host },
  }, (up) => {
    res.writeHead(up.statusCode || 502, up.headers)
    // `pipe` does not forward an error. An upstream reset after the headers are
    // out would leave the browser holding an open response, which for SSE means
    // the app's reconnect never fires; destroying delivers the EOF that starts it.
    up.on('error', () => res.destroy())
    up.pipe(res)
  })
  out.on('error', () => { if (!res.headersSent) res.writeHead(502).end('{}') })
  req.pipe(out)
}

const HOP_BY_HOP = new Set(['connection', 'keep-alive', 'proxy-authenticate',
  'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade'])

server.on('upgrade', (req, socket, head) => {
  const out = upstream.request({
    host: upstreamUrl.hostname, port: upstreamPort, path: req.url, method: req.method,
    headers: { ...req.headers, host: upstreamUrl.host },
  })
  out.on('upgrade', (up, upSocket, upHead) => {
    socket.write(`HTTP/1.1 101 Switching Protocols\r\n${Object.entries(up.headers)
      .map(([k, v]) => `${k}: ${v}`).join('\r\n')}\r\n\r\n`)
    if (upHead?.length) socket.unshift(upHead)
    upSocket.pipe(socket).pipe(upSocket)
  })
  // A backend that answers a WebSocket handshake with an ordinary response
  // (a 401 on the market-data socket is the common one) emits `response`, not
  // `upgrade`. With nothing listening the client is told nothing at all and
  // waits for its own timeout, which reads as a hang rather than as a refusal.
  out.on('response', (up) => {
    socket.write(`HTTP/1.1 ${up.statusCode} ${up.statusMessage}\r\n`)
    for (let i = 0; i < up.rawHeaders.length; i += 2) {
      // Node hands this body over already de-chunked, so forwarding the
      // upstream's `transfer-encoding: chunked` would describe framing that is
      // no longer on the wire and leave a parser waiting for a terminator that
      // never comes. The rest are hop-by-hop headers that describe the
      // connection we just terminated rather than the message.
      if (HOP_BY_HOP.has(up.rawHeaders[i].toLowerCase())) continue
      socket.write(`${up.rawHeaders[i]}: ${up.rawHeaders[i + 1]}\r\n`)
    }
    // With the framing headers gone, end-of-body is end-of-connection, which
    // also means an upstream that dies mid-body has to close this socket rather
    // than leave the client reading a body that will never end.
    socket.write('connection: close\r\n\r\n')
    up.on('error', () => socket.destroy())
    up.pipe(socket)
  })
  out.on('error', () => socket.destroy())
  if (head?.length) out.write(head)
  out.end()
})

await new Promise((resolve, reject) => {
  server.once('error', (err) => {
    if (err.code !== 'EADDRINUSE') return reject(err)
    // Worth a sentence rather than a stack: dev servers collect on this range,
    // and the fix is a flag, not a debugging session.
    console.error(`[preview] port ${port} is taken; pass --port <n>`)
    process.exit(1)
  })
  server.listen(port, host, resolve)
})
console.log(`[preview] ${origin} → ${path.relative(process.cwd(), dist)}, api → ${backend}`)
if (platformOrigin) console.log(`[preview] saas edition, console at ${platformOrigin}`)

// A user-data dir of its own, and this is not tidiness. The shell records what it
// learned about the page it loaded; sharing the installed app's dir would teach
// the *installed* app that its frontend reserves the window-button strip, when the
// build it actually loads may not, which is the buttons-on-the-logo bug.
const userData = path.join(os.tmpdir(), 'langalpha-desktop-preview')
fs.mkdirSync(userData, { recursive: true })
const settingsFile = path.join(userData, 'settings.json')
const settings = () => {
  try {
    return JSON.parse(fs.readFileSync(settingsFile, 'utf8'))
  } catch {
    return {}
  }
}
fs.writeFileSync(settingsFile, JSON.stringify({
  ...settings(),
  serverUrl: origin,
  // A hosted preview enters at the app rather than at platform sign-in. The real
  // funnel, correctly, sends a signed-in customer to the *deployed* app, which
  // would walk the user straight out of the build they came here to look at.
  reachedApp: !!platformOrigin,
}, null, 2))

// Stashed rather than overwritten: a build.json in this tree belongs to a package
// someone is building.
const stashed = fs.existsSync(buildConfig) ? fs.readFileSync(buildConfig) : null
if (platformOrigin) {
  fs.mkdirSync(path.dirname(buildConfig), { recursive: true })
  fs.writeFileSync(buildConfig, `${JSON.stringify({
    edition: 'saas', appOrigin: origin, platformOrigin: new URL(platformOrigin).origin,
  }, null, 2)}\n`)
} else {
  fs.rmSync(buildConfig, { force: true })
}

let child = null
const cleanup = () => {
  // Both directions. Leaving a written build.json behind would hand the next
  // `pnpm start` in this tree a saas edition pointed at a preview server that is
  // no longer listening, which is the same wrong-build confusion this script
  // exists to remove.
  if (stashed) fs.writeFileSync(buildConfig, stashed)
  else fs.rmSync(buildConfig, { force: true })
  server.close()
}
process.on('SIGINT', () => { child?.kill(); cleanup(); process.exit(0) })

const electron = require('electron')
const launch = () => spawn(electron, ['.', `--user-data-dir=${userData}`], { cwd: root, stdio: 'inherit' })

try {
  // titleBarStyle is fixed when the window is constructed, so a shell that has
  // never seen this build shows a standard titlebar for one run no matter what
  // the page reserves. That first run is the probe; the window worth looking at
  // is the one after it.
  if (settings().appChrome !== true) {
    console.log('[preview] first run against this build: probing whether it reserves the window-button strip')
    child = launch()
    const reserves = await waitFor(() => settings().appChrome === true, 25000)
    child.kill()
    await exited(child)
    console.log(reserves
      ? '[preview] it does; opening the window it will actually get'
      : '[preview] it does not, so the frame keeps a standard titlebar (that is the fallback working)')
  }
  child = launch()
  await exited(child)
} finally {
  cleanup()
}

/**
 * Waiting on `exit` alone hangs on the two cases where there is nothing left to
 * wait for: a process that had already gone by the time the listener attached,
 * and one that never started, where node emits `error` and no `exit` at all.
 */
function exited(proc) {
  if (proc.exitCode !== null || proc.signalCode !== null) return Promise.resolve()
  return new Promise((resolve) => {
    proc.once('exit', resolve)
    proc.once('error', (err) => {
      console.error(`[preview] could not run electron: ${err.message}`)
      resolve()
    })
  })
}

async function waitFor(predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return true
    await new Promise((r) => setTimeout(r, 250))
  }
  return false
}

function reachable(url) {
  const { hostname, port: p, protocol } = new URL(url)
  const secure = protocol === 'https:'
  return new Promise((resolve) => {
    const socket = secure
      ? tls.connect({ host: hostname, servername: hostname, port: Number(p) || 443 })
      : net.connect({ host: hostname, port: Number(p) || 80 })
    const done = (ok) => { socket.destroy(); resolve(ok) }
    socket.setTimeout(1500)
    socket.on(secure ? 'secureConnect' : 'connect', () => done(true))
    socket.on('error', () => done(false))
    socket.on('timeout', () => done(false))
  })
}
