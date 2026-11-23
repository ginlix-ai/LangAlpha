'use strict'

/**
 * How the bundled server tells the app from everything else on this Mac.
 *
 * The server has no sign-in and takes a request with no token for its one user.
 * Any container here reaches it as `host.docker.internal`, can name any Host,
 * and arrives from 127.0.0.1, so nothing about the connection says who sent it.
 * Each launch gives the server a secret and adds it, as a header, to the app's
 * own requests to that one origin, in the network layer: no page script sees
 * it, and unlike a cookie for 127.0.0.1 it never goes to another port there,
 * where the agent's previews run. The server's calls to itself carry a service
 * token of the same launch.
 */

const crypto = require('node:crypto')

const OWNER_HEADER = 'X-LangAlpha-Owner'

const newSecret = () => crypto.randomBytes(32).toString('hex')

/** Whether `url`, a page or a websocket, is on the server at `origin`. */
function onServer(url, origin) {
  try {
    const target = new URL(url)
    const server = new URL(origin)
    const scheme = { 'ws:': 'http:', 'wss:': 'https:' }[target.protocol] || target.protocol
    return scheme === server.protocol && target.host === server.host
  } catch {
    return false
  }
}

/** The request headers to send: `headers` with the owner's secret when it goes to the stack's server. */
function withOwner(url, headers, stack) {
  if (!stack.origin || !onServer(url, stack.origin)) return headers
  return { ...headers, [OWNER_HEADER]: stack.ownerSecret }
}

const proven = new WeakSet()

/** Add the secret to `session`'s requests to the stack's server, whenever it runs. */
function proveOwner(session, stack) {
  if (!stack || proven.has(session)) return
  proven.add(session)
  session.webRequest.onBeforeSendHeaders({ urls: ['http://127.0.0.1/*', 'ws://127.0.0.1/*'] }, (details, callback) => {
    callback({ requestHeaders: withOwner(details.url, details.requestHeaders, stack) })
  })
}

module.exports = { OWNER_HEADER, newSecret, withOwner, proveOwner }
