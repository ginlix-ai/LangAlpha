'use strict'

/**
 * A packaged app's own launch check, for a release to run before it publishes:
 * the bundled database, cache and server started from the package as a user's
 * first launch starts them, and asked the questions the first screens ask. It is
 * the only test of the tree that runs what the tree carries, which is how a
 * binary that refuses the runner's macOS, or a wheel built for the wrong one,
 * gets caught before a user has it.
 */

const { withOwner } = require('./owner')

const CHECKS = [
  { path: '/api/v1/setup/status', json: (body) => typeof body.needs_setup === 'boolean' },
  { path: '/', text: (body) => body.includes('<div id="root"') },
]

async function runSmoke(stack, { log = console.log } = {}) {
  const started = Date.now()
  const result = await stack.start()
  if (!result.ok) {
    const { failure } = result
    log(`[smoke] the stack did not start: ${failure.step}: ${failure.message}`)
    if (failure.tail) log(failure.tail)
    await stack.stop()
    return false
  }
  log(`[smoke] the stack is up at ${stack.origin} after ${Math.round((Date.now() - started) / 1000)}s`)
  let ok = true
  for (const check of CHECKS) {
    try {
      const url = `${stack.origin}${check.path}`
      const response = await fetch(url, { headers: withOwner(url, {}, stack) })
      const body = check.json ? await response.json() : await response.text()
      const passed = response.status === 200 && (check.json ? check.json(body) : check.text(body))
      log(`[smoke] ${passed ? 'ok  ' : 'FAIL'} ${check.path} (${response.status})`)
      ok &&= passed
    } catch (err) {
      log(`[smoke] FAIL ${check.path}: ${err.message}`)
      ok = false
    }
  }
  await stack.stop()
  return ok
}

module.exports = { runSmoke }
