/**
 * Load a URL into a window, treating a failed navigation as ordinary.
 *
 * A failed navigation is routine here (the server is down, the machine is
 * offline, a newer navigation superseded this one) and `did-fail-load` already
 * turns the ones that matter into the outage screen. Electron 43 absorbs the
 * resulting rejection itself, but the documented contract is that `loadURL`
 * rejects and Node's own default for an unhandled rejection is to end the
 * process, so catching it is the portable form rather than a bug fix. The
 * destroyed-window guard is the other shape, and that one does bite today: the
 * call throws synchronously, which a timer or an awaited probe can walk into.
 *
 * Returns when the load settles, either way. Most callers navigate and forget;
 * one waits, because for it the load is a delivery and the next one must not
 * start until this has arrived.
 */
const { forDisplay } = require('./redact')

function navigate(win, url) {
  if (!win || win.isDestroyed()) return Promise.resolve()
  return win.loadURL(url).catch((err) => {
    // Never `err.message`: Electron builds it as "CODE (errno) loading '<url>'",
    // so it reproduces the query this line exists to keep out of the log. The
    // code and errno are separate properties and carry the whole diagnosis.
    const why = err.code ? `${err.code} (${err.errno})` : 'navigation failed'
    console.warn(`[shell] could not load ${forDisplay(url)}: ${why}`)
  })
}

module.exports = { navigate }
