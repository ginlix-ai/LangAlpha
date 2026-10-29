'use strict'

// ---------------------------------------------------------------------------
// What remote control decides, apart from what it does: the URL a pairing may
// name, how the helper's state file reads, and what the bridge and the menu say
// about it. Pure, like `policy`: nothing here touches the folder, the login item
// or a process.
// ---------------------------------------------------------------------------

/**
 * What the bridge's `getRemoteControl` answers and what `enable` and `disable`
 * carry, mirrored by `RemoteControlStatus` in web/src/lib/desktop.ts. A page
 * that is not the app's is answered null instead, never a status.
 *
 * @typedef {object} RemoteControlStatus
 * @property {boolean} supported
 * @property {'platform'|'os_version'|'not_packaged'} [reason] Why not, when unsupported.
 * @property {'on'|'off'|'needs_approval'} service The login item; `off` is never registered, or paused.
 * @property {'unpaired'|'connecting'|'connected'|'refused'|null} helper The running helper's own
 *   state; null when the item is off or the helper is gone.
 * @property {boolean} paired Whether the helper holds a paired tunnel.
 * @property {string|null} fingerprint The helper's SSH host key fingerprint; null before pairing.
 * @property {string} account The macOS account the agent acts as.
 */

// `not-registered` and `not-found` both read as off. The second is also what
// SMAppService says of a service it holds no record of, so a fresh install that
// has never registered may answer it; whether the bundle carries the agent at
// all is asked of the bundle instead (see `unsupportedReason`).
const SERVICE = { enabled: 'on', 'requires-approval': 'needs_approval' }

// The states reported as the helper's. `stopped`, which it writes on its way out
// after launchd stops it, is reported as no state, like a helper that is gone.
const HELPER_STATES = new Set(['unpaired', 'connecting', 'connected', 'refused'])

const SCRIPT_PATH = /^\/api\/v1\/computers\/ssh\/pairings\/[0-9a-fA-F]{16,128}\/script$/

/** Why this shell cannot run the helper, or null. `packaged` is whether the bundle carries its plist. */
function unsupportedReason({ platform, systemVersion, packaged }) {
  if (platform !== 'darwin') return 'platform'
  // SMAppService arrived in 13. The app itself runs on 12.
  if (!(Number.parseInt(systemVersion, 10) >= 13)) return 'os_version'
  if (!packaged) return 'not_packaged'
  return null
}

/** SMAppService's word for the login item, as the status says it. */
function serviceOf(item) {
  return SERVICE[item] || 'off'
}

function isLoopbackHost(hostname) {
  const host = hostname.replace(/^\[|\]$/g, '')
  if (host === 'localhost' || host === '::1') return true
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host)
}

/**
 * The pairing script URL in the form the helper is handed, or null if it is not
 * one.
 *
 * Structural, not same-origin: a deployment serves the pairing from its API
 * host, which need not be the host the app is on. Whatever server the URL names
 * is the one this Mac will take SSH logins from, which is why `enable` puts that
 * host in front of the user before anything happens. Rebuilt from origin and
 * path, never passed through, so the one line written to D/pairing is exactly
 * what was checked: an empty `?` or `#` survives in `href` and nowhere else.
 */
function scriptUrlFrom(raw) {
  if (typeof raw !== 'string') return null
  let u
  try {
    u = new URL(raw)
  } catch {
    return null
  }
  if (u.username || u.password) return null
  if (raw.includes('?') || raw.includes('#')) return null
  // Cleartext only where it never leaves the machine. The pairing hands the
  // helper the key the backend will log in with, and a tunnel to whoever
  // answers.
  if (u.protocol !== 'https:' && !(u.protocol === 'http:' && isLoopbackHost(u.hostname))) return null
  if (!SCRIPT_PATH.test(u.pathname)) return null
  return `${u.origin}${u.pathname}`
}

/**
 * `key=value` lines. Split on the first `=` only, since `pairing_detail` is the
 * deployment's own sentence and may carry one.
 */
function parseState(text) {
  const state = {}
  for (const line of String(text).split(/\r?\n/)) {
    const at = line.indexOf('=')
    if (at <= 0) continue
    state[line.slice(0, at).trim()] = line.slice(at + 1).trim()
  }
  return state
}

/** A pid worth waiting on: never 0 or 1, which name a group or launchd, never ours. */
function pidOf(state) {
  if (!state || !/^[1-9]\d{0,9}$/.test(state.pid || '')) return null
  const pid = Number(state.pid)
  return pid > 1 && pid !== process.pid ? pid : null
}

/**
 * The status of a shell that can run the helper, all but `account`, from the
 * login item, the helper's last state and a way to ask whether a pid still runs.
 *
 * A helper that is gone has no state of its own, whatever it last wrote: a
 * SIGKILL or a crash leaves `connected` behind, and only the pid can tell. A
 * state with no usable pid cannot be checked, so it is taken at its word. With
 * the item on, launchd is about to start another, which writes afresh.
 */
function toStatus(service, state, isAlive) {
  const pid = pidOf(state)
  const running = service !== 'off' && !!state && HELPER_STATES.has(state.state) && (pid === null || isAlive(pid))
  return {
    supported: true,
    service,
    helper: running ? state.state : null,
    paired: !!state?.tunnel_id,
    fingerprint: state?.fingerprint || null,
  }
}

/**
 * Nothing read from the folder. A dev run shares it with the installed app of
 * its edition, and reporting that app's pairing as this shell's would put a
 * "This Mac" badge on a row this shell cannot pause or turn off.
 */
function unsupportedStatus(reason) {
  return { ...toStatus('off', null), supported: false, reason }
}

/**
 * What the app menu shows for remote control, or null for nothing: a switch
 * wherever a helper is paired or the login item is on, so a pairing left running
 * when the app quit mid-wait still has a way to be stopped.
 */
function menuView(status) {
  if (!status?.supported || (!status.paired && status.service === 'off')) return null
  if (status.service === 'off') return { line: 'Paused', paused: true }
  if (status.service === 'needs_approval') return { line: 'Allow in System Settings', paused: false }
  // The deployment refuses a tunnel only once it is gone, so `refused` is a Mac
  // removed from LangAlpha somewhere else: the app turns it off the next time
  // it lists computers, and until then nothing can reach it.
  if (!status.paired || status.helper === 'refused') return { line: 'Not connected', paused: false }
  return { line: status.helper === 'connected' ? 'On' : 'Connecting', paused: false }
}

module.exports = {
  unsupportedReason, serviceOf, scriptUrlFrom, parseState, pidOf, toStatus, unsupportedStatus, menuView,
}
