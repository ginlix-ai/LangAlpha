'use strict'

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { execFileSync } = require('node:child_process')
const { randomBytes } = require('node:crypto')
const { app, dialog, shell, ipcMain } = require('electron')

const config = require('./config')
const origins = require('./origins')
const { forDisplay } = require('./redact')
const { senderWindow } = require('./sender')
const {
  unsupportedReason, serviceOf, scriptUrlFrom, parseState, pidOf, toStatus, unsupportedStatus, menuView,
} = require('./remoteControlState')

// ---------------------------------------------------------------------------
// Remote control of this Mac (Preview).
//
// The bundle carries `langalpha-helper`, registered as an SMAppService agent so
// launchd runs it whether or not the app is open. It holds an outbound tunnel to
// the deployment's SSH gateway and answers the backend's SSH logins itself,
// which is what lets a Mac with no sshd be an SSH computer.
//
// The app and the helper never talk to each other directly. They share one
// folder, D: the app writes `pairing` to ask for a pairing and reads `state` to
// see what the helper is doing, and everything else in there is the helper's.
// launchd owns the process, so all the app ever does to it is turn the login
// item on and off, and nothing here depends on the app staying open.
//
// The one rule the shell keeps on the helper's behalf: the login item is
// registered only while this Mac is paired or waiting for approval. A pairing
// that does not finish is undone, so no helper runs unpaired with nothing to
// turn it off.
// ---------------------------------------------------------------------------

/** What Electron hands `SMAppService agentServiceWithPlistName:`, extension included. */
const SERVICE_NAME = `${config.helperLabel}.plist`

// General > Login Items in System Settings on macOS 13 and up, the one place a
// background item macOS held back can be allowed.
const LOGIN_ITEMS_URL = 'x-apple.systempreferences:com.apple.LoginItems-Settings.extension'

const TIMING = {
  /** Between reads of D/state. The helper notices a new pairing within about a second. */
  poll: 250,
  /** For a state naming this pairing to appear at all, or the helper never started. */
  start: 15_000,
  /** For the pairing to finish, counted from when it was asked for. */
  pair: 90_000,
  /** For the helper to exit after the login item is taken away, before D goes. */
  exit: 5_000,
}

// The helper changes state on its own (a tunnel comes up, a pairing is refused),
// and the menu has no event to hear it by, so it is read on a timer. One small
// file and one login-item query, and only on a build that can run the helper.
const MENU_POLL_MS = 5000

/**
 * D. Built from the home directory rather than `app.getPath('userData')`: the
 * plist names this same path for a helper launchd starts with no app around to
 * ask, and the preview script points userData at a temp dir, so the two would
 * part there.
 */
function helperDir() {
  return path.join(os.homedir(), 'Library', 'Application Support', config.appName, 'helper')
}

/**
 * Where this bundle keeps the agent's plist, or null when there is no bundle of
 * ours. A dev run is Electron's own app, and a mac build packaged without the
 * helper leaves the plist out, so either way the login item has nothing to run.
 */
function bundledPlist() {
  if (!app.isPackaged || !process.resourcesPath) return null
  return path.join(process.resourcesPath, '..', 'Library', 'LaunchAgents', SERVICE_NAME)
}

/**
 * This Mac's hardware UUID, the one the helper writes to D/machine when it
 * makes its keys; null where it cannot be read.
 */
function thisMachine() {
  try {
    const shown = execFileSync('/usr/sbin/ioreg', ['-rd1', '-c', 'IOPlatformExpertDevice'], { encoding: 'utf8', timeout: 5000 })
    return /"IOPlatformUUID" = "([^"]+)"/.exec(shown)?.[1] ?? null
  } catch {
    return null
  }
}

/**
 * Whether D was made on this Mac. One Migration Assistant or a restore carried
 * over from another Mac still holds that Mac's pairing and keys, which the
 * helper drops as it next starts; until then its state is the other Mac's, and
 * read as this one's it would call that Mac's computer this Mac. A folder that
 * names no machine was made by a helper from before it wrote one.
 */
function madeHere(dir, machine) {
  if (!machine) return true
  try {
    return fs.readFileSync(path.join(dir, 'machine'), 'utf8').trim() === machine
  } catch (err) {
    return err.code === 'ENOENT'
  }
}

function readState(dir) {
  let text
  try {
    text = fs.readFileSync(path.join(dir, 'state'), 'utf8')
  } catch {
    return null
  }
  return parseState(text)
}

/**
 * Ask the helper to pair. Staged in D and renamed over `pairing`, because the
 * helper watches for that name and a half-written file is one it would act on.
 * The parent is made with the default mode, since it is the app's own folder and
 * not ours to narrow; only D is 0700.
 */
function writePairing(dir, url, id) {
  fs.mkdirSync(path.dirname(dir), { recursive: true })
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 })
  // mkdir leaves a folder that already existed as it was.
  fs.chmodSync(dir, 0o700)
  const staged = path.join(dir, `.pairing.${process.pid}.${randomBytes(4).toString('hex')}`)
  try {
    fs.writeFileSync(staged, `url=${url}\nid=${id}\n`, { mode: 0o600, flag: 'wx' })
    fs.renameSync(staged, path.join(dir, 'pairing'))
  } catch (err) {
    fs.rmSync(staged, { force: true })
    throw err
  }
}

/**
 * Delete D whole or not at all. A recursive delete that fails partway leaves
 * some of a pairing behind for a helper started again to read as its own, so D
 * is renamed out of the helper's way first and only then emptied.
 */
function removeDir(dir) {
  const aside = `${dir}.removing-${randomBytes(4).toString('hex')}`
  try {
    fs.renameSync(dir, aside)
  } catch (err) {
    if (err.code === 'ENOENT') return
    throw err
  }
  try {
    fs.rmSync(aside, { recursive: true, force: true })
  } catch (err) {
    // D is gone, which is what the helper reads. The rest is litter.
    console.error(`[remote] could not finish deleting the old helper folder: ${err.code || err.message}`)
  }
}

function isRunning(pid) {
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    // EPERM is a process that exists and is not ours to signal.
    return err.code === 'EPERM'
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function account() {
  try {
    return os.userInfo().username
  } catch {
    return ''
  }
}

function openLoginItems() {
  // A URL main picked itself, so it does not go through `openExternally`, whose
  // scheme allowlist is for URLs a page chose.
  shell.openExternal(LOGIN_ITEMS_URL).catch((err) => {
    console.warn(`[remote] could not open Login Items: ${err.code || err.message}`)
  })
}

/**
 * The confirmation that decides whether this Mac pairs at all.
 *
 * Native and in main because it is the one step a page cannot take for the
 * user: the bridge is reachable from any script the app window runs, and the
 * URL it passes names the server that will be able to log in here. Cancel is the
 * default, so a stray Return turns nothing on.
 *
 * `replacing` is a Mac already paired. Its pairing may belong to another
 * LangAlpha account signed in on this same macOS account, whose computer stops
 * working once this one goes ahead, so that is said here too.
 */
async function confirm(win, host, who, { replacing = false } = {}) {
  let detail = `This Mac will connect to ${host}. The agent can run commands and change files as ${who}, `
    + 'even while LangAlpha is closed. Turn it off any time from the LangAlpha menu.'
  if (replacing) {
    detail += '\n\nThis Mac is already connected to LangAlpha, perhaps for another account. '
      + 'Continuing disconnects it, and any computer using it stops working.'
  }
  // No `type`: macOS then shows the app's icon. 'warning' swaps in the
  // system caution triangle, and the page has already shown the warning.
  const options = {
    title: 'Turn on remote control?',
    message: `Let LangAlpha's agent use this Mac as ${who}?`,
    detail,
    buttons: ['Turn On', 'Cancel'],
    defaultId: 1,
    cancelId: 1,
    noLink: true,
  }
  const { response } = win && !win.isDestroyed()
    ? await dialog.showMessageBox(win, options)
    : await dialog.showMessageBox(options)
  return response === 0
}

/** Remote control where the helper cannot run: the same answers every time, and nothing touched. */
function unsupported(dir, reason) {
  const status = () => ({ ...unsupportedStatus(reason), account: account() })
  return {
    dir,
    status,
    enable: async () => ({ ok: false, error: 'unsupported', status: status() }),
    disable: async () => status(),
    pause: status,
    resume: status,
    menuView: () => null,
    watch() {},
    unwatch() {},
  }
}

/**
 * The remote-control half of the shell for one helper folder.
 *
 * The options are the parts of this machine a test has to stand in for: the
 * folder, the OS, the bundle, this Mac's hardware UUID, and how long to wait.
 * Everything else goes through Electron. `onMenuChange` is called when what the
 * app menu shows for remote control has changed, and only then.
 */
function createRemoteControl({
  dir = helperDir(),
  platform = process.platform,
  systemVersion = typeof process.getSystemVersion === 'function' ? process.getSystemVersion() : '',
  agentPlist = bundledPlist(),
  timing = TIMING,
  onMenuChange = () => {},
  machine,
} = {}) {
  const reason = unsupportedReason({ platform, systemVersion, packaged: !!agentPlist && fs.existsSync(agentPlist) })
  if (reason) return unsupported(dir, reason)
  if (machine === undefined) machine = thisMachine()

  // Replaced by every enable the user agreed to and every disable, so a wait
  // still running stands down once a newer call has replaced its file or D has
  // gone, and leaves what it finds to that call.
  let current = new AbortController()
  // One confirmation at a time. A page can call enable in a loop, and each call
  // is a native dialog.
  let confirming = false
  // Enables under way: the pairing each writes is its own to settle.
  let enabling = 0
  let reconciling = false
  // What the menu was last built from, so a change that leaves it as it was
  // rebuilds nothing.
  let shown = null
  let timer = null

  function service() {
    try {
      return serviceOf(app.getLoginItemSettings({ type: 'agentService', serviceName: SERVICE_NAME }).status)
    } catch (err) {
      console.warn(`[remote] could not read the login item: ${err.message}`)
      return 'off'
    }
  }

  // Electron logs a refusal rather than throwing it, so the caller reads the
  // service back afterwards instead of trusting this call. False only for a throw.
  function setLoginItem(openAtLogin) {
    try {
      app.setLoginItemSettings({ openAtLogin, type: 'agentService', serviceName: SERVICE_NAME })
      return true
    } catch (err) {
      console.warn(`[remote] could not ${openAtLogin ? 'register' : 'unregister'} the login item: ${err.message}`)
      return false
    }
  }

  // D's state, none while D is another Mac's.
  const stateOf = () => (madeHere(dir, machine) ? readState(dir) : null)

  const status = () => ({ ...toStatus(service(), stateOf(), isRunning), account: account() })

  const failure = (error, message) => ({ ok: false, error, ...(message ? { message } : {}), status: status() })

  function view() {
    shown = menuView(status())
    return shown
  }

  function notify() {
    const next = menuView(status())
    if (next?.line === shown?.line && next?.paused === shown?.paused) return
    shown = next
    onMenuChange()
  }

  function supersede() {
    current.abort()
    current = new AbortController()
    return current.signal
  }

  /** The first truthy `check()`, or undefined once `ms` have passed or `signal` is aborted. */
  async function waitFor(check, ms, signal) {
    const deadline = Date.now() + ms
    for (;;) {
      if (signal?.aborted) return undefined
      const found = check()
      if (found) return found
      if (Date.now() >= deadline) return undefined
      await sleep(timing.poll)
    }
  }

  /**
   * Turn the helper off and delete D, the pairing, keys and tunnel with it.
   * D goes only once the item has: a helper that would not stop keeps its
   * folder whole, so what is left is the pairing it had rather than half of one.
   */
  async function teardown(signal) {
    const pid = pidOf(stateOf())
    if (service() !== 'off') {
      if (!setLoginItem(false) || service() !== 'off') {
        console.error('[remote] the login item did not unregister')
        return { unregistered: false, removed: false }
      }
      // launchd stops the helper when the item goes. Deleting D under a helper
      // still on its way out would race it writing `state` back, and `stopped`
      // is the last thing it writes, so either that or its exit ends the wait.
      if (pid) await waitFor(() => !isRunning(pid) || stateOf()?.state === 'stopped', timing.exit, signal)
    }
    if (signal.aborted) return { unregistered: true, removed: false }
    try {
      removeDir(dir)
    } catch (err) {
      console.error(`[remote] could not delete the helper folder: ${err.code || err.message}`)
      return { unregistered: true, removed: false }
    }
    return { unregistered: true, removed: true }
  }

  /**
   * How pairing `id` ended: nothing for done, else the error and, for a
   * refusal, the deployment's own sentence. Picked up within `timing.start` or
   * it never started; ended within the rest of `timing.pair` or it timed out.
   * A superseded wait reads as never started.
   */
  async function waitForPairing(id, signal) {
    const deadline = Date.now() + timing.pair
    const ours = () => {
      const state = stateOf()
      return state?.pairing_id === id ? state : undefined
    }
    const ended = () => {
      const state = ours()
      return state?.pairing === 'done' || state?.pairing === 'failed' ? state : undefined
    }
    if (!(await waitFor(ours, timing.start, signal))) return { error: 'not_started' }
    const state = await waitFor(ended, deadline - Date.now(), signal)
    if (signal.aborted) return { error: 'not_started' }
    if (!state) return { error: 'timeout' }
    return state.pairing === 'done' ? {} : { error: 'failed', message: state.pairing_detail }
  }

  /** Turn the item on for pairing `id`, and say how the pairing ended. */
  async function turnOn(id, signal) {
    // Already running, the helper picks the new file up on its own. Registering
    // again where macOS is holding the item for approval would only be refused.
    if (service() === 'off') setLoginItem(true)
    const now = service()
    if (now === 'needs_approval') {
      openLoginItems()
      return { error: 'needs_approval' }
    }
    if (now === 'off') {
      console.warn('[remote] the login item did not register')
      return { error: 'not_started' }
    }
    return waitForPairing(id, signal)
  }

  async function pair(scriptUrl, win) {
    const url = scriptUrlFrom(scriptUrl)
    if (!url) {
      console.warn(`[remote] refusing to pair with '${forDisplay(String(scriptUrl)).slice(0, 120)}'`)
      return failure('bad_url')
    }
    // A hosted build pairs only with its own deployment, so script running in
    // the app, an injected one included, cannot hand this Mac to another
    // server with one click on a dialog. A self-hosted build takes any host:
    // its server may be reached through a tunnel or a Docker host name.
    if (config.isSaas && new URL(url).origin !== config.pairingOrigin) {
      console.warn(`[remote] refusing to pair with ${new URL(url).host}, which is not this build's server`)
      return failure('bad_url')
    }
    if (confirming) return failure('cancelled')
    // Asked of the folder before the dialog, so the user is told what a yes
    // undoes. A pairing file alone is not a paired Mac: that is a pairing still
    // waiting for approval, which a new one simply replaces.
    const replacing = !!stateOf()?.tunnel_id
    confirming = true
    let agreed
    try {
      agreed = await confirm(win, new URL(url).host, account(), { replacing })
    } catch (err) {
      console.warn(`[remote] the confirmation did not open: ${err.message}`)
      agreed = false
    } finally {
      confirming = false
    }
    if (!agreed) return failure('cancelled')

    const signal = supersede()
    // Only now, after the yes. Turning the old pairing off any earlier would
    // break the other account's computer even when this user cancels.
    if (replacing) {
      const was = service()
      const { unregistered, removed } = await teardown(signal)
      if (signal.aborted) return failure('not_started')
      // An old pairing that could not be taken down whole is left running as it
      // was, not half undone: still on if the item would not go, and back on if
      // only its folder would not.
      if (!removed) {
        if (unregistered && was !== 'off') setLoginItem(true)
        return failure('not_replaced')
      }
    }
    const id = randomBytes(16).toString('hex')
    try {
      writePairing(dir, url, id)
    } catch (err) {
      console.error(`[remote] could not write the pairing: ${err.code || err.message}`)
      return failure('not_started')
    }

    const ended = await turnOn(id, signal)
    // One waiting for approval keeps its file: once the user allows the item,
    // the helper starts, finds it, and pairs. A superseded one is the newer
    // call's to settle. Anything else that did not finish is undone.
    if (ended.error && ended.error !== 'needs_approval' && !signal.aborted) await teardown(signal)
    return ended.error ? failure(ended.error, ended.message) : { ok: true, status: status() }
  }

  async function enable(scriptUrl, { win } = {}) {
    enabling += 1
    try {
      return await pair(scriptUrl, win)
    } finally {
      enabling -= 1
      notify()
    }
  }

  /**
   * Turn off a helper left on with nothing paired and nothing to pair. A
   * pairing outlives the enable that wrote it when macOS holds the item for
   * approval or the app quits mid-pairing, so when it then fails no caller is
   * left to undo it. A helper restarted since has forgotten the failure, so
   * this asks only what the folder still holds.
   */
  async function reconcile() {
    if (confirming || enabling || reconciling || service() !== 'on') return
    const state = stateOf()
    if (state?.tunnel_id || state?.pairing === 'running') return
    if (['paired', 'pairing', 'pairing.taken'].some((name) => fs.existsSync(path.join(dir, name)))) return
    reconciling = true
    try {
      console.warn('[remote] the helper is on with nothing paired; turning it off')
      await teardown(current.signal)
    } finally {
      reconciling = false
      notify()
    }
  }

  async function disable() {
    try {
      await teardown(supersede())
    } finally {
      notify()
    }
    return status()
  }

  /** The menu's switch: the login item goes, D and the pairing in it stay. */
  function pause() {
    if (service() !== 'off') setLoginItem(false)
    notify()
    return status()
  }

  function resume() {
    if (service() === 'off') setLoginItem(true)
    if (service() === 'needs_approval') openLoginItems()
    notify()
    return status()
  }

  /** Poll for what the helper changed on its own, and undo one left on unpaired, this launch's included. */
  function watch(ms = MENU_POLL_MS) {
    if (timer) return
    const tick = () => {
      void reconcile()
      notify()
    }
    timer = setInterval(tick, ms)
    tick()
  }

  function unwatch() {
    clearInterval(timer)
    timer = null
  }

  return { dir, status, enable, disable, pause, resume, menuView: view, watch, unwatch }
}

/**
 * The window asking, if it shows the app, else null.
 *
 * Gated like a connector flow: every page the shell renders gets the bridge,
 * and this one hands the Mac to a server. The console is refused too. It has no
 * reason to ask, and each page that can is one more page to trust. The frame is
 * asked too, since a document served with a sandboxing CSP keeps the app's URL
 * but runs as no origin, and a file the agent wrote can be served that way.
 */
function appWindowOf(event) {
  return senderWindow(event, (win) => {
    const from = win.webContents.getURL()
    const frame = event.senderFrame
    if (origins.isApp(from) && frame === win.webContents.mainFrame && origins.isApp(frame.origin)) return true
    console.warn(`[remote] a request from '${forDisplay(from)}' is not the app's; refusing`)
    return false
  })
}

function registerIpc(remote) {
  ipcMain.handle('shell:remote-control-status', (event) => (appWindowOf(event) ? remote.status() : null))

  ipcMain.handle('shell:remote-control-enable', (event, scriptUrl) => {
    const win = appWindowOf(event)
    return win ? remote.enable(scriptUrl, { win }) : null
  })

  ipcMain.handle('shell:remote-control-disable', (event) => (appWindowOf(event) ? remote.disable() : null))
}

module.exports = { createRemoteControl, registerIpc, writePairing, helperDir, SERVICE_NAME, LOGIN_ITEMS_URL }
