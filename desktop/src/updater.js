'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron')
const config = require('./config')
const origins = require('./origins')
const store = require('./store')

// ---------------------------------------------------------------------------
// Auto-update.
//
// Two ways a build learns where to look, and it needs exactly one:
//   * `app-update.yml`, written into the package by electron-builder when a
//     `publish` target was configured at build time. This is the normal path.
//   * `updateFeed` in the desktop config, which overrides it. This exists so a
//     feed can be pointed somewhere else without rebuilding the packaging config.
//
// A build with neither is not broken, it is a build that updates by download.
// The menu item says so rather than pretending to check.
//
// Two modes, because an unsigned build can still be useful here:
//   * `auto` downloads and installs. Applying a macOS update needs a build
//     signed with a Developer ID, so this mode is only honest once one exists.
//   * `notify` never downloads. `checkForUpdates` with autoDownload off is a
//     manifest fetch and a semver compare; Squirrel is handed nothing, so no
//     signature is involved and it works on an unsigned preview. It exists so a
//     preview install is not a dead end: when GA ships, the app is the channel
//     that tells the user, with a route to the real download.
//
// The modes are uniform across platforms on purpose. Windows can apply an
// unsigned update and macOS cannot, but a preview that silently upgrades itself
// on one OS and asks on another is two behaviours to explain and to test. A
// preview may also point at different origins than GA, and moving someone's
// backend without their say-so is not a thing to do quietly.
// ---------------------------------------------------------------------------

// Long, because the alternative is asking a machine that is asleep most of the
// day. A shell stays open for days at a time, so this is not a startup-only job.
const CHECK_INTERVAL_MS = 6 * 60 * 60 * 1000

let autoUpdater = null
let checking = false
// The timed check in flight. electron-updater hands a check made meanwhile the
// same answer, so a check after a channel pick waits it out to ask the new one.
let background = null
let timer = null
// What `init` was given: the stop a pre-quit install waits for, and whether a
// restart takes this app's own server down with it.
let hooks = { beforeInstall: () => null, stopsServer: () => false }

const notifyOnly = config.updateMode === 'notify'

// ---------------------------------------------------------------------------
// Channels, in the open-source edition. Its feed carries two: stable releases
// (`latest*.yml`) and nightly builds of main (`nightly*.yml`). An install follows
// the channel its running version was published on until the person picks the
// other under Update Channel in the menu. The hosted edition's feed has stable
// releases only.
//
// Never a downgrade, whichever way the person moves: a newer build has already
// migrated the bundled stack's database, and an older backend cannot run on it.
// Moving from nightly to stable keeps the nightly until a stable release newer
// than it is out.
// ---------------------------------------------------------------------------

/** The channels a person picks between, each with the name its manifests carry. */
const CHANNELS = { stable: 'latest', nightly: 'nightly' }

// On a Mac, electron-updater hands a downloaded update to Squirrel.Mac when
// `autoInstallOnAppQuit` is on at that moment (MacUpdater), and from then on it
// installs at the next quit whatever the flag says. The version handed over, so
// a channel pick can say it still installs rather than claim to hold it back.
let staged = null

// A channel picked while a manual check was out, and the window to answer in.
// That check asked the channel the person left, so its answer is dropped and
// the picked one is asked once it settles.
let recheck = null

/**
 * The channel a version was published on, as desktop-nightly.yml names its
 * builds. Null for another prerelease (`0.4.0-rc.1`), which electron-builder
 * publishes under manifests of its own (`rc*.yml`) and names in the build's
 * `app-update.yml`.
 */
function versionChannel(version = app.getVersion()) {
  if (/-nightly(\.|$)/.test(version)) return 'nightly'
  return version.includes('-') ? null : 'stable'
}

/** Whether this install can move between channels: the open-source edition, with a feed. */
function canPickChannel() {
  return !config.isSaas && isConfigured()
}

/**
 * The channel this install follows: the person's pick, else its running
 * version's. Null for a release candidate nobody has moved, which keeps
 * following its own manifests.
 */
function currentChannel() {
  const picked = store.get('updateChannel')
  return Object.hasOwn(CHANNELS, picked) ? picked : versionChannel()
}

function applyChannel(updaterInstance) {
  if (config.isSaas) return
  const channel = currentChannel()
  if (channel) updaterInstance.channel = CHANNELS[channel]
  // The setter turns downgrades on, and a downgrade is the one move a bundled
  // stack's data cannot survive.
  updaterInstance.allowDowngrade = false
}

// ---------------------------------------------------------------------------
// The page's update button. Once an update is ready to install (auto) or found
// (notify), every window showing the app hears it on `shell:update-state`, and
// the sidebar offers it with what a restart will do. A window whose page has
// asked for the state shows that button, so the native prompt is kept for a
// page that never asks (a web app from before the button) and is never shown on
// top of it.
// ---------------------------------------------------------------------------

/** `idle`, `available` (notify: found, not downloaded) or `ready` (auto: downloaded). */
let update = { status: 'idle', version: null }
// Each window whose page shows the button, with what forgets it at the next document.
const showing = new WeakMap()

/** The release page of `version`, from the releases page a build was given; null without one. */
function notesUrl(version) {
  if (!config.releasesPage || !version) return null
  // The tags desktop-release.yml and desktop-nightly.yml publish under.
  const tag = versionChannel(version) === 'nightly' ? 'desktop-oss-nightly' : `desktop-oss-v${version}`
  return `${config.releasesPage.replace(/\/+$/, '')}/tag/${encodeURIComponent(tag)}`
}

function updateView() {
  return {
    status: update.status,
    version: update.version,
    currentVersion: app.getVersion(),
    channel: canPickChannel() ? currentChannel() : 'stable',
    // Notify mode's button sends the person to the download page instead.
    mode: notifyOnly ? 'notify' : 'auto',
    notesUrl: notesUrl(update.version),
    stopsServer: Boolean(hooks.stopsServer()),
  }
}

function appWindows() {
  return BrowserWindow.getAllWindows().filter((win) => !win.isDestroyed() && origins.isApp(win.webContents.getURL()))
}

function setUpdate(next) {
  update = next
  const view = updateView()
  for (const win of appWindows()) win.webContents.send('shell:update-state', view)
}

/** Whether a window showing the app shows the update button, which a native prompt would only repeat. */
function pageShowsUpdates() {
  return appWindows().some((win) => showing.has(win))
}

function hideUpdates(win) {
  const forget = showing.get(win)
  if (!forget) return
  showing.delete(win)
  win.webContents.removeListener('did-navigate', forget)
}

/**
 * The button's click: restart into a downloaded update, or in notify mode open
 * the download page. Anything else is a stale click and does nothing.
 */
async function installFromPage() {
  if (update.status === 'ready' && !notifyOnly) {
    const updaterInstance = load()
    if (updaterInstance) await restartToInstall(updaterInstance, hooks.beforeInstall)
  } else if (update.status === 'available' && notifyOnly) {
    await shell.openExternal(config.downloadPage)
    store.set('updateNotifiedVersion', update.version)
  }
}

/** `admit(event)` answers the window asking if its page is the app's own. */
function registerIpc(admit) {
  ipcMain.handle('shell:get-update-state', (event) => {
    const win = admit(event)
    if (!win) return null
    // Each document asks for itself as it boots, so a new one, which may be a
    // web app without the button, starts out not showing it.
    if (!showing.has(win)) {
      const forget = () => hideUpdates(win)
      showing.set(win, forget)
      win.webContents.once('did-navigate', forget)
    }
    return updateView()
  })
  // The page took the button down without a new document, as when a narrow
  // window drops the sidebar, so the native prompt is the person's again.
  ipcMain.handle('shell:hide-updates', (event) => {
    const win = admit(event)
    if (win) hideUpdates(win)
  })
  ipcMain.handle('shell:install-update', (event) => (admit(event) ? installFromPage() : null))
}

/** The packaged feed descriptor, absent in a dev run and in an unpublished build. */
function bakedFeedPath() {
  return path.join(process.resourcesPath || '', 'app-update.yml')
}

/**
 * Can this build update itself at all? Deliberately not `config.updateFeed`
 * alone: a build packaged with a publish target carries its feed in
 * `app-update.yml` and needs no config entry.
 */
function isConfigured() {
  if (!app.isPackaged) return false
  if (config.updateFeed) return true
  try {
    return fs.existsSync(bakedFeedPath())
  } catch {
    return false
  }
}

function load() {
  if (autoUpdater) return autoUpdater
  try {
    ({ autoUpdater } = require('electron-updater'))
  } catch {
    return null
  }
  // Both off in notify mode, and that is the whole mechanism: nothing is ever
  // fetched, so nothing is ever handed to Squirrel to validate.
  autoUpdater.autoDownload = !notifyOnly
  // The alternative is restarting under a user mid-turn, and a research turn
  // runs for minutes. Install on the next launch instead.
  autoUpdater.autoInstallOnAppQuit = !notifyOnly
  autoUpdater.logger = { info: log, warn: log, error: log, debug: () => {} }
  if (config.updateFeed) autoUpdater.setFeedURL(config.updateFeed)
  applyChannel(autoUpdater)
  return autoUpdater
}

function log(message) {
  console.log(`[updater] ${typeof message === 'string' ? message : JSON.stringify(message)}`)
}

/**
 * Notify mode's one piece of UI. Shared by the timer and the menu item so the
 * user is told the same thing however they got here.
 */
async function announce(version, win) {
  if (!win || win.isDestroyed()) return false
  const { response } = await dialog.showMessageBox(win, {
    type: 'info',
    buttons: ['Later', 'Download'],
    defaultId: 1,
    cancelId: 0,
    title: 'Update available',
    message: `LangAlpha ${version} is available.`,
    detail: `This build does not update itself. You are on ${app.getVersion()}; download the new version to move over.`,
  })
  // config.downloadPage is guaranteed by src/config.js, which refuses to launch
  // a notify build without one.
  if (response === 1) {
    try {
      await shell.openExternal(config.downloadPage)
    } catch (err) {
      // Nothing opened, so nothing was announced. Returning false leaves
      // `updateNotifiedVersion` unset, and the user is told again next time
      // rather than having the notice retired for a page they never saw.
      console.error(`[updater] could not open the download page: ${err.message}`)
      return false
    }
  }
  return true
}

/**
 * Restart Now. Where the updater installs before it quits, `beforeInstall` runs
 * first and is waited for: electron-updater's `BaseUpdater.quitAndInstall` calls
 * `install()` and only then `app.quit()`, and an AppImage's install starts the new
 * version at once (`AppImageUpdater.doInstall`, `isForceRunAfter`), which would
 * find the old one's stack still holding its data and its single-instance lock.
 *
 * A Mac installs after it quits. `MacUpdater.quitAndInstall` hands over to
 * Electron's `autoUpdater.quitAndInstall`, which closes the windows and then
 * calls `app.quit()`, and Squirrel's ShipIt replaces the bundle only once this
 * process has ended. So the quit's own drain (`lifecycle.onDrain`) stops the
 * stack in order there, as on the install-on-quit path, and a page that cancels
 * the quit keeps a stack that was never stopped.
 */
async function restartToInstall(updaterInstance, beforeInstall, platform = process.platform) {
  if (platform !== 'darwin') await beforeInstall()
  updaterInstance.quitAndInstall()
}

/**
 * `beforeInstall()` answers a promise to wait for before an update that installs
 * ahead of the quit replaces the app, or null.
 */
function init(getWindow, { beforeInstall = () => null, stopsServer = () => false } = {}) {
  hooks = { beforeInstall, stopsServer }
  if (!isConfigured()) {
    console.log(`[updater] no feed for this build (packaged=${app.isPackaged}); updates are manual`)
    return
  }
  const updaterInstance = load()
  if (!updaterInstance) return

  if (notifyOnly) {
    // `update-available` rather than `update-downloaded`: with autoDownload off
    // the latter never fires, so listening for it would be a check that finds an
    // update and then says nothing.
    updaterInstance.on('update-available', async (info) => {
      // A manual check gets this event too, before `checkForUpdates()` resolves,
      // and that path announces for itself — with different rules, since asking
      // on purpose overrides having been told already. Without this the user who
      // clicked "Check for Updates" was shown the same dialog twice.
      if (checking) return
      setUpdate({ status: 'available', version: info.version })
      if (pageShowsUpdates()) return
      if (store.get('updateNotifiedVersion') === info.version) return
      // Recorded only once the dialog has actually been on screen. Marking it
      // first meant a check that landed with no window open — an OSS first run
      // still on the server picker, or macOS with every window closed — retired
      // the notice for a version the user was never shown, permanently.
      try {
        if (!(await announce(info.version, getWindow()))) return
        store.set('updateNotifiedVersion', info.version)
      } catch (err) {
        // An async listener on an EventEmitter has nobody to reject to, and
        // Node's default for that is to end the process. Failing to announce an
        // update is not worth the running app.
        console.error(`[updater] could not announce ${info.version}: ${err.message}`)
      }
    })
  } else {
    updaterInstance.on('update-downloaded', (info) => {
      // A channel pick holds back an update downloaded before it. A download
      // already under way when the person picked finishes after the pick, so
      // one from the channel they left is held back too, unoffered.
      const left = canPickChannel() && versionChannel(info.version) !== currentChannel()
      updaterInstance.autoInstallOnAppQuit = !left
      if (left) return
      if (process.platform === 'darwin') staged = info.version
      setUpdate({ status: 'ready', version: info.version })
      if (pageShowsUpdates()) return
      const win = getWindow()
      if (!win || win.isDestroyed()) return
      dialog.showMessageBox(win, {
        type: 'info',
        buttons: ['Later', 'Restart Now'],
        defaultId: 0,
        title: 'Update ready',
        message: `LangAlpha ${info.version} is ready to install.`,
        detail: 'It will be applied the next time you quit, or you can restart now.',
      }).then(async ({ response }) => {
        if (response === 1) await restartToInstall(updaterInstance, hooks.beforeInstall)
      }).catch((err) => console.error(`[updater] update prompt failed: ${err.message}`))
    })
  }

  // Silent on purpose. A background check that cannot reach its feed is not
  // something to interrupt a user over; the menu item reports honestly when
  // they ask.
  updaterInstance.on('error', (err) => console.error(`[updater] ${err.message}`))

  const check = () => {
    const run = updaterInstance.checkForUpdates()
      .catch((err) => console.error(`[updater] ${err.message}`))
      .finally(() => { if (background === run) background = null })
    background = run
    return run
  }
  check()
  clearInterval(timer)
  timer = setInterval(check, CHECK_INTERVAL_MS)
}

function stop() {
  clearInterval(timer)
  timer = null
}

/** Menu-driven check. Always answers, including the boring answers. */
async function checkManually(win) {
  if (checking) return
  checking = true
  try {
    if (!isConfigured()) {
      return void dialog.showMessageBox(win, {
        type: 'info',
        title: 'Updates',
        message: 'This build does not update itself.',
        detail: `You are on ${app.getVersion()}. Download a newer build from the releases page.`,
      })
    }
    const updaterInstance = load()
    if (!updaterInstance) {
      return void dialog.showMessageBox(win, {
        type: 'warning',
        title: 'Updates',
        message: 'The updater is unavailable in this build.',
      })
    }

    if (background) await background
    const result = await updaterInstance.checkForUpdates()
    if (recheck) return
    // `isUpdateAvailable` rather than comparing versions ourselves: `updateInfo`
    // describes the newest build in the feed whether or not it is newer than
    // this one, so a string compare calls a rollback an update and offers to
    // download it. The updater has already applied the channel and semver rules
    // this build was configured with; this is the same answer the background
    // check acts on.
    const version = result?.updateInfo?.version
    if (!result?.isUpdateAvailable || !version) {
      // A nightly on the stable channel is ahead of every stable release until
      // the next one passes it, and "the latest version" would read as if the
      // pick had not taken.
      const ahead = currentChannel() === 'stable' && versionChannel() === 'nightly'
      dialog.showMessageBox(win, {
        type: 'info',
        title: 'Updates',
        message: ahead
          ? `LangAlpha ${app.getVersion()} is newer than the latest stable release.`
          : `LangAlpha ${app.getVersion()} is the latest version.`,
        ...(ahead ? { detail: 'It moves to the next stable release once one newer than it is out. An older version cannot run on the data this one has upgraded.' } : {}),
      })
    } else if (notifyOnly) {
      setUpdate({ status: 'available', version })
      // Announced first, recorded second, for the reason the background handler
      // gives above: the window can be gone by the time the check resolves, and
      // `announce` then shows nothing. Recording first would retire the notice
      // for a version the user never saw, and the timer would never raise it
      // again.
      if (await announce(version, win)) store.set('updateNotifiedVersion', version)
    } else if (update.status === 'ready' && update.version === version) {
      const { response } = await dialog.showMessageBox(win, {
        type: 'info',
        buttons: ['Later', 'Restart Now'],
        defaultId: 0,
        title: 'Updates',
        message: `LangAlpha ${version} is ready to install.`,
        detail: 'It will be applied the next time you quit, or you can restart now.',
      })
      if (response === 1) await restartToInstall(updaterInstance, hooks.beforeInstall)
    } else {
      dialog.showMessageBox(win, {
        type: 'info',
        title: 'Updates',
        message: `LangAlpha ${version} is downloading.`,
        detail: pageShowsUpdates()
          ? 'Update ready shows in the sidebar once it has downloaded.'
          : 'You will be asked once it is ready to install.',
      })
    }
  } catch (err) {
    if (recheck) return
    dialog.showMessageBox(win, {
      type: 'warning',
      title: 'Updates',
      message: 'Could not check for updates.',
      detail: err.message,
    })
  } finally {
    checking = false
    if (recheck) {
      const next = recheck
      recheck = null
      void checkManually(next.win)
    }
  }
}

/**
 * Update Channel in the menu. Saved before the check, so a check that fails
 * still leaves the pick for the next timed one, and checked at once so the
 * person sees what the new channel offers.
 */
function pickChannel(name, win) {
  if (!Object.hasOwn(CHANNELS, name) || name === currentChannel()) return
  store.set('updateChannel', name)
  const updaterInstance = load()
  if (updaterInstance) {
    applyChannel(updaterInstance)
    // An update the other channel already downloaded would still install on
    // quit, so the pick would not take. The next download, from this channel,
    // turns installing on quit back on.
    updaterInstance.autoInstallOnAppQuit = false
  }
  if (staged) {
    const label = name === 'nightly' ? 'Nightly' : 'Stable'
    return dialog.showMessageBox(win, {
      type: 'info',
      title: 'Update Channel',
      message: `LangAlpha ${staged} installs when you quit.`,
      detail: `It downloaded before you picked ${label}. Updates after it come from ${label}.`,
    })
  }
  // Whatever the other channel found is no longer what a restart installs.
  setUpdate({ status: 'idle', version: null })
  if (checking) {
    recheck = { win }
    return
  }
  return checkManually(win)
}

module.exports = {
  init, stop, checkManually, isConfigured, restartToInstall, registerIpc,
  canPickChannel, currentChannel, pickChannel, versionChannel,
}
