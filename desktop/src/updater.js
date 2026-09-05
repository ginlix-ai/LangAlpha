'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { app, dialog, shell } = require('electron')
const config = require('./config')
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
let timer = null

const notifyOnly = config.updateMode === 'notify'

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

function init(getWindow) {
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
      const win = getWindow()
      if (!win || win.isDestroyed()) return
      dialog.showMessageBox(win, {
        type: 'info',
        buttons: ['Later', 'Restart Now'],
        defaultId: 0,
        title: 'Update ready',
        message: `LangAlpha ${info.version} is ready to install.`,
        detail: 'It will be applied the next time you quit, or you can restart now.',
      }).then(({ response }) => {
        if (response === 1) updaterInstance.quitAndInstall()
      }).catch((err) => console.error(`[updater] update prompt failed: ${err.message}`))
    })
  }

  // Silent on purpose. A background check that cannot reach its feed is not
  // something to interrupt a user over; the menu item reports honestly when
  // they ask.
  updaterInstance.on('error', (err) => console.error(`[updater] ${err.message}`))

  const check = () => updaterInstance.checkForUpdates().catch((err) => console.error(`[updater] ${err.message}`))
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

    const result = await updaterInstance.checkForUpdates()
    // `isUpdateAvailable` rather than comparing versions ourselves: `updateInfo`
    // describes the newest build in the feed whether or not it is newer than
    // this one, so a string compare calls a rollback an update and offers to
    // download it. The updater has already applied the channel and semver rules
    // this build was configured with; this is the same answer the background
    // check acts on.
    const version = result?.updateInfo?.version
    if (!result?.isUpdateAvailable || !version) {
      dialog.showMessageBox(win, {
        type: 'info',
        title: 'Updates',
        message: `LangAlpha ${app.getVersion()} is the latest version.`,
      })
    } else if (notifyOnly) {
      // Announced first, recorded second, for the reason the background handler
      // gives above: the window can be gone by the time the check resolves, and
      // `announce` then shows nothing. Recording first would retire the notice
      // for a version the user never saw, and the timer would never raise it
      // again.
      if (await announce(version, win)) store.set('updateNotifiedVersion', version)
    } else {
      dialog.showMessageBox(win, {
        type: 'info',
        title: 'Updates',
        message: `LangAlpha ${version} is downloading.`,
        detail: 'You will be asked once it is ready to install.',
      })
    }
  } catch (err) {
    dialog.showMessageBox(win, {
      type: 'warning',
      title: 'Updates',
      message: 'Could not check for updates.',
      detail: err.message,
    })
  } finally {
    checking = false
  }
}

module.exports = { init, stop, checkManually, isConfigured }
