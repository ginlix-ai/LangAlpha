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
// In `auto` the person can still turn automatic updates off (`automaticUpdates`
// in the desktop settings, from Settings or the update button's panel). Off, a
// check still runs and the update is offered, but nothing downloads until the
// person asks, and only an update they asked for installs on quit. Checking
// stays on so someone who never updates is still told there is a newer build.
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
let hooks = { beforeInstall: () => null, stopsServer: () => false, getWindow: () => null }

const notifyOnly = config.updateMode === 'notify'

/** Whether updates download and install by themselves: `auto` mode, unless the person turned it off. */
function automatic() {
  return !notifyOnly && store.get('desktop').automaticUpdates !== false
}

/** Whether there is an automatic update to turn off: a build that downloads its own. */
function canTurnOffAutomatic() {
  return !notifyOnly && isConfigured()
}

// The version the person asked to download with automatic updates off. That one
// installs on quit; one downloaded before they turned it off does not.
let requested = null

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
/** Downloads held back from a channel the person left, so `download()` can tell its transfer was another's. */
let heldBack = 0
// Off a Mac, whether electron-updater has added its quit handler, which it
// does once a download lands with `autoInstallOnAppQuit` up (BaseUpdater).
let quitHandler = false
// The newest version a check found, which is what `downloadUpdate()` fetches
// next even while the page still offers an older one.
let found = null

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
// The page's update button. Once an update is ready to install, or found where
// nothing downloads it by itself (notify mode, or automatic updates off), every
// window showing the app hears it on `shell:update-state`, and
// the sidebar offers it with what a restart will do. A window whose page has
// asked for the state shows that button, so the native prompt is kept for a
// page that never asks (a web app from before the button) and is never shown on
// top of it.
// ---------------------------------------------------------------------------

/**
 * `idle`; `available`, found and not downloaded (notify mode, or automatic
 * updates off); `downloading`, which the person asked for; or `ready`,
 * downloaded and installed by a restart.
 */
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
    automatic: automatic(),
    installsOnQuit: installsOnQuit(),
    notesUrl: notesUrl(update.version),
    stopsServer: Boolean(hooks.stopsServer()),
  }
}

function appWindows() {
  return BrowserWindow.getAllWindows().filter((win) => !win.isDestroyed() && origins.isApp(win.webContents.getURL()))
}

function setUpdate(next) {
  update = next
  // A ready update is the one a restart installs: a newer one downloading
  // behind it would take over the installer, and a transfer that then failed
  // would leave Restart Now with nothing to run. The newer one comes after.
  if (autoUpdater && !notifyOnly) autoUpdater.autoDownload = automatic() && update.status !== 'ready'
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

// How long a page that took the button down has to put it back, as a sidebar
// does when it collapses to its rail and mounts the button anew, and how long a
// new document has to boot and ask for it.
const REMOUNT_MS = 1000
const BOOT_MS = 5000
let standIn = null
// The ready update the native prompt has offered, so its own prompt at download
// and the stand-in for a button taken down, as zooming across the phone layout
// does, ask once between them.
let promptedFor = null

/**
 * The page took the button down, by its phone layout or by leaving for a new
 * document that may predate the button. Once no window shows it, the native
 * prompt offers what the button offered, as it would have had the page never
 * shown it.
 */
function buttonGone(wait = REMOUNT_MS) {
  clearTimeout(standIn)
  standIn = setTimeout(() => {
    standIn = null
    if (pageShowsUpdates()) return
    promptNatively(hooks.getWindow()).catch((err) => console.error(`[updater] could not stand in for the button: ${err.message}`))
  }, wait)
}

async function promptNatively(win) {
  const updaterInstance = load()
  if (!updaterInstance || !win || win.isDestroyed()) return
  const { status, version } = update
  if (status === 'ready' && !notifyOnly) return promptReady(updaterInstance, win, version)
  if (status !== 'available' || store.get('updateNotifiedVersion') === version) return
  const answered = notifyOnly ? await announce(version, win) : !automatic() && await offerDownload(version, win)
  if (answered) store.set('updateNotifiedVersion', version)
}

/** The native prompt for a downloaded update. */
async function promptReady(updaterInstance, win, version) {
  if (promptedFor === version) return
  promptedFor = version
  const { response } = await dialog.showMessageBox(win, {
    type: 'info',
    buttons: ['Later', 'Restart Now'],
    defaultId: 0,
    title: 'Update ready',
    message: `LangAlpha ${version} is ready to install.`,
    // Automatic updates turned off while it downloaded keep it from installing on quit.
    detail: installsOnQuit()
      ? 'It will be applied the next time you quit, or you can restart now.'
      : 'Restart to install it.',
  })
  if (response === 1) await restartToInstall(updaterInstance, hooks.beforeInstall)
}

/**
 * The button's click: restart into a downloaded update, download an available
 * one, or in notify mode open the download page. Anything else is a stale click
 * and does nothing.
 */
async function installFromPage() {
  if (update.status === 'ready' && !notifyOnly) {
    const updaterInstance = load()
    if (updaterInstance) await restartToInstall(updaterInstance, hooks.beforeInstall)
  } else if (update.status === 'available' && notifyOnly) {
    await shell.openExternal(config.downloadPage)
    store.set('updateNotifiedVersion', update.version)
  } else if (update.status === 'available') {
    await download({ asked: true })
  }
}

/**
 * Download the available update. `update-downloaded` makes it ready; a failed
 * download offers it again, so the person can retry. `asked` is true when the
 * person pressed Download, which installs on quit even with automatic updates
 * off; turning automatic updates on downloads without that, so turning them
 * off again stops the install on quit.
 */
async function download({ asked }) {
  const updaterInstance = load()
  if (!updaterInstance || update.status !== 'available') return
  const { version } = update
  // Each download says whose it is, so a failed one asked for leaves no mark
  // on the next, which automatic updates may start.
  requested = asked ? version : null
  setUpdate({ status: 'downloading', version })
  try {
    // `downloadUpdate()` hands back a transfer already under way, which after a
    // channel pick is the channel the person left: that one lands held back,
    // and this version's starts once it has.
    for (let tries = 0; ; tries++) {
      const before = heldBack
      await updaterInstance.downloadUpdate()
      if (heldBack === before || update.status !== 'downloading' || update.version !== version) return
      if (tries > 0) throw new Error('another download kept landing first')
    }
  } catch (err) {
    console.error(`[updater] could not download ${version}: ${err.message}`)
    // Offered again as what the next download fetches, which a check run
    // while this one was out may have moved on to a newer version.
    if (update.status === 'downloading' && update.version === version) setUpdate({ status: 'available', version: found ?? version })
  }
}

/**
 * The person turned automatic updates on or off. On, an update already found
 * starts downloading; off, one that downloaded by itself waits for a restart
 * the person chooses rather than installing on quit.
 *
 * Only the update offered as ready installs on quit. electron-updater installs
 * whatever it last downloaded, which after a channel pick is a build the person
 * moved away from; `update-downloaded` sets the flag for the next download.
 */
function setAutomatic() {
  if (notifyOnly || !autoUpdater) return
  const on = automatic()
  // Whether a ready update can install on quit was settled when it landed. On
  // a Mac Squirrel.Mac took it then (`staged`) or never will, and raising the
  // flag would leave `quitAndInstall()` waiting for a hand-off that never
  // comes. Elsewhere electron-updater adds its quit handler only for a download
  // that lands with the flag up, so without one a raised flag installs nothing.
  const installs = process.platform === 'darwin'
    ? update.version === staged
    : quitHandler && (on || update.version === requested)
  autoUpdater.autoInstallOnAppQuit = update.status === 'ready' && installs
  if (on && update.status === 'available') return void download({ asked: false })
  setUpdate(update)
}

/**
 * Whether the downloaded update installs at the next quit. Not when automatic
 * updates were turned off after it downloaded on its own, except on a Mac, where
 * Squirrel.Mac already holds it.
 */
function installsOnQuit() {
  return Boolean(autoUpdater?.autoInstallOnAppQuit) || (update.status === 'ready' && update.version === staged)
}

/** `admit(event)` answers the window asking if its page is the app's own. */
function registerIpc(admit) {
  ipcMain.handle('shell:get-update-state', (event) => {
    const win = admit(event)
    if (!win) return null
    // Each document asks for itself as it boots, so a new one, which may be a
    // web app without the button, starts out not showing it.
    if (!showing.has(win)) {
      const forget = () => {
        hideUpdates(win)
        buttonGone(BOOT_MS)
      }
      showing.set(win, forget)
      win.webContents.once('did-navigate', forget)
    }
    return updateView()
  })
  // The page took the button down without a new document, as when a narrow
  // window drops the sidebar, so the native prompt is the person's again.
  ipcMain.handle('shell:hide-updates', (event) => {
    const win = admit(event)
    if (!win || !showing.has(win)) return
    hideUpdates(win)
    buttonGone()
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
  // fetched, so nothing is ever handed to Squirrel to validate. Off too while
  // the person has automatic updates off, until they ask for one.
  autoUpdater.autoDownload = automatic()
  // The alternative is restarting under a user mid-turn, and a research turn
  // runs for minutes. Install on the next launch instead.
  autoUpdater.autoInstallOnAppQuit = automatic()
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
 * With automatic updates off, the native offer of an update, for a window whose
 * page has no update button. Answers whether the person has had their answer:
 * Later, or a download that went through. A download that failed is offered
 * again at the next check, since that page has nowhere else to retry it.
 */
async function offerDownload(version, win) {
  if (!win || win.isDestroyed()) return false
  const { response } = await dialog.showMessageBox(win, {
    type: 'info',
    buttons: ['Later', 'Download'],
    defaultId: 1,
    cancelId: 0,
    title: 'Update available',
    message: `LangAlpha ${version} is available.`,
    detail: `Automatic updates are off. You are on ${app.getVersion()}; download it now and it installs when you restart.`,
  })
  if (response !== 1) return true
  // A check that found a newer one while the dialog was open offers it for itself.
  if (update.status !== 'available' || update.version !== version) return true
  await download({ asked: true })
  return update.status !== 'available'
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
  hooks = { beforeInstall, stopsServer, getWindow }
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
    // With automatic updates off; on, the download starts by itself and the
    // update is offered once it is ready.
    updaterInstance.on('update-available', async (info) => {
      found = info.version
      // A manual check offers for itself, as in notify mode above.
      if (checking || automatic()) return
      // An update the person is downloading or has downloaded stays offered
      // until they restart into it; a newer one is offered after that.
      if (update.status === 'downloading' || update.status === 'ready') return
      // Offered again at each check until a window has shown the offer: one
      // that found it with no window open showed nothing.
      setUpdate({ status: 'available', version: info.version })
      if (pageShowsUpdates() || store.get('updateNotifiedVersion') === info.version) return
      try {
        if (await offerDownload(info.version, getWindow())) store.set('updateNotifiedVersion', info.version)
      } catch (err) {
        console.error(`[updater] could not offer ${info.version}: ${err.message}`)
      }
    })
    updaterInstance.on('update-downloaded', (info) => {
      // A channel pick holds back an update downloaded before it. A download
      // already under way when the person picked finishes after the pick, so
      // one from the channel they left is held back too, unoffered. With
      // automatic updates off, only the one the person asked for installs on
      // quit.
      const left = canPickChannel() && versionChannel(info.version) !== currentChannel()
      updaterInstance.autoInstallOnAppQuit = !left && (automatic() || info.version === requested)
      if (left) {
        heldBack += 1
        return
      }
      if (updaterInstance.autoInstallOnAppQuit) {
        if (process.platform === 'darwin') staged = info.version
        else quitHandler = true
      }
      setUpdate({ status: 'ready', version: info.version })
      if (pageShowsUpdates()) return
      const win = getWindow()
      if (!win || win.isDestroyed()) return
      promptReady(updaterInstance, win, info.version)
        .catch((err) => console.error(`[updater] update prompt failed: ${err.message}`))
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
  clearTimeout(standIn)
  standIn = null
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
    } else if (update.status === 'ready' && (update.version === version || !automatic())) {
      // With automatic updates off, a downloaded update stays offered until
      // the restart into it, as the timed check keeps it; a newer one is
      // offered after that.
      const { response } = await dialog.showMessageBox(win, {
        type: 'info',
        buttons: ['Later', 'Restart Now'],
        defaultId: 0,
        title: 'Updates',
        message: `LangAlpha ${update.version} is ready to install.`,
        // Turning automatic updates off keeps an update downloaded before then
        // from installing on quit, so only a restart installs it.
        detail: installsOnQuit()
          ? 'It will be applied the next time you quit, or you can restart now.'
          : 'Restart to install it.',
      })
      if (response === 1) await restartToInstall(updaterInstance, hooks.beforeInstall)
    } else if (!automatic() && update.status !== 'downloading') {
      setUpdate({ status: 'available', version })
      if (await offerDownload(version, win)) store.set('updateNotifiedVersion', version)
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
  requested = null
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
  canTurnOffAutomatic, setAutomatic,
}
