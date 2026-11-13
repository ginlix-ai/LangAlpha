'use strict'

const electron = require('electron')

// Quitting, and what closing the last of the user's windows means.
//
// The hidden menu bar window keeps `window-all-closed` from firing once it
// exists, so every user window's close asks `afterUserWindowClosed` instead.
// And it hides on close rather than closing, so a quit has to say it is one
// (`isQuitting`) before Electron's sweep reaches it.
//
// Saying so commits to nothing. Electron closes every window and quits only if
// all of them went, so a page's `beforeunload` (the app holds one over unsaved
// file edits) can still cancel. Everything that tears down registers with
// `onWillQuit`, which runs only once the quit is certain: an app that kept
// running without its icon, update checks or sign-in callback is a broken one.

/**
 * `userWindowOpen()` says whether any of the user's windows is still open.
 * `keepInTray()` says whether the app stays in the notification area instead of
 * quitting, where `traits.lastWindowClosed` is `'tray'`. `onQuitCancelled()`
 * puts back what a cancelled quit's sweep closed.
 */
function createLifecycle({
  traits, userWindowOpen, keepInTray = () => false, onQuitCancelled = () => {},
  app = electron.app, autoUpdater = () => electron.autoUpdater,
}) {
  let quitting = false
  const teardown = []

  function markQuitting() {
    quitting = true
  }

  /**
   * Hear a user window's page refuse to unload. Nothing here lets it through,
   * so a refusal during a quit is the quit cancelled: the app keeps running, so
   * it keeps its menu bar window and icon. Outside a quit it is only a window
   * that stays open, which needs nothing.
   */
  function watchQuitCancel(win) {
    win.webContents.on('will-prevent-unload', () => {
      if (!quitting) return
      quitting = false
      console.log('[shell] a page cancelled the quit; the app keeps running')
      onQuitCancelled()
    })
  }

  /**
   * Every user window's `closed` comes here: the app, the account window, the
   * server picker, and the Market Glance window. Never the panel or the hidden
   * menu bar window, which are the shell's.
   */
  function afterUserWindowClosed() {
    if (quitting || traits.lastWindowClosed === 'stay') return
    if (userWindowOpen()) return
    if (traits.lastWindowClosed === 'tray' && keepInTray()) return
    app.quit()
  }

  /** `fn` runs once the quit is certain, in the order registered. */
  function onWillQuit(fn) {
    teardown.push(fn)
  }

  function install() {
    app.on('before-quit', markQuitting)
    if (traits.updateClosesWindowsFirst) {
      try {
        autoUpdater().on('before-quit-for-update', markQuitting)
      } catch {
        // No native updater in this runtime (the test stub).
      }
    }
    // Only reached before the menu bar window exists (an app never reached, or
    // an OSS build with no server yet); after that `afterUserWindowClosed` decides.
    app.on('window-all-closed', () => {
      if (traits.lastWindowClosed !== 'stay') app.quit()
    })
    app.on('will-quit', () => {
      for (const fn of teardown) fn()
    })
  }

  return { isQuitting: () => quitting, watchQuitCancel, afterUserWindowClosed, onWillQuit, install }
}

module.exports = { createLifecycle }
