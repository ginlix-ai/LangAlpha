'use strict'

const { BrowserWindow } = require('electron')

const origins = require('./origins')
const { merge, view } = require('./desktopSettings')
const { gatedIpc } = require('./sender')

// The live half of the desktop settings: the stored copy, a patch applied to it,
// and every window showing the app told of the result. Each feature subscribes
// to the change it acts on (`onChange`) rather than being told by a list here.
//
// Separate from desktopSettings.js because the store requires that file, and
// this one requires origins, which requires the store.

/**
 * `loginItem` is loginItem.js's, since the OS has the last word on open at
 * login. `shortcutError()` is why the global shortcut did not register, if it
 * did not.
 */
function createSettingsService({ store, traits, loginItem, shortcutError = () => null }) {
  const listeners = []

  const get = () => store.get('desktop')

  /**
   * The settings with open at login as the OS has it, since the user can
   * change it in System Settings or by deleting the file; null means nothing
   * was registered (a dev run) and the stored choice is all there is.
   */
  function current() {
    const stored = get()
    const registered = loginItem.isEnabled()
    return registered === null ? stored : { ...stored, openAtLogin: registered }
  }

  const viewNow = () => view(current(), { traits, shortcutError: shortcutError() })

  /** Every window showing the app hears the new settings: the main window's page and the menu bar's. */
  function broadcast(v) {
    for (const win of BrowserWindow.getAllWindows()) {
      if (!win.isDestroyed() && origins.isApp(win.webContents.getURL())) {
        win.webContents.send('shell:desktop-settings', v)
      }
    }
  }

  /** Apply `patch`, let every subscriber act on it, and answer the view. */
  function update(patch) {
    const prev = get()
    const next = merge(prev, patch)
    store.set('desktop', next)
    // Applied whenever asked, not only on a change from what was stored: the
    // user may have removed the item in System Settings, and the page then
    // shows it off while the store still says on.
    if (patch && typeof patch === 'object' && 'openAtLogin' in patch) loginItem.set(next.openAtLogin)
    for (const fn of listeners) fn(next, prev)
    const v = viewNow()
    broadcast(v)
    return v
  }

  /** `fn(next, prev)` after every change, from a page or from one of the shell's menus. */
  function onChange(fn) {
    listeners.push(fn)
  }

  /** `admit(event)` answers the window asking if its page is the app's own. */
  function registerIpc(admit) {
    const ipc = gatedIpc(admit)
    ipc.handle('shell:get-desktop-settings', viewNow)
    ipc.handle('shell:set-desktop-settings', update)
  }

  return { get, view: viewNow, update, onChange, registerIpc }
}

module.exports = { createSettingsService }
