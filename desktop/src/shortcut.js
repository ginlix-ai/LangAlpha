'use strict'

const { globalShortcut } = require('electron')

// The global shortcut that toggles the menu bar panel. At most one is held, and
// why the last one did not register is kept for the settings page to show.

/** `supported` is `traits.globalShortcut`; `onPress` is what the key does. */
function createShortcut({ supported, onPress }) {
  let registered = ''
  let error = null

  /** Hold `accelerator` instead of whatever was held; '' holds none. */
  function apply(accelerator) {
    if (registered) {
      try {
        globalShortcut.unregister(registered)
      } catch {
        // Gone already.
      }
      registered = ''
    }
    error = null
    if (!accelerator) return
    if (!supported) {
      error = 'This desktop does not let apps register a global shortcut.'
      return
    }
    let ok = false
    try {
      ok = globalShortcut.register(accelerator, onPress)
    } catch {
      ok = false
    }
    if (ok) registered = accelerator
    else error = `${accelerator} could not be registered. Another app may be using it.`
  }

  function dispose() {
    try {
      globalShortcut.unregisterAll()
    } catch {
      // Never registered.
    }
    registered = ''
  }

  return { apply, error: () => error, dispose }
}

module.exports = { createShortcut }
