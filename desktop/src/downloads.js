'use strict'

const { app, dialog } = require('electron')
const notify = require('./notify')

// Sessions, not windows: the app and the console share the default session, and
// attaching twice would report every download twice. A WeakSet so a session that
// goes away is not held alive by having been seen.
const attached = new WeakSet()

/**
 * Give a download a visible ending.
 *
 * Electron already prompts for a location, so the missing half is what happens
 * afterwards. A browser has a download shelf that reports progress, completion
 * and failure; a shell with no chrome at all shows none of it, which leaves an
 * interrupted transfer looking exactly like a finished one. The user's next move
 * is to go hunting in Finder for a file that never arrived.
 *
 * Reports the two endings the user cannot otherwise see and stays silent on the
 * third: a download they cancelled themselves needs no dialog telling them so.
 */
/**
 * A filename fit to put inside a dialog that wears the app's own name.
 *
 * The name comes off the wire (Content-Disposition, or the page's own download
 * attribute) and both macOS and Linux allow newlines in one. Interpolated
 * whole, a name ending in a blank line and a sentence composes extra lines of
 * text in a native dialog the user reads as the application speaking.
 */
function displayName(raw) {
  return String(raw || 'the file').replace(/\s+/g, ' ').trim().slice(0, 80) || 'the file'
}

function attach(session) {
  if (!session || attached.has(session)) return
  attached.add(session)

  session.on('will-download', (_event, item) => {
    // Read now rather than in the handler below: after an interrupted transfer
    // the item still answers, but this is the name the user was shown.
    const name = displayName(item.getFilename())
    console.log(`[download] started ${name}`)

    item.once('done', (_doneEvent, state) => {
      if (state === 'completed') {
        console.log(`[download] completed ${name}`)
        notify.fileLanded(item.getSavePath())
        return
      }
      if (state === 'interrupted') {
        console.log(`[download] interrupted ${name}`)
        dialog.showErrorBox(
          app.getName(),
          `The download of ${name} did not finish.\n\nCheck your connection and try again.`,
        )
      }
    })
  })
}

module.exports = { attach }
