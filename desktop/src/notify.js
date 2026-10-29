'use strict'

const { app } = require('electron')

/**
 * Tell the desktop a file just landed.
 *
 * The Downloads stack bounce: the platform's own "a file arrived" signal, and
 * the only one that reports without taking focus from a window that may be
 * mid-turn. Every route that writes a file for the user goes through here, so
 * a saved PDF and a finished download announce themselves the same way.
 *
 * Silent everywhere else. Windows and Linux have no equivalent that does not
 * interrupt, and an error box for a success is worse than saying nothing.
 */
function fileLanded(filePath) {
  if (process.platform === 'darwin' && app.dock) app.dock.downloadFinished(filePath)
}

module.exports = { fileLanded }
