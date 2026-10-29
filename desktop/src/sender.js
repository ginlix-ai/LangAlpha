'use strict'

const { BrowserWindow } = require('electron')

/**
 * The window behind an IPC call, if `accepts` takes it, else null.
 *
 * Every page the shell renders gets the same bridge, so a handler that acts for
 * the asking window judges the window itself, never anything the message says
 * about where it came from.
 */
function senderWindow(event, accepts = () => true) {
  const win = BrowserWindow.fromWebContents(event.sender)
  return win && !win.isDestroyed() && accepts(win) ? win : null
}

module.exports = { senderWindow }
