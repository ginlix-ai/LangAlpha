'use strict'

const { BrowserWindow, ipcMain } = require('electron')

const origins = require('./origins')

/** A window that is still there. */
const alive = (win) => Boolean(win) && !win.isDestroyed()

/**
 * The window behind an IPC call, if `accepts` takes it, else null.
 *
 * Every page the shell renders gets the same bridge, so a handler that acts for
 * the asking window judges the window itself, never anything the message says
 * about where it came from.
 */
function senderWindow(event, accepts = () => true) {
  const win = BrowserWindow.fromWebContents(event.sender)
  return alive(win) && accepts(win) ? win : null
}

/**
 * The window behind an IPC call if its page is the app's own, else null.
 *
 * The frame is asked as well as the window's URL: a document served with a
 * sandboxing CSP keeps the app's URL but runs as no origin, and a file the agent
 * wrote can be served that way. Only the main frame, so an embedded page under
 * the app cannot speak for it. `onRefuse(win)` hears a window whose page is not
 * the app's.
 */
function appSenderWindow(event, { accepts = () => true, onRefuse } = {}) {
  return senderWindow(event, (win) => {
    const frame = event.senderFrame
    const isApp = Boolean(frame) && frame === win.webContents.mainFrame
      && origins.isApp(win.webContents.getURL()) && origins.isApp(frame.origin)
    if (!isApp && onRefuse) onRefuse(win)
    return isApp && accepts(win)
  })
}

/**
 * `ipcMain` for channels only some windows may use: `admit(event)` answers the
 * window or null, and a call it refuses never reaches the handler. A refused
 * invoke answers `refusal`.
 */
function gatedIpc(admit) {
  return {
    on(channel, handler) {
      ipcMain.on(channel, (event, ...args) => {
        if (admit(event)) handler(...args)
      })
    },
    handle(channel, handler, refusal = null) {
      ipcMain.handle(channel, (event, ...args) => (admit(event) ? handler(...args) : refusal))
    },
  }
}

module.exports = { alive, senderWindow, appSenderWindow, gatedIpc }
