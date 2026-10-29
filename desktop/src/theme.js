'use strict'

const { ipcMain } = require('electron')

const store = require('./store')
const { senderWindow } = require('./sender')

// Must equal the page's --color-bg-page (tokens.css: --background is 0 0% 9.8%
// dark, 0 0% 100% light). During a live resize the window frame outruns the
// painted content and this colour fills the gap, so a mismatch reads as a
// flashing band at the edge. Electron cannot eliminate that gap, only match it,
// which is why a hardcoded dark value gave light-theme users a dark band.
const BACKGROUNDS = {
  dark: '#191919',
  light: '#ffffff',
}

function backgroundFor(theme) {
  return BACKGROUNDS[theme] || BACKGROUNDS.dark
}

function isTheme(value) {
  return value === 'dark' || value === 'light'
}

// Theme has to come from the page: the shell cannot read a CSS variable, and
// the user's choice lives in the renderer's localStorage, not the OS setting.
function registerIpc() {
  ipcMain.on('shell:set-theme', (event, value) => {
    // Answered rather than dropped: the ack exists so a renderer can tell a
    // shell that understands this message from one that does not, and returning
    // undefined for a value this shell simply rejected says the wrong thing.
    if (!isTheme(value)) {
      event.returnValue = false
      return
    }
    // Painted before the store is consulted, because the guard below is about
    // the disk write and this is an in-memory native call. A window is created
    // with whatever colour the store held then, so a sibling window moving the
    // store between this one's creation and its first report would leave this
    // one wearing the old theme with the guard suppressing the correction.
    //
    // Only the window that reported it. The app and the console are separate
    // deploys resolving their own preference, so painting both from one message
    // gives the other window a frame in a theme its page is not using, which is
    // the mismatched resize band this exists to prevent.
    senderWindow(event)?.setBackgroundColor(backgroundFor(value))
    // Unchanged is the common case, not the rare one: every page load reports
    // the theme it resolved, nearly always the stored one, and this is
    // sendSync, so the renderer blocks on it. Writing anyway put a synchronous
    // mkdir and a synchronous file write on the main event loop of every load,
    // which is the whole app, menus and both windows, waiting on a disk for a
    // value that already had that value.
    if (store.get('theme') !== value) store.set('theme', value)
    // Acknowledged only so a renderer can tell an old shell from a new one.
    event.returnValue = true
  })
}

module.exports = { backgroundFor, isTheme, registerIpc }
