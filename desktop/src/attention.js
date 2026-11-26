'use strict'

const { app, Menu, nativeImage } = require('electron')

const { shownBadge } = require('./desktopSettings')

// What the app wants the user to come back for, said where the OS says it: the
// Dock badge and Dock menu on macOS, a dot on the taskbar button and a jump list
// task on Windows, the launcher badge on Linux where the desktop has one. The
// menu bar page reports it (`setAttention`); this only draws it, as a number, a
// dot or nothing by the user's `badge` setting (`shownBadge`).

const STATES = ['running', 'unread', 'needs_input']
const MAX_THREADS = 5

// A glyph before each thread in the Dock menu, which shows no secondary text.
const PREFIX = { needs_input: '◆', unread: '●', running: '◌' }

// The Dock's badge without the number: its red capsule around a dot.
const DOCK_DOT = '•'

/** The attention the page reported, trimmed to the contract. */
function validate(raw) {
  const count = Number.isInteger(raw?.count) ? Math.min(Math.max(raw.count, 0), 9999) : 0
  const threads = Array.isArray(raw?.threads) ? raw.threads : []
  return {
    count,
    threads: threads
      .filter((t) => t && typeof t.id === 'string' && typeof t.title === 'string'
        && typeof t.path === 'string' && STATES.includes(t.state))
      .slice(0, MAX_THREADS)
      .map((t) => ({ id: t.id.slice(0, 128), title: t.title, path: t.path.slice(0, 2048), state: t.state })),
  }
}

function threadLabel(t) {
  const title = t.title.replace(/\s+/g, ' ').trim() || 'Untitled thread'
  const short = title.length > 48 ? `${title.slice(0, 47)}…` : title
  return `${PREFIX[t.state]} ${short}`
}

/** The Dock menu template: the two commands, then the threads waiting on the user. */
function dockTemplate(attention, { onNewThread, onGlance, onOpen }) {
  const template = [
    { label: 'New Thread', click: onNewThread },
    { label: 'Market Glance', click: onGlance },
  ]
  if (attention.threads.length) {
    template.push({ type: 'separator' })
    for (const t of attention.threads) template.push({ label: threadLabel(t), click: () => onOpen(t.path) })
  }
  return template
}

/**
 * A filled circle as a raw BGRA bitmap, for the Windows overlay icon. Drawn
 * rather than shipped: one colour, one shape, and no file to keep in step with
 * the app icon.
 */
function dotBitmap(size = 16, [r, g, b] = [0xe5, 0x48, 0x4d]) {
  const buf = Buffer.alloc(size * size * 4)
  const c = (size - 1) / 2
  const radius = size / 2 - 0.5
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const d = Math.hypot(x - c, y - c)
      // One pixel of anti-aliasing at the rim.
      const a = Math.max(0, Math.min(1, radius - d + 0.5))
      const i = (y * size + x) * 4
      buf[i] = Math.round(b * a)
      buf[i + 1] = Math.round(g * a)
      buf[i + 2] = Math.round(r * a)
      buf[i + 3] = Math.round(255 * a)
    }
  }
  return buf
}

function createAttention({ traits, getMainWindow, onNewThread, onGlance, onOpen, badge }) {
  let current = { count: 0, threads: [] }
  let lastDockKey = null
  let dot = null

  function overlayDot() {
    if (!dot) dot = nativeImage.createFromBitmap(dotBitmap(16), { width: 16, height: 16 })
    return dot
  }

  /**
   * The count as the setting and the platform have it drawn. Called again when
   * the setting changes, and when a main window is made, since the Windows
   * overlay sits on that window.
   */
  function drawBadge() {
    const n = current.count
    const shown = n > 0 ? shownBadge(badge(), traits) : 'off'
    if (traits.badge === 'overlay') {
      const win = getMainWindow()
      if (win && !win.isDestroyed()) win.setOverlayIcon(shown === 'off' ? null : overlayDot(), shown === 'off' ? '' : `${n} waiting`)
    } else if (shown === 'dot') {
      app.dock.setBadge(DOCK_DOT)
    } else {
      app.setBadgeCount(shown === 'count' ? n : 0)
    }
  }

  function set(raw) {
    current = validate(raw)
    drawBadge()
    if (traits.dock && app.dock) {
      // Rebuilt only when it changed: the page reports on every feed event, and
      // a Dock menu rebuilt under an open menu closes it.
      const key = JSON.stringify(current.threads)
      if (key !== lastDockKey) {
        lastDockKey = key
        app.dock.setMenu(Menu.buildFromTemplate(dockTemplate(current, { onNewThread, onGlance, onOpen })))
      }
    }
  }

  /**
   * The jump list's one task. It starts a second launch carrying the command,
   * which the running instance hears as `second-instance`. A dev run is
   * Electron's own binary, so the app's path rides along ahead of the flag.
   */
  function installUserTasks() {
    if (!traits.jumpList) return
    const devApp = process.defaultApp && process.argv[1] ? `"${process.argv[1]}" ` : ''
    app.setUserTasks([{
      program: process.execPath,
      arguments: `${devApp}--command=new-thread`,
      iconPath: process.execPath,
      iconIndex: 0,
      title: 'New Thread',
      description: 'Start a new thread',
    }])
  }

  return { set, drawBadge, installUserTasks }
}

module.exports = { createAttention, validate, dockTemplate, dotBitmap, threadLabel }
