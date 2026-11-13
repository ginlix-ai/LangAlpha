'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { Tray, Menu, nativeImage } = require('electron')

const config = require('./config')
const { clampInt } = require('./desktopSettings')

// The menu bar icon (the notification-area icon on Windows, an AppIndicator on
// most Linux desktops). On macOS a template image, which the system tints for a
// light or dark menu bar and for the highlighted state; elsewhere the app's own
// colours, since neither taskbar tints anything.
//
// Beside it, the lines the menu bar page formats (`NVDA ▼5.60%`, `Port ▲0.42%`).
// The page formats; this lays them out per platform and keeps them moving. How
// they are laid out (`style`, `rotateSeconds`) and whether any are wanted at all
// (`items`) is read from the desktop settings, never from the page, so a page a
// step behind the settings cannot show what the user turned off.
// Rotation lives here rather than in the page because the page is a hidden
// window, and a hidden page's timers are throttled to once a minute: a slot that
// should turn every four seconds would sit for sixty. For the same reason the
// shell, not the page's own timer, says when to fetch fresh lines
// (`onRefresh`): an IPC message reaches a hidden page at once.

const RESOURCES = path.join(__dirname, '..', 'resources')

const MAX_LINES = 8
const MAX_LINE = 32
// Side by side, more than three is wider than a crowded menu bar has room for.
const SIDE_COUNT = 3
const SIDE_GAP = '  '
// A Windows tooltip holds 127 characters and cuts the rest mid-word.
const TOOLTIP_MAX = 127
const REFRESH_MS = { min: 10_000, max: 600_000, fallback: 60_000 }

/**
 * Both scales in one image. Read as bytes rather than by path because the
 * files sit inside the app archive in a package, and decoding a buffer is the
 * same call whether or not they do.
 */
function trayImage(traits) {
  const stem = traits.templateIcon ? 'trayTemplate' : 'tray'
  const image = nativeImage.createFromBuffer(fs.readFileSync(path.join(RESOURCES, `${stem}.png`)), { scaleFactor: 1 })
  try {
    image.addRepresentation({ scaleFactor: 2, buffer: fs.readFileSync(path.join(RESOURCES, `${stem}@2x.png`)) })
  } catch {
    // One scale is still an icon.
  }
  if (traits.templateIcon) image.setTemplateImage(true)
  return image
}

/** One line from the page, kept to one short line. */
function cleanLine(text) {
  if (typeof text !== 'string') return ''
  return text.replace(/\s+/g, ' ').trim().slice(0, MAX_LINE)
}

/** The page's items (`{ text }`, or a bare string) as lines: blanks dropped, at most eight. */
function cleanLines(items) {
  if (!Array.isArray(items)) return []
  return items
    .map((item) => cleanLine(typeof item === 'string' ? item : item && item.text))
    .filter(Boolean)
    .slice(0, MAX_LINES)
}

/** How often to ask the page for fresh lines, from what it asked for. */
function refreshPeriod(ms) {
  return clampInt(ms, REFRESH_MS, REFRESH_MS.fallback)
}

const width = (s) => [...s].length

/** `line` padded with spaces to `size` characters, so in a monospaced font every frame is as wide. */
function padTo(line, size) {
  return line + ' '.repeat(Math.max(0, size - width(line)))
}

/**
 * What is drawn beside the icon, one frame per slot. None for no lines; one
 * static frame for one line or side by side (the first three, two spaces
 * apart); for rotate, a frame per line, each padded to the longest so the
 * status item never changes width and nothing beside it in the menu bar moves.
 */
function titleFrames(lines, style) {
  if (lines.length === 0) return []
  if (lines.length === 1) return [lines[0]]
  if (style === 'side') return [lines.slice(0, SIDE_COUNT).join(SIDE_GAP)]
  const size = Math.max(...lines.map(width))
  return lines.map((line) => padTo(line, size))
}

/** The frame showing at `slot`, which counts up for as long as rotation runs. */
function frameAt(frames, slot) {
  if (frames.length === 0) return ''
  return frames[((slot % frames.length) + frames.length) % frames.length]
}

/**
 * Windows' tooltip: the app's name, then a line each, as many as fit in the
 * 127 characters Windows keeps. The name first, since the tooltip is the only
 * place that says whose icon it is.
 */
function tooltipText(appName, lines) {
  let text = appName
  for (const line of lines) {
    const next = `${text}\n${line}`
    if (next.length > TOOLTIP_MAX) break
    text = next
  }
  return text
}

/** Linux's lines: disabled items heading the tray menu, then a separator. */
function headlineItems(lines) {
  if (lines.length === 0) return []
  return [...lines.map((label) => ({ label, enabled: false })), { type: 'separator' }]
}

/**
 * Everything `traits` shows for `lines`: `frames` for the title beside the icon
 * (empty where there is none), the `tooltip`, and the `headlines` at the top of
 * a menu-only icon's menu.
 */
function layout(lines, { style = 'rotate', traits, appName = config.appName }) {
  return {
    frames: traits.trayTitle ? titleFrames(lines, style) : [],
    tooltip: traits.tooltipLines ? tooltipText(appName, lines) : appName,
    headlines: traits.trayMenuOnly ? headlineItems(lines) : [],
  }
}

/**
 * `onClick` opens the panel where the icon reports clicks; a menu-only icon
 * (an AppIndicator) gets the menu as its only behaviour. `menubar()` answers the
 * current `menubar` settings. `onRefresh` asks the page for fresh lines; it runs
 * only while there are lines and the icon is shown.
 */
function createTray({
  traits, onClick, menuTemplate, menubar, onRefresh = () => {}, timers = globalThis,
}) {
  let tray = null
  let lines = []
  let refreshMs = REFRESH_MS.fallback
  let slot = 0
  let rotation = null
  let rotationEvery = 0
  let refresh = null
  let refreshEvery = 0

  // With nothing chosen the icon stands alone, whatever the page sends.
  const gate = (next) => (menubar().items.length > 0 ? next : [])
  const current = () => layout(lines, { style: menubar().style, traits })

  function stopRotation() {
    if (rotation) timers.clearInterval(rotation)
    rotation = null
    rotationEvery = 0
  }

  function stopRefresh() {
    if (refresh) timers.clearInterval(refresh)
    refresh = null
    refreshEvery = 0
  }

  function every(ms, fn) {
    const handle = timers.setInterval(fn, ms)
    if (handle && typeof handle.unref === 'function') handle.unref()
    return handle
  }

  function drawTitle() {
    if (!tray || !traits.trayTitle) return
    const frame = frameAt(current().frames, slot)
    tray.setTitle(frame ? ` ${frame}` : '', { fontType: 'monospaced' })
  }

  /**
   * Start, keep or stop each timer to match the state. A running timer at the
   * right period is left alone, so lines refreshed every fifteen seconds do not
   * restart a four-second rotation, which would pin it to its first slot.
   */
  function schedule() {
    const rotateMs = menubar().rotateSeconds * 1000
    const rotating = Boolean(tray) && current().frames.length > 1
    if (!rotating) stopRotation()
    else if (rotationEvery !== rotateMs) {
      stopRotation()
      rotationEvery = rotateMs
      rotation = every(rotateMs, () => {
        slot += 1
        drawTitle()
      })
    }

    const refreshing = Boolean(tray) && lines.length > 0
    if (!refreshing) stopRefresh()
    else if (refreshEvery !== refreshMs) {
      stopRefresh()
      refreshEvery = refreshMs
      refresh = every(refreshMs, () => onRefresh())
    }
  }

  function buildMenu() {
    return Menu.buildFromTemplate([...current().headlines, ...menuTemplate()])
  }

  /** A menu-only icon's menu is set on it rather than built per click, so it is set again whenever it changes. */
  function refreshMenu() {
    if (tray && traits.trayMenuOnly) tray.setContextMenu(buildMenu())
  }

  function apply() {
    if (!tray) return
    drawTitle()
    tray.setToolTip(current().tooltip)
    schedule()
  }

  function show() {
    if (tray) return tray
    tray = new Tray(trayImage(traits))
    if (traits.trayMenuOnly) {
      refreshMenu()
    } else {
      tray.on('click', () => onClick())
      // A secondary click gets the short menu rather than the panel, as other
      // menu bar apps do. Built on demand so it never shows a stale label.
      tray.on('right-click', () => tray.popUpContextMenu(buildMenu()))
    }
    apply()
    // Lines kept while the icon was off are as old as when it went off, and the
    // next timed ask can be minutes away, so they are asked for afresh now.
    if (lines.length > 0) onRefresh()
    return tray
  }

  function hide() {
    stopRotation()
    stopRefresh()
    if (!tray) return
    tray.destroy()
    tray = null
  }

  /** The page's lines, and how often it wants to be asked for fresh ones (`{ refreshMs }`). */
  function setItems(items, options) {
    const next = gate(cleanLines(items))
    const changed = next.length !== lines.length || next.some((line, i) => line !== lines[i])
    lines = next
    refreshMs = refreshPeriod(options && typeof options === 'object' ? options.refreshMs : undefined)
    apply()
    if (changed) refreshMenu()
  }

  /** The settings changed: what is chosen, the layout, and the menu that shows them. */
  function update() {
    lines = gate(lines)
    apply()
    refreshMenu()
  }

  /** Where the icon is on screen, or null with no icon. */
  function bounds() {
    if (!tray || tray.isDestroyed()) return null
    try {
      return tray.getBounds()
    } catch {
      return null
    }
  }

  return {
    show,
    hide,
    setItems,
    update,
    bounds,
  }
}

module.exports = {
  createTray,
  cleanLine,
  cleanLines,
  refreshPeriod,
  padTo,
  titleFrames,
  frameAt,
  tooltipText,
  headlineItems,
  layout,
  trayImage,
  REFRESH_MS,
}
