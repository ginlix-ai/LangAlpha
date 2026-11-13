'use strict'

// Where the menu bar panel opens. Pure, because the inputs are whatever the OS
// reports for the tray icon and the display, and the cases that matter (a
// taskbar on the left, an icon at the very edge of the screen, an icon that
// reports no bounds at all) are ones a test can hand it and a desk cannot.

const MARGIN = 8
const GAP = 6

// The panel is as tall as its page, as the system's own menu bar panels are,
// up to the screen less a margin; the page scrolls inside it only past that.
// The floor keeps a page that has not laid out yet (or reports nothing) from
// opening as a sliver.
const WIDTH = 380
const MIN_HEIGHT = 260
// Until the page has said how tall it is.
const DEFAULT_HEIGHT = 480

const empty = (r) => !r || !(r.width > 0 && r.height > 0)

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), Math.max(min, max))
}

/**
 * The panel's size for a page that wants `preferredHeight`, on a work area
 * `workArea`: the width fixed, the height the page's own, held between the floor
 * and the screen's height less a margin. On a screen shorter than the floor,
 * the screen wins.
 */
function panelSize({ preferredHeight, workArea }) {
  const wanted = Number.isFinite(preferredHeight) && preferredHeight > 0 ? Math.ceil(preferredHeight) : DEFAULT_HEIGHT
  const ceiling = workArea.height - 2 * MARGIN
  const floor = Math.min(MIN_HEIGHT, ceiling)
  return { width: WIDTH, height: Math.max(floor, Math.min(wanted, ceiling)) }
}

/**
 * Which edge of the display the taskbar is on, read off the gap between the
 * display and its work area. An auto-hidden taskbar leaves no gap, so the tray
 * icon's own position answers instead: it sits on the taskbar, so whichever
 * edge it is nearest is the taskbar's.
 */
function taskbarEdge({ bounds, workArea, trayBounds }) {
  if (workArea.y > bounds.y) return 'top'
  if (workArea.x > bounds.x) return 'left'
  if (workArea.height < bounds.height) return 'bottom'
  if (workArea.width < bounds.width) return 'right'
  if (empty(trayBounds)) return 'bottom'
  const cx = trayBounds.x + trayBounds.width / 2
  const cy = trayBounds.y + trayBounds.height / 2
  const distances = {
    top: cy - bounds.y,
    bottom: bounds.y + bounds.height - cy,
    left: cx - bounds.x,
    right: bounds.x + bounds.width - cx,
  }
  return Object.entries(distances).sort((a, b) => a[1] - b[1])[0][0]
}

/**
 * The panel's top-left corner, and its height when the work area is shorter
 * than the panel wants. `anchor` is `traits.panel`.
 *
 * `menubar` (macOS): centred under the icon, just below the menu bar, as the
 * system's own menu bar extras open. `taskbar` (Windows): beside the icon on the
 * taskbar's side, which can be any edge. Either way clamped into the work area, so an icon at the edge of
 * the screen does not push the panel off it. An icon with no bounds (macOS
 * reports none for a menu bar item that is hidden behind the notch, and none at
 * all with the icon turned off) opens in the corner where the icon would be.
 */
function panelPosition({ anchor, trayBounds, display, size }) {
  const { workArea, bounds = workArea } = display
  const height = Math.min(size.height, workArea.height - 2 * MARGIN)
  const width = Math.min(size.width, workArea.width - 2 * MARGIN)
  const minX = workArea.x + MARGIN
  const maxX = workArea.x + workArea.width - width - MARGIN
  const minY = workArea.y + MARGIN
  const maxY = workArea.y + workArea.height - height - MARGIN
  const hasTray = !empty(trayBounds)

  if (anchor === 'menubar') {
    const x = hasTray ? trayBounds.x + trayBounds.width / 2 - width / 2 : maxX
    const top = hasTray ? Math.max(trayBounds.y + trayBounds.height, workArea.y) : workArea.y
    return {
      x: Math.round(clamp(x, minX, maxX)),
      y: Math.round(clamp(top + GAP, workArea.y, maxY)),
      width,
      height,
    }
  }

  const edge = taskbarEdge({ bounds, workArea, trayBounds })
  let x
  let y
  if (!hasTray) {
    x = edge === 'left' ? minX : maxX
    y = edge === 'top' ? minY : maxY
  } else if (edge === 'top' || edge === 'bottom') {
    x = trayBounds.x + trayBounds.width / 2 - width / 2
    y = edge === 'top' ? trayBounds.y + trayBounds.height + GAP : trayBounds.y - height - GAP
  } else {
    // A vertical taskbar: beside it, bottom edge level with the icon, the way
    // the system's own flyouts open there.
    x = edge === 'left' ? trayBounds.x + trayBounds.width + GAP : trayBounds.x - width - GAP
    y = trayBounds.y + trayBounds.height - height
  }
  return {
    x: Math.round(clamp(x, minX, maxX)),
    y: Math.round(clamp(y, minY, maxY)),
    width,
    height,
  }
}

module.exports = {
  panelPosition, panelSize, taskbarEdge, WIDTH, MIN_HEIGHT, DEFAULT_HEIGHT, MARGIN,
}
