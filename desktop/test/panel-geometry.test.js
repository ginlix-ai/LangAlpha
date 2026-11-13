'use strict'

// Where the menu bar panel opens and how tall it is. Pure, and worth pinning for
// the inputs a desk cannot produce on demand: a taskbar on the left, an icon at
// the edge of the screen, an icon that reports no bounds at all.

const { test, describe } = require('node:test')
const assert = require('node:assert/strict')

const geometry = require('../src/panelGeometry.js')

const { panelPosition, panelSize, taskbarEdge } = geometry

describe('where the panel opens', () => {
  const SIZE = { width: 380, height: 600 }
  const screen = { x: 0, y: 0, width: 1512, height: 982 }

  test('macOS: centred under the icon, just below the menu bar', () => {
    const display = { bounds: screen, workArea: { x: 0, y: 33, width: 1512, height: 949 } }
    const p = panelPosition({ anchor: 'menubar', trayBounds: { x: 1000, y: 0, width: 24, height: 33 }, display, size: SIZE })
    assert.equal(p.x, 1012 - 190)
    assert.equal(p.y, 33 + 6)
    assert.equal(p.width, 380)
    assert.equal(p.height, 600)
  })

  test('macOS: an icon at the edge of the screen keeps the panel on it', () => {
    const display = { bounds: screen, workArea: { x: 0, y: 33, width: 1512, height: 949 } }
    const p = panelPosition({ anchor: 'menubar', trayBounds: { x: 1500, y: 0, width: 24, height: 33 }, display, size: SIZE })
    assert.equal(p.x, 1512 - 380 - 8)
  })

  test('macOS: no bounds (icon off, or behind the notch) opens top right', () => {
    const display = { bounds: screen, workArea: { x: 0, y: 33, width: 1512, height: 949 } }
    for (const trayBounds of [null, { x: 0, y: 0, width: 0, height: 0 }]) {
      const p = panelPosition({ anchor: 'menubar', trayBounds, display, size: SIZE })
      assert.equal(p.x, 1512 - 380 - 8)
      assert.equal(p.y, 33 + 6)
    }
  })

  test('a second display to the left, at negative coordinates', () => {
    const display = { bounds: { x: -1920, y: 0, width: 1920, height: 1080 }, workArea: { x: -1920, y: 25, width: 1920, height: 1055 } }
    const p = panelPosition({ anchor: 'menubar', trayBounds: { x: -400, y: 0, width: 22, height: 24 }, display, size: SIZE })
    assert.equal(p.x, Math.round(-389 - 190))
    assert.ok(p.x >= -1920 + 8)
  })

  test('a short screen shrinks the panel rather than spilling it', () => {
    const display = { bounds: { x: 0, y: 0, width: 1280, height: 600 }, workArea: { x: 0, y: 25, width: 1280, height: 575 } }
    const p = panelPosition({ anchor: 'menubar', trayBounds: { x: 600, y: 0, width: 22, height: 24 }, display, size: SIZE })
    assert.equal(p.height, 575 - 16)
    assert.ok(p.y + p.height <= 25 + 575)
  })

  const full = { x: 0, y: 0, width: 1920, height: 1080 }
  const cases = [
    {
      edge: 'bottom',
      workArea: { x: 0, y: 0, width: 1920, height: 1040 },
      tray: { x: 1800, y: 1048, width: 24, height: 24 },
      // Above the icon, which on a taskbar outside the work area means held inside it.
      check: (p) => { assert.equal(p.y, 1040 - 600 - 8); assert.equal(p.x, 1920 - 380 - 8) },
    },
    {
      edge: 'top',
      workArea: { x: 0, y: 40, width: 1920, height: 1040 },
      tray: { x: 1000, y: 8, width: 24, height: 24 },
      check: (p) => { assert.equal(p.y, 40 + 8); assert.equal(p.x, 1012 - 190) },
    },
    {
      edge: 'left',
      workArea: { x: 60, y: 0, width: 1860, height: 1080 },
      tray: { x: 18, y: 1000, width: 24, height: 24 },
      check: (p) => { assert.equal(p.x, 60 + 8); assert.equal(p.y, 1024 - 600) },
    },
    {
      edge: 'right',
      workArea: { x: 0, y: 0, width: 1860, height: 1080 },
      tray: { x: 1878, y: 1000, width: 24, height: 24 },
      check: (p) => { assert.equal(p.x, 1860 - 380 - 8); assert.equal(p.y, 1024 - 600) },
    },
  ]
  for (const c of cases) {
    test(`Windows: a taskbar on the ${c.edge}`, () => {
      const display = { bounds: full, workArea: c.workArea }
      assert.equal(taskbarEdge({ bounds: full, workArea: c.workArea, trayBounds: c.tray }), c.edge)
      const p = panelPosition({ anchor: 'taskbar', trayBounds: c.tray, display, size: SIZE })
      c.check(p)
      assert.ok(p.x >= c.workArea.x && p.x + p.width <= c.workArea.x + c.workArea.width, 'off the work area')
      assert.ok(p.y >= c.workArea.y && p.y + p.height <= c.workArea.y + c.workArea.height, 'off the work area')
    })
  }

  test('Windows: an auto-hidden taskbar is found from where the icon sits', () => {
    assert.equal(taskbarEdge({ bounds: full, workArea: full, trayBounds: { x: 1800, y: 1060, width: 20, height: 20 } }), 'bottom')
    assert.equal(taskbarEdge({ bounds: full, workArea: full, trayBounds: { x: 2, y: 500, width: 20, height: 20 } }), 'left')
    assert.equal(taskbarEdge({ bounds: full, workArea: full, trayBounds: null }), 'bottom')
  })

  test('Windows: no bounds opens in the taskbar corner', () => {
    const display = { bounds: full, workArea: { x: 0, y: 0, width: 1920, height: 1040 } }
    const p = panelPosition({ anchor: 'taskbar', trayBounds: null, display, size: SIZE })
    assert.equal(p.x, 1920 - 380 - 8)
    assert.equal(p.y, 1040 - 600 - 8)
  })
})

describe('how tall the panel is', () => {
  const tall = { x: 0, y: 33, width: 1512, height: 949 }

  test('as tall as its page, between the floor and the screen, at a fixed width', () => {
    assert.deepEqual(panelSize({ preferredHeight: 412, workArea: tall }), { width: 380, height: 412 })
    assert.equal(panelSize({ preferredHeight: 120, workArea: tall }).height, geometry.MIN_HEIGHT)
    assert.equal(panelSize({ preferredHeight: 760, workArea: tall }).height, 760, 'a panel taller than 640 is not cut to scroll')
    assert.equal(panelSize({ preferredHeight: 2000, workArea: tall }).height, tall.height - 2 * geometry.MARGIN)
    assert.equal(panelSize({ preferredHeight: 300.4, workArea: tall }).height, 301, 'a fractional height clipped the last row')
  })

  test('before the page has said, a default; a nonsense answer is no answer', () => {
    for (const preferredHeight of [null, undefined, 0, -5, NaN, Infinity]) {
      assert.equal(panelSize({ preferredHeight, workArea: tall }).height, geometry.DEFAULT_HEIGHT, String(preferredHeight))
    }
  })

  test('a short screen lowers the ceiling to the work area less its margins, and below the floor the screen wins', () => {
    const short = { x: 0, y: 25, width: 1280, height: 575 }
    assert.equal(panelSize({ preferredHeight: 900, workArea: short }).height, 575 - 2 * geometry.MARGIN)
    const tiny = { x: 0, y: 0, width: 800, height: 200 }
    assert.equal(panelSize({ preferredHeight: 900, workArea: tiny }).height, 200 - 2 * geometry.MARGIN)
    assert.equal(panelSize({ preferredHeight: 10, workArea: tiny }).height, 200 - 2 * geometry.MARGIN)
  })

  test('growing keeps it under the icon on macOS and level with the taskbar on Windows', () => {
    const mac = { bounds: { x: 0, y: 0, width: 1512, height: 982 }, workArea: tall }
    const icon = { x: 1000, y: 0, width: 24, height: 33 }
    const small = panelPosition({ anchor: 'menubar', trayBounds: icon, display: mac, size: panelSize({ preferredHeight: 300, workArea: tall }) })
    const big = panelPosition({ anchor: 'menubar', trayBounds: icon, display: mac, size: panelSize({ preferredHeight: 600, workArea: tall }) })
    assert.deepEqual([small.x, small.y], [big.x, big.y], 'the top moved as it grew')
    assert.equal(big.height, 600)

    const work = { x: 0, y: 0, width: 1920, height: 1040 }
    const win = { bounds: { x: 0, y: 0, width: 1920, height: 1080 }, workArea: work }
    const tray = { x: 1500, y: 1048, width: 24, height: 24 }
    const before = panelPosition({ anchor: 'taskbar', trayBounds: tray, display: win, size: panelSize({ preferredHeight: 300, workArea: work }) })
    const after = panelPosition({ anchor: 'taskbar', trayBounds: tray, display: win, size: panelSize({ preferredHeight: 600, workArea: work }) })
    assert.equal(before.y + before.height, after.y + after.height, 'the bottom moved off the taskbar as it grew')
    assert.equal(before.x, after.x)
  })
})
