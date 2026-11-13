'use strict'

// The desktop settings (desktopSettings.js) and what each platform can do with
// them (platformTraits.js). What is worth pinning is what the OS cannot be asked
// for on demand: a hand-edited settings file, and another platform's answers.

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')

const { loadShell, cleanup } = require('./helpers')

const settings = require('../src/desktopSettings.js')
const { traits } = require('../src/platformTraits.js')

after(cleanup)

describe('desktop settings', () => {
  test('defaults are the contract\'s', () => {
    assert.deepEqual(settings.normalize(undefined), {
      notifications: { turn: true, needs_input: true, wakeup: true, automation: true, started: true },
      menubar: { enabled: true, items: [], style: 'rotate', rotateSeconds: 4, show: 'change', extendedHours: true },
      panel: {
        sections: [
          { id: 'market', visible: true }, { id: 'portfolio', visible: true },
          { id: 'watchlist', visible: true }, { id: 'agent', visible: true },
        ],
        tiles: [
          { symbol: 'GSPC', kind: 'index' }, { symbol: 'IXIC', kind: 'index' },
          { symbol: 'DJI', kind: 'index' }, { symbol: 'VIX', kind: 'index' },
        ],
        watchlistId: null,
        watchlistRows: 5,
      },
      openAtLogin: false,
      shortcut: '',
      keepRunningInTray: true,
    })
  })

  test('a hand-edited file costs only its bad fields, and unknown keys go', () => {
    const read = settings.normalize({
      notifications: { turn: 'false', wakeup: false, extra: true },
      menubar: { enabled: 0, items: 'NVDA', style: 'tape', rotateSeconds: '4', show: 'both', extendedHours: 'no', title: 'portfolio' },
      panel: { tiles: {}, watchlistId: 42, watchlistRows: 'many', extra: 1 },
      openAtLogin: true,
      shortcut: 'Alt+CommandOrControl+L',
      keepRunningInTray: null,
      evil: 'x',
    })
    assert.equal(read.notifications.turn, true, 'the string "false" was read as a switch')
    assert.equal(read.notifications.wakeup, false)
    assert.equal('extra' in read.notifications, false)
    // `items` is not a list, and a key the shape does not have (`title`) is dropped.
    assert.deepEqual(read.menubar, { enabled: true, items: [], style: 'rotate', rotateSeconds: 4, show: 'both', extendedHours: true })
    assert.deepEqual(read.panel, settings.normalize({}).panel)
    assert.equal(read.openAtLogin, true)
    assert.equal(read.shortcut, 'Alt+CommandOrControl+L')
    assert.equal(read.keepRunningInTray, true)
    assert.equal('evil' in read, false)
  })

  test('menu bar items: bad entries go, repeats go, at most eight, an index is bare', () => {
    const items = [
      { kind: 'portfolio', extra: 1 },
      { kind: 'stock', symbol: ' nvda ' },
      { kind: 'stock', symbol: 'NVDA' },
      { kind: 'index', symbol: '^gspc' },
      { kind: 'index', symbol: 'GSPC' },
      { kind: 'stock', symbol: 'GSPC' },
      { kind: 'portfolio' },
      { kind: 'crypto', symbol: 'BTC' },
      { kind: 'stock', symbol: 'not a ticker!' },
      'AAPL',
      null,
      ...['A', 'B', 'C', 'D', 'E', 'F'].map((symbol) => ({ kind: 'stock', symbol })),
    ]
    assert.deepEqual(settings.normalize({ menubar: { items } }).menubar.items, [
      { kind: 'portfolio' },
      { symbol: 'NVDA', kind: 'stock' },
      { symbol: 'GSPC', kind: 'index' },
      { symbol: 'GSPC', kind: 'stock' },
      { symbol: 'A', kind: 'stock' },
      { symbol: 'B', kind: 'stock' },
      { symbol: 'C', kind: 'stock' },
      { symbol: 'D', kind: 'stock' },
    ])
  })

  test('the panel: every section once in the order given, four tiles, clamps', () => {
    const panel = settings.normalize({
      panel: {
        sections: [
          { id: 'agent', visible: false }, { id: 'agent', visible: true }, { id: 'news', visible: true },
          'market', { id: 'watchlist', visible: 'no' },
        ],
        tiles: [
          { symbol: '^VIX', kind: 'index' }, { symbol: 'VIX', kind: 'index' }, { symbol: 'spy', kind: 'stock' },
          { symbol: 'QQQ', kind: 'stock' }, { symbol: 'IWM', kind: 'stock' }, { symbol: 'DIA', kind: 'stock' },
        ],
        watchlistId: '  wl-1 ',
        watchlistRows: 99,
      },
    }).panel
    assert.deepEqual(panel.sections, [
      { id: 'agent', visible: false },
      { id: 'watchlist', visible: true },
      { id: 'market', visible: true },
      { id: 'portfolio', visible: true },
    ])
    assert.deepEqual(panel.tiles.map((t) => `${t.kind}:${t.symbol}`), ['index:VIX', 'stock:SPY', 'stock:QQQ', 'stock:IWM'])
    assert.equal(panel.watchlistId, 'wl-1')
    assert.equal(panel.watchlistRows, 10)
    const low = settings.normalize({ panel: { watchlistRows: 0.2, watchlistId: '' }, menubar: { rotateSeconds: 1 } })
    assert.equal(low.panel.watchlistRows, 1)
    assert.equal(low.panel.watchlistId, null)
    assert.equal(low.menubar.rotateSeconds, 2)
    assert.equal(settings.normalize({ menubar: { rotateSeconds: 300 } }).menubar.rotateSeconds, 30)
    assert.deepEqual(settings.normalize({ panel: { tiles: [] } }).panel.tiles, [])
  })

  test('a patch merges one level deep, an array replaces the whole, and a bad value keeps what the user had', () => {
    const current = settings.merge(settings.DEFAULTS, {
      menubar: { items: [{ kind: 'portfolio' }, { kind: 'stock', symbol: 'QQQ' }], style: 'side' },
    })
    const next = settings.merge(current, {
      notifications: { started: false },
      menubar: { items: 'not a list', rotateSeconds: 'fast' },
      panel: { sections: [{ id: 'agent', visible: false }], tiles: [{ symbol: 'NDX', kind: 'index' }] },
      shortcut: 'Shift+',
      unknown: 1,
    })
    assert.deepEqual(next.menubar.items, [{ kind: 'portfolio' }, { symbol: 'QQQ', kind: 'stock' }])
    assert.equal(next.menubar.style, 'side')
    assert.equal(next.menubar.rotateSeconds, 4)
    assert.deepEqual(next.panel.tiles, [{ symbol: 'NDX', kind: 'index' }], 'tiles were merged, not replaced')
    assert.deepEqual(next.panel.sections.map((s) => s.id), ['agent', 'market', 'portfolio', 'watchlist'])
    assert.equal(next.panel.sections[0].visible, false)
    assert.equal(next.notifications.started, false)
    assert.equal(next.notifications.turn, true)
    assert.equal(next.shortcut, '', 'an accelerator with a dangling + was stored')
    assert.equal('unknown' in next, false)
    assert.deepEqual(settings.merge(current, 'nope'), current)
    assert.equal(settings.merge(current, { shortcut: '' }).shortcut, '')
    // A section left out of a patch keeps how the user had it, not the default.
    const hidden = settings.merge(next, { panel: { sections: [{ id: 'market', visible: true }] } })
    assert.deepEqual(hidden.panel.sections, [
      { id: 'market', visible: true }, { id: 'agent', visible: false },
      { id: 'portfolio', visible: true }, { id: 'watchlist', visible: true },
    ])
    assert.deepEqual(settings.merge(current, { menubar: { items: [] } }).menubar.items, [])
  })


  test('what each platform supports is a projection of its traits', () => {
    const mac = settings.support(traits('darwin', {}))
    assert.deepEqual(mac, {
      menubarPanel: true, trayTitle: true, openAtLogin: true, shortcut: true, keepRunningInTray: false, notificationActions: true,
    })
    const win = settings.support(traits('win32', {}))
    assert.equal(win.trayTitle, false)
    assert.equal(win.keepRunningInTray, true)
    assert.equal(win.menubarPanel, true)
    const x11 = settings.support(traits('linux', { XDG_SESSION_TYPE: 'x11' }))
    assert.equal(x11.menubarPanel, false)
    assert.equal(x11.notificationActions, false)
    assert.equal(x11.shortcut, true)
    assert.equal(settings.support(traits('linux', { XDG_SESSION_TYPE: 'wayland' })).shortcut, false)
  })

  test('the view carries support and, only when there is one, the shortcut error', () => {
    const plain = settings.view(settings.DEFAULTS, { traits: traits('darwin', {}) })
    assert.equal('shortcutError' in plain, false)
    assert.ok(plain.support)
    const failed = settings.view(settings.DEFAULTS, { traits: traits('darwin', {}), shortcutError: 'taken' })
    assert.equal(failed.shortcutError, 'taken')
  })

  test('the store reads a hand-edited group back into shape', () => {
    const { store } = loadShell({ settings: { desktop: { menubar: { enabled: 'yes', title: 'portfolio' }, junk: 1 } } })
    const desktop = store.get('desktop')
    assert.equal(desktop.menubar.enabled, true)
    assert.deepEqual(desktop.menubar.items, [], 'a key the shape does not have filled in the items')
    assert.equal('title' in desktop.menubar, false)
    assert.equal('junk' in desktop, false)
    assert.equal(loadShell({ settings: { desktop: 'broken' } }).store.get('desktop').openAtLogin, false)
  })
})

describe('platform traits', () => {
  test('any platform that is neither macOS nor Windows is treated as Linux', () => {
    const linux = traits('linux', {})
    for (const platform of ['freebsd', 'openbsd', 'sunos']) {
      assert.deepEqual(traits(platform, {}), linux, platform)
    }
    assert.equal(linux.panel, null)
    assert.equal(linux.trayMenuOnly, true)
    assert.equal(linux.replaceNotifications, false)
    assert.equal(linux.lastWindowClosed, 'quit')
    assert.equal(linux.loginItem, 'xdg-autostart')
  })

  test('macOS and Windows each answer as themselves', () => {
    const mac = traits('darwin', {})
    assert.deepEqual(
      [mac.panel, mac.trayTitle, mac.dock, mac.lastWindowClosed, mac.loginItem],
      ['menubar', true, true, 'stay', 'login-items'],
    )
    const win = traits('win32', {})
    assert.deepEqual(
      [win.panel, win.tooltipLines, win.overlayBadge, win.jumpList, win.lastWindowClosed, win.loginItem],
      ['taskbar', true, true, true, 'tray', 'run-key'],
    )
    assert.equal(win.badgeCount, false)
  })
})
